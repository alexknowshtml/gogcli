package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"google.golang.org/api/docs/v1"
	"google.golang.org/api/drive/v3"
	gapi "google.golang.org/api/googleapi"

	"github.com/steipete/gogcli/internal/googleapi"
	"github.com/steipete/gogcli/internal/outfmt"
	"github.com/steipete/gogcli/internal/ui"
)

var newDocsService = googleapi.NewDocs

type DocsCmd struct {
	Export   DocsExportCmd   `cmd:"" name:"export" help:"Export a Google Doc (pdf|docx|txt)"`
	Info     DocsInfoCmd     `cmd:"" name:"info" help:"Get Google Doc metadata"`
	Create   DocsCreateCmd   `cmd:"" name:"create" help:"Create a Google Doc"`
	Copy     DocsCopyCmd     `cmd:"" name:"copy" help:"Copy a Google Doc"`
	Cat      DocsCatCmd      `cmd:"" name:"cat" help:"Print a Google Doc as plain text"`
	ListTabs DocsListTabsCmd `cmd:"" name:"list-tabs" help:"List all tabs in a Google Doc"`
	Update   DocsUpdateCmd   `cmd:"" name:"update" help:"Update content in a Google Doc"`
}
type DocsExportCmd struct {
	DocID  string         `arg:"" name:"docId" help:"Doc ID"`
	Output OutputPathFlag `embed:""`
	Format string         `name:"format" help:"Export format: pdf|docx|txt" default:"pdf"`
}

func (c *DocsExportCmd) Run(ctx context.Context, flags *RootFlags) error {
	return exportViaDrive(ctx, flags, exportViaDriveOptions{
		ArgName:       "docId",
		ExpectedMime:  "application/vnd.google-apps.document",
		KindLabel:     "Google Doc",
		DefaultFormat: "pdf",
	}, c.DocID, c.Output.Path, c.Format)
}

type DocsInfoCmd struct {
	DocID string `arg:"" name:"docId" help:"Doc ID"`
}

func (c *DocsInfoCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	id := strings.TrimSpace(c.DocID)
	if id == "" {
		return usage("empty docId")
	}

	svc, err := newDocsService(ctx, account)
	if err != nil {
		return err
	}

	doc, err := svc.Documents.Get(id).
		Fields("documentId,title,revisionId").
		Context(ctx).
		Do()
	if err != nil {
		if isDocsNotFound(err) {
			return fmt.Errorf("doc not found or not a Google Doc (id=%s)", id)
		}
		return err
	}
	if doc == nil {
		return errors.New("doc not found")
	}

	file := map[string]any{
		"id":       doc.DocumentId,
		"name":     doc.Title,
		"mimeType": driveMimeGoogleDoc,
	}
	if link := docsWebViewLink(doc.DocumentId); link != "" {
		file["webViewLink"] = link
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(os.Stdout, map[string]any{
			strFile:    file,
			"document": doc,
		})
	}

	u.Out().Printf("id\t%s", doc.DocumentId)
	u.Out().Printf("name\t%s", doc.Title)
	u.Out().Printf("mime\t%s", driveMimeGoogleDoc)
	if link := docsWebViewLink(doc.DocumentId); link != "" {
		u.Out().Printf("link\t%s", link)
	}
	if doc.RevisionId != "" {
		u.Out().Printf("revision\t%s", doc.RevisionId)
	}
	return nil
}

type DocsCreateCmd struct {
	Title  string `arg:"" name:"title" help:"Doc title"`
	Parent string `name:"parent" help:"Destination folder ID"`
	File   string `name:"file" help:"Markdown file to import" type:"existingfile"`
}

func (c *DocsCreateCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	title := strings.TrimSpace(c.Title)
	if title == "" {
		return usage("empty title")
	}

	driveSvc, err := newDriveService(ctx, account)
	if err != nil {
		return err
	}

	f := &drive.File{
		Name:     title,
		MimeType: "application/vnd.google-apps.document",
	}
	parent := strings.TrimSpace(c.Parent)
	if parent != "" {
		f.Parents = []string{parent}
	}

	createCall := driveSvc.Files.Create(f).
		SupportsAllDrives(true).
		Fields("id, name, mimeType, webViewLink")

	// When --file is set, upload the markdown content and let Drive convert it.
	var images []markdownImage
	if c.File != "" {
		raw, readErr := os.ReadFile(c.File)
		if readErr != nil {
			return fmt.Errorf("read markdown file: %w", readErr)
		}
		content := string(raw)

		var cleaned string
		cleaned, images = extractMarkdownImages(content)

		createCall = createCall.Media(
			strings.NewReader(cleaned),
			gapi.ContentType("text/markdown"),
		)
	}

	created, err := createCall.Context(ctx).Do()
	if err != nil {
		return err
	}
	if created == nil {
		return errors.New("create failed")
	}

	// Pass 2: insert images if any were found.
	if len(images) > 0 {
		if err := c.insertImages(ctx, account, driveSvc, created.Id, images); err != nil {
			return fmt.Errorf("insert images: %w", err)
		}
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(os.Stdout, map[string]any{strFile: created})
	}

	u.Out().Printf("id\t%s", created.Id)
	u.Out().Printf("name\t%s", created.Name)
	u.Out().Printf("mime\t%s", created.MimeType)
	if created.WebViewLink != "" {
		u.Out().Printf("link\t%s", created.WebViewLink)
	}
	return nil
}

// insertImages performs pass 2: reads back the created doc, resolves image URLs,
// and replaces placeholder text with inline images.
func (c *DocsCreateCmd) insertImages(ctx context.Context, account string, driveSvc *drive.Service, docID string, images []markdownImage) error {
	docsSvc, err := newDocsService(ctx, account)
	if err != nil {
		return err
	}

	// Read back the document to find placeholder positions.
	doc, err := docsSvc.Documents.Get(docID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("read back document: %w", err)
	}

	placeholders := findPlaceholderIndices(doc, len(images))
	if len(placeholders) == 0 {
		return nil
	}

	// Resolve image URLs — upload local files to Drive temporarily.
	imageURLs := make(map[int]string)
	var tempFileIDs []string
	defer cleanupDriveFileIDsBestEffort(ctx, driveSvc, tempFileIDs)

	for _, img := range images {
		if _, ok := placeholders[img.placeholder()]; !ok {
			continue
		}
		if img.isRemote() {
			imageURLs[img.index] = img.originalRef
			continue
		}

		realPath, resolveErr := resolveMarkdownImagePath(c.File, img.originalRef)
		if resolveErr != nil {
			return resolveErr
		}

		url, fileID, uploadErr := uploadLocalImage(ctx, driveSvc, realPath)
		if uploadErr != nil {
			return uploadErr
		}
		tempFileIDs = append(tempFileIDs, fileID)
		imageURLs[img.index] = url
	}

	reqs := buildImageInsertRequests(placeholders, images, imageURLs)
	if len(reqs) == 0 {
		return nil
	}

	_, err = docsSvc.Documents.BatchUpdate(docID, &docs.BatchUpdateDocumentRequest{
		Requests: reqs,
	}).Context(ctx).Do()
	return err
}

type DocsCopyCmd struct {
	DocID  string `arg:"" name:"docId" help:"Doc ID"`
	Title  string `arg:"" name:"title" help:"New title"`
	Parent string `name:"parent" help:"Destination folder ID"`
}

func (c *DocsCopyCmd) Run(ctx context.Context, flags *RootFlags) error {
	return copyViaDrive(ctx, flags, copyViaDriveOptions{
		ArgName:      "docId",
		ExpectedMime: "application/vnd.google-apps.document",
		KindLabel:    "Google Doc",
	}, c.DocID, c.Title, c.Parent)
}

type DocsCatCmd struct {
	DocID    string `arg:"" name:"docId" help:"Doc ID"`
	MaxBytes int64  `name:"max-bytes" help:"Max bytes to read (0 = unlimited)" default:"2000000"`
	Tab      string `name:"tab" help:"Tab title or ID to read (omit for default behavior)"`
	AllTabs  bool   `name:"all-tabs" help:"Show all tabs with headers"`
}

func (c *DocsCatCmd) Run(ctx context.Context, flags *RootFlags) error {
	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	id := strings.TrimSpace(c.DocID)
	if id == "" {
		return usage("empty docId")
	}

	svc, err := newDocsService(ctx, account)
	if err != nil {
		return err
	}

	// Use tabs API when --tab or --all-tabs is specified.
	if c.Tab != "" || c.AllTabs {
		doc, err := svc.Documents.Get(id).
			IncludeTabsContent(true).
			Context(ctx).
			Do()
		if err != nil {
			if isDocsNotFound(err) {
				return fmt.Errorf("doc not found or not a Google Doc (id=%s)", id)
			}
			return err
		}
		if doc == nil {
			return errors.New("doc not found")
		}

		tabs := flattenTabs(doc.Tabs)

		if c.Tab != "" {
			tab := findTab(tabs, c.Tab)
			if tab == nil {
				return fmt.Errorf("tab not found: %s", c.Tab)
			}
			text := tabPlainText(tab, c.MaxBytes)
			if outfmt.IsJSON(ctx) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{
					"tab": tabJSON(tab, text),
				})
			}
			_, err = io.WriteString(os.Stdout, text)
			return err
		}

		// --all-tabs
		if outfmt.IsJSON(ctx) {
			var out []map[string]any
			for _, tab := range tabs {
				text := tabPlainText(tab, c.MaxBytes)
				out = append(out, tabJSON(tab, text))
			}
			return outfmt.WriteJSON(os.Stdout, map[string]any{"tabs": out})
		}

		for i, tab := range tabs {
			title := tabTitle(tab)
			if i > 0 {
				fmt.Fprintln(os.Stdout)
			}
			fmt.Fprintf(os.Stdout, "=== Tab: %s ===\n", title)
			text := tabPlainText(tab, c.MaxBytes)
			_, _ = io.WriteString(os.Stdout, text)
			if text != "" && !strings.HasSuffix(text, "\n") {
				fmt.Fprintln(os.Stdout)
			}
		}
		return nil
	}

	// Default: original behavior (no tabs API).
	doc, err := svc.Documents.Get(id).
		Context(ctx).
		Do()
	if err != nil {
		if isDocsNotFound(err) {
			return fmt.Errorf("doc not found or not a Google Doc (id=%s)", id)
		}
		return err
	}
	if doc == nil {
		return errors.New("doc not found")
	}

	text := docsPlainText(doc, c.MaxBytes)

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(os.Stdout, map[string]any{"text": text})
	}
	_, err = io.WriteString(os.Stdout, text)
	return err
}

type DocsUpdateCmd struct {
	DocID       string `arg:"" name:"docId" help:"Doc ID"`
	Content     string `name:"content" help:"Text content to insert (mutually exclusive with --content-file)"`
	ContentFile string `name:"content-file" help:"File containing text content to insert"`
	Format      string `name:"format" help:"Content format: plain|markdown" default:"plain"`
	Append      bool   `name:"append" help:"Append to end of document instead of replacing all content"`
	Debug       bool   `name:"debug" help:"Enable debug output for markdown formatter"`
}

const (
	docsContentFormatPlain    = "plain"
	docsContentFormatMarkdown = "markdown"
)

func (c *DocsUpdateCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	if c.Debug {
		debugMarkdown = true
	}

	id := strings.TrimSpace(c.DocID)
	if id == "" {
		return usage("empty docId")
	}

	var content string
	switch {
	case c.ContentFile != "":
		var data []byte
		data, err = os.ReadFile(c.ContentFile)
		if err != nil {
			return fmt.Errorf("read content file: %w", err)
		}
		content = string(data)
	case c.Content != "":
		content = c.Content
	default:
		return usage("either --content or --content-file is required")
	}

	format := strings.ToLower(strings.TrimSpace(c.Format))
	if format == "" {
		format = docsContentFormatPlain
	}
	switch format {
	case docsContentFormatPlain, docsContentFormatMarkdown:
	default:
		return usage("format must be plain or markdown")
	}

	svc, err := newDocsService(ctx, account)
	if err != nil {
		return err
	}

	doc, err := svc.Documents.Get(id).
		Context(ctx).
		Do()
	if err != nil {
		if isDocsNotFound(err) {
			return fmt.Errorf("doc not found or not a Google Doc (id=%s)", id)
		}
		return err
	}
	if doc == nil {
		return errors.New("doc not found")
	}

	insertIndex := int64(1)
	if c.Append && doc.Body != nil && len(doc.Body.Content) > 0 {
		lastEl := doc.Body.Content[len(doc.Body.Content)-1]
		if lastEl != nil && lastEl.EndIndex > 1 {
			insertIndex = lastEl.EndIndex - 1
		}
	}

	baseIndex := int64(1)
	if c.Append {
		baseIndex = insertIndex
	}

	var requests []*docs.Request
	var textToInsert string
	var formattingRequests []*docs.Request
	var tables []TableData

	if format == docsContentFormatMarkdown {
		elements := ParseMarkdown(content)
		formattingRequests, textToInsert, tables = MarkdownToDocsRequests(elements, baseIndex)
	} else {
		textToInsert = content
	}

	if c.Append {
		requests = append(requests, &docs.Request{
			InsertText: &docs.InsertTextRequest{
				Location: &docs.Location{Index: insertIndex},
				Text:     textToInsert,
			},
		})
		if format == docsContentFormatMarkdown {
			requests = append(requests, formattingRequests...)
		}
	} else {
		if doc.Body != nil && len(doc.Body.Content) > 0 {
			lastEl := doc.Body.Content[len(doc.Body.Content)-1]
			if lastEl != nil && lastEl.EndIndex > 2 {
				endIdx := lastEl.EndIndex - 1
				requests = append(requests, &docs.Request{
					DeleteContentRange: &docs.DeleteContentRangeRequest{
						Range: &docs.Range{
							StartIndex: 1,
							EndIndex:   endIdx,
						},
					},
				})
			}
		}

		requests = append(requests, &docs.Request{
			InsertText: &docs.InsertTextRequest{
				Location: &docs.Location{Index: 1},
				Text:     textToInsert,
			},
		})

		if format == docsContentFormatMarkdown {
			requests = append(requests, formattingRequests...)
		}
	}

	_, err = svc.Documents.BatchUpdate(id, &docs.BatchUpdateDocumentRequest{
		Requests: requests,
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("update document: %w", err)
	}

	if len(tables) > 0 {
		tableInserter := NewTableInserter(svc, id)
		tableOffset := int64(0)
		for _, table := range tables {
			tableIndex := table.StartIndex + tableOffset
			tableEnd, err := tableInserter.InsertNativeTable(ctx, tableIndex, table.Cells)
			if err != nil {
				return fmt.Errorf("insert native table: %w", err)
			}
			if tableEnd > tableIndex {
				tableOffset += (tableEnd - tableIndex) - 1
			}
		}
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(os.Stdout, map[string]any{
			"success": true,
			"docId":   id,
			"action":  map[string]any{"append": c.Append},
		})
	}

	action := "Updated"
	if c.Append {
		action = "Appended to"
	}
	u.Out().Printf("%s document %s", action, id)
	return nil
}

type DocsListTabsCmd struct {
	DocID string `arg:"" name:"docId" help:"Doc ID"`
}

func (c *DocsListTabsCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	id := strings.TrimSpace(c.DocID)
	if id == "" {
		return usage("empty docId")
	}

	svc, err := newDocsService(ctx, account)
	if err != nil {
		return err
	}

	doc, err := svc.Documents.Get(id).
		IncludeTabsContent(true).
		Context(ctx).
		Do()
	if err != nil {
		if isDocsNotFound(err) {
			return fmt.Errorf("doc not found or not a Google Doc (id=%s)", id)
		}
		return err
	}
	if doc == nil {
		return errors.New("doc not found")
	}

	tabs := flattenTabs(doc.Tabs)

	if outfmt.IsJSON(ctx) {
		var out []map[string]any
		for _, tab := range tabs {
			out = append(out, tabInfoJSON(tab))
		}
		return outfmt.WriteJSON(os.Stdout, map[string]any{"tabs": out})
	}

	u.Out().Printf("ID\tTITLE\tINDEX")
	for _, tab := range tabs {
		if tab.TabProperties != nil {
			u.Out().Printf("%s\t%s\t%d",
				tab.TabProperties.TabId,
				tab.TabProperties.Title,
				tab.TabProperties.Index,
			)
		}
	}
	return nil
}

func docsWebViewLink(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	return "https://docs.google.com/document/d/" + id + "/edit"
}

func docsPlainText(doc *docs.Document, maxBytes int64) string {
	if doc == nil || doc.Body == nil {
		return ""
	}

	var buf bytes.Buffer
	for _, el := range doc.Body.Content {
		if !appendDocsElementText(&buf, maxBytes, el) {
			break
		}
	}

	return buf.String()
}

func appendDocsElementText(buf *bytes.Buffer, maxBytes int64, el *docs.StructuralElement) bool {
	if el == nil {
		return true
	}

	switch {
	case el.Paragraph != nil:
		for _, p := range el.Paragraph.Elements {
			if p.TextRun == nil {
				continue
			}
			if !appendLimited(buf, maxBytes, p.TextRun.Content) {
				return false
			}
		}
	case el.Table != nil:
		for rowIdx, row := range el.Table.TableRows {
			if rowIdx > 0 {
				if !appendLimited(buf, maxBytes, "\n") {
					return false
				}
			}
			for cellIdx, cell := range row.TableCells {
				if cellIdx > 0 {
					if !appendLimited(buf, maxBytes, "\t") {
						return false
					}
				}
				for _, content := range cell.Content {
					if !appendDocsElementText(buf, maxBytes, content) {
						return false
					}
				}
			}
		}
	case el.TableOfContents != nil:
		for _, content := range el.TableOfContents.Content {
			if !appendDocsElementText(buf, maxBytes, content) {
				return false
			}
		}
	}

	return true
}

func appendLimited(buf *bytes.Buffer, maxBytes int64, s string) bool {
	if maxBytes <= 0 {
		_, _ = buf.WriteString(s)
		return true
	}

	remaining := int(maxBytes) - buf.Len()
	if remaining <= 0 {
		return false
	}
	if len(s) > remaining {
		_, _ = buf.WriteString(s[:remaining])
		return false
	}
	_, _ = buf.WriteString(s)
	return true
}

// flattenTabs recursively collects all tabs (including nested child tabs)
// into a flat slice in document order.
func flattenTabs(tabs []*docs.Tab) []*docs.Tab {
	var result []*docs.Tab
	for _, tab := range tabs {
		if tab == nil {
			continue
		}
		result = append(result, tab)
		if len(tab.ChildTabs) > 0 {
			result = append(result, flattenTabs(tab.ChildTabs)...)
		}
	}
	return result
}

// findTab looks up a tab by title or ID (case-insensitive title match).
func findTab(tabs []*docs.Tab, query string) *docs.Tab {
	query = strings.TrimSpace(query)
	// Try exact ID match first.
	for _, tab := range tabs {
		if tab.TabProperties != nil && tab.TabProperties.TabId == query {
			return tab
		}
	}
	// Fall back to case-insensitive title match.
	lower := strings.ToLower(query)
	for _, tab := range tabs {
		if tab.TabProperties != nil && strings.ToLower(tab.TabProperties.Title) == lower {
			return tab
		}
	}
	return nil
}

// tabTitle returns the display title for a tab.
func tabTitle(tab *docs.Tab) string {
	if tab.TabProperties != nil && tab.TabProperties.Title != "" {
		return tab.TabProperties.Title
	}
	return "(untitled)"
}

// tabPlainText extracts plain text from a tab's document content.
func tabPlainText(tab *docs.Tab, maxBytes int64) string {
	if tab == nil || tab.DocumentTab == nil || tab.DocumentTab.Body == nil {
		return ""
	}
	var buf bytes.Buffer
	for _, el := range tab.DocumentTab.Body.Content {
		if !appendDocsElementText(&buf, maxBytes, el) {
			break
		}
	}
	return buf.String()
}

// tabJSON returns a JSON-friendly map for a tab with its text content.
func tabJSON(tab *docs.Tab, text string) map[string]any {
	m := map[string]any{"text": text}
	if tab.TabProperties != nil {
		m["id"] = tab.TabProperties.TabId
		m["title"] = tab.TabProperties.Title
		m["index"] = tab.TabProperties.Index
	}
	return m
}

// tabInfoJSON returns a JSON-friendly map for a tab's metadata (no content).
func tabInfoJSON(tab *docs.Tab) map[string]any {
	m := map[string]any{}
	if tab.TabProperties != nil {
		m["id"] = tab.TabProperties.TabId
		m["title"] = tab.TabProperties.Title
		m["index"] = tab.TabProperties.Index
		if tab.TabProperties.NestingLevel > 0 {
			m["nestingLevel"] = tab.TabProperties.NestingLevel
		}
		if tab.TabProperties.ParentTabId != "" {
			m["parentTabId"] = tab.TabProperties.ParentTabId
		}
	}
	return m
}

func isDocsNotFound(err error) bool {
	var apiErr *gapi.Error
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Code == http.StatusNotFound
}
