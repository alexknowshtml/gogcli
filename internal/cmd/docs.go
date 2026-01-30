package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steipete/gogcli/internal/googleapi"
	"github.com/steipete/gogcli/internal/outfmt"
	"github.com/steipete/gogcli/internal/ui"
	"google.golang.org/api/docs/v1"
	"google.golang.org/api/drive/v3"
	gapi "google.golang.org/api/googleapi"
)

var newDocsService = googleapi.NewDocs

func newDocsCmd(flags *rootFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "docs",
		Short: "Google Docs commands",
	}
	cmd.AddCommand(newDocsExportCmd(flags))
	cmd.AddCommand(newDocsInfoCmd(flags))
	cmd.AddCommand(newDocsCreateCmd(flags))
	cmd.AddCommand(newDocsCopyCmd(flags))
	cmd.AddCommand(newDocsCatCmd(flags))
	cmd.AddCommand(newDocsWriteCmd(flags))
	return cmd
}

func newDocsExportCmd(flags *rootFlags) *cobra.Command {
	return newExportViaDriveCmd(flags, exportViaDriveOptions{
		Use:           "export <docId>",
		Short:         "Export a Google Doc (pdf|docx|txt)",
		ArgName:       "docId",
		ExpectedMime:  "application/vnd.google-apps.document",
		KindLabel:     "Google Doc",
		DefaultFormat: "pdf",
		FormatHelp:    "Export format: pdf|docx|txt",
	})
}

func newDocsInfoCmd(flags *rootFlags) *cobra.Command {
	return newInfoViaDriveCmd(flags, infoViaDriveOptions{
		Use:          "info <docId>",
		Short:        "Get Google Doc metadata",
		ArgName:      "docId",
		ExpectedMime: "application/vnd.google-apps.document",
		KindLabel:    "Google Doc",
	})
}

func newDocsCreateCmd(flags *rootFlags) *cobra.Command {
	var parent string

	cmd := &cobra.Command{
		Use:   "create <title>",
		Short: "Create a Google Doc",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}

			title := strings.TrimSpace(args[0])
			if title == "" {
				return usage("empty title")
			}

			svc, err := newDriveService(cmd.Context(), account)
			if err != nil {
				return err
			}

			f := &drive.File{
				Name:     title,
				MimeType: "application/vnd.google-apps.document",
			}
			parent = strings.TrimSpace(parent)
			if parent != "" {
				f.Parents = []string{parent}
			}

			created, err := svc.Files.Create(f).
				SupportsAllDrives(true).
				Fields("id, name, mimeType, webViewLink").
				Context(cmd.Context()).
				Do()
			if err != nil {
				return err
			}
			if created == nil {
				return errors.New("create failed")
			}

			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{"file": created})
			}

			u.Out().Printf("id\t%s", created.Id)
			u.Out().Printf("name\t%s", created.Name)
			u.Out().Printf("mime\t%s", created.MimeType)
			if created.WebViewLink != "" {
				u.Out().Printf("link\t%s", created.WebViewLink)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&parent, "parent", "", "Destination folder ID")
	return cmd
}

func newDocsCopyCmd(flags *rootFlags) *cobra.Command {
	return newCopyViaDriveCmd(flags, copyViaDriveOptions{
		Use:          "copy <docId> <title>",
		Short:        "Copy a Google Doc",
		ArgName:      "docId",
		ExpectedMime: "application/vnd.google-apps.document",
		KindLabel:    "Google Doc",
	})
}

func newDocsCatCmd(flags *rootFlags) *cobra.Command {
	var maxBytes int64

	cmd := &cobra.Command{
		Use:   "cat <docId>",
		Short: "Print a Google Doc as plain text",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}

			id := strings.TrimSpace(args[0])
			if id == "" {
				return usage("empty docId")
			}

			svc, err := newDriveService(cmd.Context(), account)
			if err != nil {
				return err
			}

			meta, err := svc.Files.Get(id).
				SupportsAllDrives(true).
				Fields("id, mimeType").
				Context(cmd.Context()).
				Do()
			if err != nil {
				return err
			}
			if meta == nil {
				return errors.New("file not found")
			}
			if meta.MimeType != "application/vnd.google-apps.document" {
				return fmt.Errorf("file is not a Google Doc (mimeType=%q)", meta.MimeType)
			}

			resp, err := driveExportDownload(cmd.Context(), svc, id, "text/plain")
			if err != nil {
				return err
			}
			if resp == nil || resp.Body == nil {
				return errors.New("empty response")
			}
			defer resp.Body.Close()

			var r io.Reader = resp.Body
			if maxBytes > 0 {
				r = io.LimitReader(resp.Body, maxBytes)
			}
			b, err := io.ReadAll(r)
			if err != nil {
				return err
			}

			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{"text": string(b)})
			}
			_, err = os.Stdout.Write(b)
			return err
		},
	}

	cmd.Flags().Int64Var(&maxBytes, "max-bytes", 2_000_000, "Max bytes to read (0 = unlimited)")
	return cmd
}

func newDocsWriteCmd(flags *rootFlags) *cobra.Command {
	var contentFile string
	var replace bool
	var markdown bool

	cmd := &cobra.Command{
		Use:   "write <docId> [content]",
		Short: "Write content to a Google Doc",
		Long: `Write or append content to a Google Doc.

Content can be provided via:
  - Argument: gog docs write <docId> "Your content here"
  - File: gog docs write <docId> --file content.md
  - Stdin: echo "content" | gog docs write <docId>

By default, content is appended to the end of the document.
Use --replace to clear the document first.
Use --markdown to convert markdown to Google Docs formatting (requires --replace).`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}

			docID := strings.TrimSpace(args[0])
			if docID == "" {
				return usage("empty docId")
			}

			// Get content from args, file, or stdin
			var content string
			if len(args) > 1 {
				content = strings.Join(args[1:], " ")
			} else if contentFile != "" {
				data, err := os.ReadFile(contentFile)
				if err != nil {
					return fmt.Errorf("reading file: %w", err)
				}
				content = string(data)
			} else {
				// Check if stdin has data
				stat, _ := os.Stdin.Stat()
				if (stat.Mode() & os.ModeCharDevice) == 0 {
					data, err := io.ReadAll(os.Stdin)
					if err != nil {
						return fmt.Errorf("reading stdin: %w", err)
					}
					content = string(data)
				}
			}

			if content == "" {
				return usage("no content provided (use argument, --file, or stdin)")
			}

			// Markdown mode uses Drive API to convert markdown to Google Docs format
			if markdown {
				if !replace {
					return usage("--markdown requires --replace (cannot append formatted markdown)")
				}

				driveSvc, err := newDriveService(cmd.Context(), account)
				if err != nil {
					return err
				}

				// Update the file content with markdown mime type - Drive will convert it
				updated, err := driveSvc.Files.Update(docID, &drive.File{}).
					Media(strings.NewReader(content), gapi.ContentType("text/markdown")).
					SupportsAllDrives(true).
					Fields("id, name, webViewLink").
					Context(cmd.Context()).
					Do()
				if err != nil {
					return fmt.Errorf("writing markdown to document: %w", err)
				}

				if outfmt.IsJSON(cmd.Context()) {
					return outfmt.WriteJSON(os.Stdout, map[string]any{
						"documentId": updated.Id,
						"written":    len(content),
						"replaced":   true,
						"markdown":   true,
					})
				}

				u.Out().Printf("documentId\t%s", updated.Id)
				u.Out().Printf("written\t%d bytes", len(content))
				u.Out().Printf("mode\treplaced (markdown converted)")
				if updated.WebViewLink != "" {
					u.Out().Printf("link\t%s", updated.WebViewLink)
				}
				return nil
			}

			// Plain text mode uses Docs API
			svc, err := newDocsService(cmd.Context(), account)
			if err != nil {
				return err
			}

			var requests []*docs.Request

			if replace {
				// First, get the document to find content length
				doc, err := svc.Documents.Get(docID).Context(cmd.Context()).Do()
				if err != nil {
					return fmt.Errorf("getting document: %w", err)
				}

				// Calculate end index (content length minus 1 for the trailing newline)
				endIndex := doc.Body.Content[len(doc.Body.Content)-1].EndIndex - 1

				// Only delete if there's content to delete
				if endIndex > 1 {
					requests = append(requests, &docs.Request{
						DeleteContentRange: &docs.DeleteContentRangeRequest{
							Range: &docs.Range{
								StartIndex: 1,
								EndIndex:   endIndex,
							},
						},
					})
				}
			}

			// Insert text at end of document
			requests = append(requests, &docs.Request{
				InsertText: &docs.InsertTextRequest{
					Text:                  content,
					EndOfSegmentLocation: &docs.EndOfSegmentLocation{},
				},
			})

			result, err := svc.Documents.BatchUpdate(docID, &docs.BatchUpdateDocumentRequest{
				Requests: requests,
			}).Context(cmd.Context()).Do()
			if err != nil {
				return fmt.Errorf("writing to document: %w", err)
			}

			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{
					"documentId": result.DocumentId,
					"written":    len(content),
					"replaced":   replace,
				})
			}

			u.Out().Printf("documentId\t%s", result.DocumentId)
			u.Out().Printf("written\t%d bytes", len(content))
			if replace {
				u.Out().Printf("mode\treplaced")
			} else {
				u.Out().Printf("mode\tappended")
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&contentFile, "file", "f", "", "Read content from file")
	cmd.Flags().BoolVar(&replace, "replace", false, "Replace all content (default: append)")
	cmd.Flags().BoolVar(&markdown, "markdown", false, "Convert markdown to Google Docs formatting (requires --replace)")
	return cmd
}
