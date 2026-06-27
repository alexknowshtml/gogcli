package cmd

import (
	"context"
	"os"
	"strings"

	"google.golang.org/api/docs/v1"

	"github.com/steipete/gogcli/internal/outfmt"
	"github.com/steipete/gogcli/internal/ui"
)

type DocsAddTabCmd struct {
	DocID   string `arg:"" name:"docId" help:"Document ID"`
	TabName string `arg:"" name:"tabName" help:"Name for the new tab"`
	Index   *int64 `name:"index" help:"Zero-based tab index for the new tab"`
}

func (c *DocsAddTabCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)

	docID := normalizeGoogleID(strings.TrimSpace(c.DocID))
	tabName := strings.TrimSpace(c.TabName)
	if docID == "" {
		return usage("empty docId")
	}
	if tabName == "" {
		return usage("empty tabName")
	}

	payload := map[string]any{
		"doc_id":   docID,
		"tab_name": tabName,
	}
	if c.Index != nil {
		payload["index"] = *c.Index
	}
	if err := dryRunExit(ctx, flags, "docs.add-tab", payload); err != nil {
		return err
	}

	svc, err := requireDocsService(ctx, flags)
	if err != nil {
		return err
	}

	props := &docs.TabProperties{Title: tabName}
	if c.Index != nil {
		props.Index = *c.Index
		props.ForceSendFields = []string{"Index"}
	}

	req := &docs.BatchUpdateDocumentRequest{
		Requests: []*docs.Request{
			{
				AddDocumentTab: &docs.AddDocumentTabRequest{
					TabProperties: props,
				},
			},
		},
	}

	resp, err := svc.Documents.BatchUpdate(docID, req).Context(ctx).Do()
	if err != nil {
		return err
	}

	var newTabID string
	var newIndex int64
	hasNewIndex := false
	if len(resp.Replies) > 0 && resp.Replies[0].AddDocumentTab != nil && resp.Replies[0].AddDocumentTab.TabProperties != nil {
		tabProps := resp.Replies[0].AddDocumentTab.TabProperties
		newTabID = tabProps.TabId
		newIndex = tabProps.Index
		hasNewIndex = true
	}

	if outfmt.IsJSON(ctx) {
		out := map[string]any{
			"documentId": docID,
			"tabName":    tabName,
			"title":      tabName,
			"tabId":      newTabID,
		}
		if c.Index != nil || hasNewIndex {
			out["index"] = newIndex
		}
		return outfmt.WriteJSON(ctx, os.Stdout, out)
	}

	if c.Index != nil || hasNewIndex {
		u.Out().Printf("Added tab %q (tabId %s, index %d) to document %s", tabName, newTabID, newIndex, docID)
		return nil
	}
	u.Out().Printf("Added tab %q (tabId %s) to document %s", tabName, newTabID, docID)
	return nil
}
