package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

func TestReplyHeaders(t *testing.T) {
	type hdr struct {
		Name  string
		Value string
	}
	type msg struct {
		ThreadID string
		Headers  []hdr
	}

	messages := map[string]msg{
		"m1": {ThreadID: "t1", Headers: []hdr{{Name: "Message-ID", Value: "<id1@example.com>"}}},
		"m2": {ThreadID: "t2", Headers: []hdr{
			{Name: "Message-Id", Value: "<id2@example.com>"},
			{Name: "References", Value: "<ref@example.com>"},
		}},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/gmail/v1/users/me/messages/") {
			http.NotFound(w, r)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/gmail/v1/users/me/messages/")
		m, ok := messages[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		hs := make([]map[string]any, 0, len(m.Headers))
		for _, h := range m.Headers {
			hs = append(hs, map[string]any{"name": h.Name, "value": h.Value})
		}
		resp := map[string]any{
			"id":       id,
			"threadId": m.ThreadID,
			"payload": map[string]any{
				"headers": hs,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	svc, err := gmail.NewService(context.Background(),
		option.WithoutAuthentication(),
		option.WithHTTPClient(srv.Client()),
		option.WithEndpoint(srv.URL+"/"),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	inReplyTo, refs, threadID, err := replyHeaders(cmd, svc, "m1")
	if err != nil {
		t.Fatalf("replyHeaders: %v", err)
	}
	if inReplyTo != "<id1@example.com>" || refs != "<id1@example.com>" || threadID != "t1" {
		t.Fatalf("unexpected: inReplyTo=%q refs=%q thread=%q", inReplyTo, refs, threadID)
	}

	inReplyTo, refs, threadID, err = replyHeaders(cmd, svc, "m2")
	if err != nil {
		t.Fatalf("replyHeaders: %v", err)
	}
	if inReplyTo != "<id2@example.com>" {
		t.Fatalf("unexpected inReplyTo: %q", inReplyTo)
	}
	if !strings.Contains(refs, "<ref@example.com>") || !strings.Contains(refs, "<id2@example.com>") {
		t.Fatalf("unexpected refs: %q", refs)
	}
	if threadID != "t2" {
		t.Fatalf("unexpected thread: %q", threadID)
	}
}
