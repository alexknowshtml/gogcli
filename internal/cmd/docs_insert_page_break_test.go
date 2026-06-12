package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/api/docs/v1"
)

func pageBreakDocWithEndIndex(end int64) map[string]any {
	return map[string]any{
		"documentId": "doc1",
		"body": map[string]any{
			"content": []any{
				map[string]any{"startIndex": 0, "endIndex": end},
			},
		},
	}
}

func TestDocsInsertPageBreakCmd_ExplicitIndex(t *testing.T) {
	t.Parallel()

	var batchRequests [][]*docs.Request
	var getCalls int

	docSvc, cleanup := newDocsServiceForTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/documents/"):
			getCalls++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pageBreakDocWithEndIndex(50))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, ":batchUpdate"):
			var req docs.BatchUpdateDocumentRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode: %v", err)
			}
			batchRequests = append(batchRequests, req.Requests)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"documentId": "doc1"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer cleanup()

	flags := &RootFlags{Account: "a@b.com"}
	ctx := withDocsTestService(newCmdRuntimeOutputContext(t, io.Discard, io.Discard), docSvc)

	if err := runKong(t, &DocsInsertPageBreakCmd{}, []string{"doc1", "--index", "7"}, ctx, flags); err != nil {
		t.Fatalf("insert-page-break: %v", err)
	}

	if getCalls != 0 {
		t.Fatalf("explicit --index should not GET the doc, got %d GET calls", getCalls)
	}
	if len(batchRequests) != 1 || len(batchRequests[0]) != 1 {
		t.Fatalf("unexpected requests: %#v", batchRequests)
	}
	pb := batchRequests[0][0].InsertPageBreak
	if pb == nil {
		t.Fatalf("expected InsertPageBreak, got %#v", batchRequests[0][0])
	}
	if pb.Location == nil || pb.Location.Index != 7 {
		t.Fatalf("expected page break at index 7, got %#v", pb.Location)
	}
}

func TestDocsInsertPageBreakCmd_DefaultsToEndOfDoc(t *testing.T) {
	t.Parallel()

	var batchRequests [][]*docs.Request
	var getCalls int

	docSvc, cleanup := newDocsServiceForTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/documents/"):
			getCalls++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pageBreakDocWithEndIndex(42))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, ":batchUpdate"):
			var req docs.BatchUpdateDocumentRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode: %v", err)
			}
			batchRequests = append(batchRequests, req.Requests)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"documentId": "doc1"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer cleanup()

	flags := &RootFlags{Account: "a@b.com"}
	ctx := withDocsTestService(newCmdRuntimeOutputContext(t, io.Discard, io.Discard), docSvc)

	if err := runKong(t, &DocsInsertPageBreakCmd{}, []string{"doc1"}, ctx, flags); err != nil {
		t.Fatalf("insert-page-break: %v", err)
	}

	if getCalls != 1 {
		t.Fatalf("expected 1 GET to resolve end, got %d", getCalls)
	}
	pb := batchRequests[0][0].InsertPageBreak
	if pb == nil || pb.Location == nil {
		t.Fatalf("expected InsertPageBreak with Location, got %#v", batchRequests[0][0])
	}
	if pb.Location.Index != 41 {
		t.Fatalf("expected page break at end-1 (41), got %d", pb.Location.Index)
	}
}

func TestDocsInsertPageBreakCmd_AtEndFlag(t *testing.T) {
	t.Parallel()

	var batchRequests [][]*docs.Request
	var getCalls int

	docSvc, cleanup := newDocsServiceForTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/documents/"):
			getCalls++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pageBreakDocWithEndIndex(100))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, ":batchUpdate"):
			var req docs.BatchUpdateDocumentRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode: %v", err)
			}
			batchRequests = append(batchRequests, req.Requests)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"documentId": "doc1"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer cleanup()

	flags := &RootFlags{Account: "a@b.com"}
	ctx := withDocsTestService(newCmdRuntimeOutputContext(t, io.Discard, io.Discard), docSvc)

	if err := runKong(t, &DocsInsertPageBreakCmd{}, []string{"doc1", "--at-end"}, ctx, flags); err != nil {
		t.Fatalf("insert-page-break: %v", err)
	}

	if getCalls != 1 {
		t.Fatalf("expected 1 GET to resolve end, got %d", getCalls)
	}
	if got := batchRequests[0][0].InsertPageBreak.Location.Index; got != 99 {
		t.Fatalf("expected end-1 (99), got %d", got)
	}
}

func TestDocsInsertPageBreakCmd_AtEndAndIndexRejected(t *testing.T) {
	t.Parallel()

	flags := &RootFlags{Account: "a@b.com"}
	ctx := newCmdRuntimeOutputContext(t, io.Discard, io.Discard)
	err := runKong(t, &DocsInsertPageBreakCmd{}, []string{"doc1", "--at-end", "--index", "5"}, ctx, flags)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected mutual-exclusion error, got %v", err)
	}
}

func TestDocsInsertPageBreakCmd_NegativeIndexRejected(t *testing.T) {
	t.Parallel()

	flags := &RootFlags{Account: "a@b.com"}
	ctx := newCmdRuntimeOutputContext(t, io.Discard, io.Discard)
	err := runKong(t, &DocsInsertPageBreakCmd{}, []string{"doc1", "--index=-1"}, ctx, flags)
	if err == nil || !strings.Contains(err.Error(), "--index must be >= 1") {
		t.Fatalf("expected --index validation error, got %v", err)
	}
}

func TestDocsInsertPageBreakCmd_ZeroIndexRejected(t *testing.T) {
	t.Parallel()

	flags := &RootFlags{Account: "a@b.com"}
	ctx := newCmdRuntimeOutputContext(t, io.Discard, io.Discard)
	err := runKong(t, &DocsInsertPageBreakCmd{}, []string{"doc1", "--index", "0"}, ctx, flags)
	if err == nil || !strings.Contains(err.Error(), "--index must be >= 1") {
		t.Fatalf("expected --index validation error, got %v", err)
	}
}

func TestDocsInsertPageBreakCmd_WithTab(t *testing.T) {
	t.Parallel()

	var batchRequests [][]*docs.Request

	docSvc, cleanup := newDocsServiceForTest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/documents/"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(tabsDocWithEndIndex())
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, ":batchUpdate"):
			var req docs.BatchUpdateDocumentRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode: %v", err)
			}
			batchRequests = append(batchRequests, req.Requests)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"documentId": "doc1"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer cleanup()

	flags := &RootFlags{Account: "a@b.com"}
	ctx := withDocsTestService(newCmdRuntimeOutputContext(t, io.Discard, io.Discard), docSvc)

	if err := runKong(t, &DocsInsertPageBreakCmd{}, []string{"doc1", "--index", "5", "--tab", "Second"}, ctx, flags); err != nil {
		t.Fatalf("insert-page-break: %v", err)
	}

	pb := batchRequests[0][0].InsertPageBreak
	if pb == nil || pb.Location == nil {
		t.Fatalf("expected InsertPageBreak with Location, got %#v", batchRequests[0][0])
	}
	if pb.Location.TabId != "t.second" || pb.Location.Index != 5 {
		t.Fatalf("unexpected location: %#v", pb.Location)
	}
}
