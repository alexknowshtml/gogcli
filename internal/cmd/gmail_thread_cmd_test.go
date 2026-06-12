package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGmailThreadModifyCmd_JSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && (strings.HasSuffix(r.URL.Path, "/users/me/labels") || strings.HasSuffix(r.URL.Path, "/gmail/v1/users/me/labels")):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"labels": []map[string]any{
					{"id": "INBOX", "name": "INBOX", "type": "system"},
					{"id": "Label_1", "name": "Custom", "type": "user"},
				},
			})
			return
		case r.Method == http.MethodPost && (strings.Contains(r.URL.Path, "/users/me/threads/") || strings.Contains(r.URL.Path, "/gmail/v1/users/me/threads/")) && strings.HasSuffix(r.URL.Path, "/modify"):
			var body struct {
				AddLabelIds    []string `json:"addLabelIds"`
				RemoveLabelIds []string `json:"removeLabelIds"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if len(body.AddLabelIds) != 1 || body.AddLabelIds[0] != "INBOX" {
				http.Error(w, "bad addLabelIds", http.StatusBadRequest)
				return
			}
			if len(body.RemoveLabelIds) != 1 || body.RemoveLabelIds[0] != "Label_1" {
				http.Error(w, "bad removeLabelIds", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{})
			return
		default:
			http.NotFound(w, r)
			return
		}
	}))
	defer srv.Close()

	svc := newGmailServiceFromServer(t, srv)
	flags := &RootFlags{Account: "a@b.com"}

	var jsonOut bytes.Buffer
	jsonCtx := withGmailTestService(newCmdRuntimeJSONOutputContext(t, &jsonOut, io.Discard), svc)
	if err := runKong(t, &GmailThreadModifyCmd{}, []string{
		"t1",
		"--add", "INBOX",
		"--remove", "Custom",
	}, jsonCtx, flags); err != nil {
		t.Fatalf("execute: %v", err)
	}

	var parsed struct {
		Modified      string   `json:"modified"`
		AddedLabels   []string `json:"addedLabels"`
		RemovedLabels []string `json:"removedLabels"`
	}
	if err := json.Unmarshal(jsonOut.Bytes(), &parsed); err != nil {
		t.Fatalf("json parse: %v\nout=%q", err, jsonOut.String())
	}
	if parsed.Modified != "t1" {
		t.Fatalf("unexpected modified: %q", parsed.Modified)
	}
	if len(parsed.AddedLabels) != 1 || parsed.AddedLabels[0] != "INBOX" {
		t.Fatalf("unexpected added labels: %#v", parsed.AddedLabels)
	}
	if len(parsed.RemovedLabels) != 1 || parsed.RemovedLabels[0] != "Label_1" {
		t.Fatalf("unexpected removed labels: %#v", parsed.RemovedLabels)
	}

	var plainOut bytes.Buffer
	plainCtx := withGmailTestService(newCmdRuntimeOutputContext(t, &plainOut, io.Discard), svc)
	if err := runKong(t, &GmailThreadModifyCmd{}, []string{
		"t1",
		"--add", "INBOX",
		"--remove", "Custom",
	}, plainCtx, flags); err != nil {
		t.Fatalf("execute plain: %v", err)
	}
	if !strings.Contains(plainOut.String(), "Modified thread") {
		t.Fatalf("unexpected plain output: %q", plainOut.String())
	}
}
