package cmd

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestExecute_DriveGet_Text(t *testing.T) {
	svc, closeSrv := newDriveTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/files/id1") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":           "id1",
			"name":         "Doc",
			"mimeType":     "application/pdf",
			"size":         "1024",
			"createdTime":  "2025-12-11T00:00:00Z",
			"modifiedTime": "2025-12-12T14:37:47Z",
			"starred":      true,
			"webViewLink":  "https://example.com/id1",
		})
	}))
	defer closeSrv()

	result := executeWithDriveTestService(t, []string{"--account", "a@b.com", "drive", "get", "id1"}, svc)
	if result.err != nil {
		t.Fatalf("Execute: %v", result.err)
	}
	if !strings.Contains(result.stdout, "id\tid1") || !strings.Contains(result.stdout, "name\tDoc") || !strings.Contains(result.stdout, "starred\ttrue") || !strings.Contains(result.stdout, "link\thttps://example.com/id1") {
		t.Fatalf("unexpected out=%q", result.stdout)
	}
}

func TestExecute_DrivePermissions_Text_NoPermissions(t *testing.T) {
	svc, closeSrv := newDriveTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/permissions") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"permissions": []any{}})
	}))
	defer closeSrv()

	result := executeWithDriveTestService(t, []string{"--account", "a@b.com", "drive", "permissions", "id1"}, svc)
	if result.err != nil {
		t.Fatalf("Execute: %v", result.err)
	}
	if !strings.Contains(result.stderr, "No permissions") {
		t.Fatalf("unexpected stderr=%q", result.stderr)
	}
}

func TestExecute_DrivePermissions_Text_WithPermissions(t *testing.T) {
	svc, closeSrv := newDriveTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/permissions") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"permissions": []map[string]any{
				{"id": "p1", "type": "anyone", "role": "reader"},
				{"id": "p2", "type": "user", "role": "writer", "emailAddress": "a@b.com"},
			},
		})
	}))
	defer closeSrv()

	result := executeWithDriveTestService(t, []string{"--account", "a@b.com", "drive", "permissions", "id1"}, svc)
	if result.err != nil {
		t.Fatalf("Execute: %v", result.err)
	}
	if !strings.Contains(result.stdout, "ID") || !strings.Contains(result.stdout, "EMAIL") || !strings.Contains(result.stdout, "p1") || !strings.Contains(result.stdout, "p2") || !strings.Contains(result.stdout, "a@b.com") {
		t.Fatalf("unexpected out=%q", result.stdout)
	}
}

func TestExecute_DriveSearch_Text(t *testing.T) {
	svc, closeSrv := newDriveTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/files") || strings.Contains(r.URL.Path, "/files/") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"files": []map[string]any{
				{"id": "id1", "name": "Doc", "mimeType": "application/pdf", "size": "1", "modifiedTime": "2025-12-12T14:37:47Z"},
			},
			"nextPageToken": "npt",
		})
	}))
	defer closeSrv()

	result := executeWithDriveTestService(t, []string{"--account", "a@b.com", "drive", "search", "Doc", "--max", "1"}, svc)
	if result.err != nil {
		t.Fatalf("Execute: %v", result.err)
	}
	if !strings.Contains(result.stderr, "# Next page: --page npt") {
		t.Fatalf("unexpected stderr=%q", result.stderr)
	}
	if !strings.Contains(result.stdout, "ID") || !strings.Contains(result.stdout, "Doc") || !strings.Contains(result.stdout, "file") || !strings.Contains(result.stdout, "2025-12-12") {
		t.Fatalf("unexpected out=%q", result.stdout)
	}
}
