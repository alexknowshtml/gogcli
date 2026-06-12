package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/api/drive/v3"
)

func TestExecute_DriveMoreCommands_JSON(t *testing.T) {
	svc, closeSrv := newDriveTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/files") && r.Method == http.MethodGet:
			// files.list or files.get
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(path, "/files/") {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":          "id1",
					"name":        "Doc",
					"parents":     []string{"p0"},
					"webViewLink": "https://example.com/id1",
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"files": []map[string]any{
					{"id": "id1", "name": "Doc", "mimeType": "application/pdf"},
				},
				"nextPageToken": "npt",
			})
			return
		case strings.Contains(path, "/upload/drive/v3/files") && r.Method == http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":          "up1",
				"name":        "upload.bin",
				"mimeType":    "application/octet-stream",
				"webViewLink": "https://example.com/up1",
			})
			return
		case strings.Contains(path, "/files") && r.Method == http.MethodPost && !strings.Contains(path, "/permissions"):
			// mkdir
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":          "f1",
				"name":        "Folder",
				"webViewLink": "https://example.com/f1",
			})
			return
		case strings.Contains(path, "/files/id1") && r.Method == http.MethodDelete:
			if got := r.URL.Query().Get("supportsAllDrives"); got != "true" {
				t.Fatalf("expected supportsAllDrives=true, got: %q (raw=%q)", got, r.URL.RawQuery)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		case strings.Contains(path, "/files/id1") && (r.Method == http.MethodPatch || r.Method == http.MethodPut):
			w.Header().Set("Content-Type", "application/json")
			requireSupportsAllDrives(t, r)
			if addParents := r.URL.Query().Get("addParents"); addParents != "" {
				if addParents != "np" {
					t.Fatalf("expected addParents=np, got: %q", r.URL.RawQuery)
				}
				if got := r.URL.Query().Get("removeParents"); got != "p0" {
					t.Fatalf("expected removeParents=p0, got: %q", r.URL.RawQuery)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":          "id1",
					"name":        "New",
					"parents":     []string{"np"},
					"webViewLink": "https://example.com/id1",
				})
				return
			}
			body := readBody(t, r)
			if strings.Contains(body, "\"trashed\":true") {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":      "id1",
					"trashed": true,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":          "id1",
				"name":        "New",
				"parents":     []string{"p0"},
				"webViewLink": "https://example.com/id1",
			})
			return
		case strings.Contains(path, "/files/id1/permissions") && r.Method == http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "p1", "type": "anyone", "role": "reader"})
			return
		case strings.Contains(path, "/files/id1/permissions") && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"permissions": []map[string]any{
					{"id": "p1", "type": "anyone", "role": "reader"},
				},
			})
			return
		case strings.Contains(path, "/files/id1/permissions/p1") && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}))
	defer closeSrv()

	tmpFile := filepath.Join(t.TempDir(), "upload.bin")
	if err := os.WriteFile(tmpFile, []byte("abc"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"search", []string{"--json", "--account", "a@b.com", "drive", "search", "hello"}},
		{"upload", []string{"--json", "--account", "a@b.com", "drive", "upload", tmpFile, "--name", "upload.bin", "--parent", "np"}},
		{"mkdir", []string{"--json", "--account", "a@b.com", "drive", "mkdir", "Folder", "--parent", "np"}},
		{"rename", []string{"--json", "--account", "a@b.com", "drive", "rename", "id1", "New"}},
		{"move", []string{"--json", "--account", "a@b.com", "drive", "move", "id1", "--parent", "np"}},
		{"share", []string{"--json", "--force", "--account", "a@b.com", "drive", "share", "id1", "--anyone", "--role", "reader"}},
		{"permissions", []string{"--json", "--account", "a@b.com", "drive", "permissions", "id1"}},
		{"unshare", []string{"--json", "--force", "--account", "a@b.com", "drive", "unshare", "id1", "p1"}},
		{"delete", []string{"--json", "--force", "--account", "a@b.com", "drive", "delete", "id1"}},
	} {
		result := executeWithDriveTestService(t, tc.args, svc)
		if result.err != nil {
			t.Fatalf("%s: %v", tc.name, result.err)
		}
	}
}

func TestDriveShare_ValidationErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--account", "a@b.com", "drive", "share", "id1"},
		{"--account", "a@b.com", "drive", "share", "id1", "--anyone", "--role", "nope"},
	} {
		result := executeWithDriveTestServiceFactory(t, args, func(context.Context, string) (*drive.Service, error) {
			t.Fatal("Drive service should not be created for validation errors")
			return nil, errors.New("unexpected Drive service call")
		})
		if result.err == nil {
			t.Fatalf("expected error for %v", args)
		}
	}
}

func TestExecute_DriveMoreCommands_Text(t *testing.T) {
	svc, closeSrv := newDriveTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/files") && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(path, "/files/") {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":          "id1",
					"name":        "Doc",
					"parents":     []string{"p0"},
					"webViewLink": "https://example.com/id1",
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"files": []map[string]any{
					{"id": "id1", "name": "Doc", "mimeType": "application/pdf"},
				},
			})
			return
		case strings.Contains(path, "/upload/drive/v3/files") && r.Method == http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":          "up1",
				"name":        "upload.bin",
				"mimeType":    "application/octet-stream",
				"webViewLink": "https://example.com/up1",
			})
			return
		case strings.Contains(path, "/files") && r.Method == http.MethodPost && !strings.Contains(path, "/permissions"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":          "f1",
				"name":        "Folder",
				"webViewLink": "https://example.com/f1",
			})
			return
		case strings.Contains(path, "/files/id1") && r.Method == http.MethodDelete:
			if got := r.URL.Query().Get("supportsAllDrives"); got != "true" {
				t.Fatalf("expected supportsAllDrives=true, got: %q (raw=%q)", got, r.URL.RawQuery)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		case strings.Contains(path, "/files/id1") && (r.Method == http.MethodPatch || r.Method == http.MethodPut):
			w.Header().Set("Content-Type", "application/json")
			requireSupportsAllDrives(t, r)
			body := readBody(t, r)
			if strings.Contains(body, "\"trashed\":true") {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":      "id1",
					"trashed": true,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":          "id1",
				"name":        "New",
				"parents":     []string{"p0"},
				"webViewLink": "https://example.com/id1",
			})
			return
		case strings.Contains(path, "/files/id1/permissions") && r.Method == http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "p1", "type": "anyone", "role": "reader"})
			return
		case strings.Contains(path, "/files/id1/permissions") && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"permissions": []map[string]any{
					{"id": "p1", "type": "anyone", "role": "reader"},
				},
			})
			return
		case strings.Contains(path, "/files/id1/permissions/p1") && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}))
	defer closeSrv()

	tmpFile := filepath.Join(t.TempDir(), "upload.bin")
	if err := os.WriteFile(tmpFile, []byte("abc"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var out strings.Builder
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"search", []string{"--account", "a@b.com", "drive", "search", "hello"}},
		{"upload", []string{"--account", "a@b.com", "drive", "upload", tmpFile, "--name", "upload.bin", "--parent", "np"}},
		{"mkdir", []string{"--account", "a@b.com", "drive", "mkdir", "Folder", "--parent", "np"}},
		{"rename", []string{"--account", "a@b.com", "drive", "rename", "id1", "New"}},
		{"move", []string{"--account", "a@b.com", "drive", "move", "id1", "--parent", "np"}},
		{"share", []string{"--force", "--account", "a@b.com", "drive", "share", "id1", "--anyone", "--role", "reader"}},
		{"permissions", []string{"--account", "a@b.com", "drive", "permissions", "id1"}},
		{"unshare", []string{"--force", "--account", "a@b.com", "drive", "unshare", "id1", "p1"}},
		{"delete", []string{"--force", "--account", "a@b.com", "drive", "delete", "id1"}},
	} {
		result := executeWithDriveTestService(t, tc.args, svc)
		if result.err != nil {
			t.Fatalf("%s: %v", tc.name, result.err)
		}
		out.WriteString(result.stdout)
	}
	if strings.TrimSpace(out.String()) == "" {
		t.Fatalf("expected text output")
	}
}
