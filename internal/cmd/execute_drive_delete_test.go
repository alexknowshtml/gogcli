package cmd

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestExecute_DriveDelete_DefaultAndPermanent(t *testing.T) {
	t.Run("default_trash", func(t *testing.T) {
		var patchCount int
		svc, closeSrv := newDriveTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.Contains(r.URL.Path, "/files/id1") || (r.Method != http.MethodPatch && r.Method != http.MethodPut) {
				http.NotFound(w, r)
				return
			}
			patchCount++
			requireSupportsAllDrives(t, r)
			body := readBody(t, r)
			if !strings.Contains(body, "\"trashed\":true") {
				t.Fatalf("expected trashed=true body, got: %q", body)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      "id1",
				"trashed": true,
				"kind":    "drive#file",
			})
		}))
		defer closeSrv()

		result := executeWithDriveTestService(t, []string{"--force", "--account", "a@b.com", "drive", "delete", "id1"}, svc)
		if result.err != nil {
			t.Fatalf("Execute: %v", result.err)
		}
		if !strings.Contains(result.stdout, "trashed\ttrue") || !strings.Contains(result.stdout, "deleted\tfalse") {
			t.Fatalf("unexpected text output: %q", result.stdout)
		}

		jsonResult := executeWithDriveTestService(t, []string{"--json", "--force", "--account", "a@b.com", "drive", "delete", "id1"}, svc)
		if jsonResult.err != nil {
			t.Fatalf("Execute: %v", jsonResult.err)
		}
		var parsed struct {
			Trashed bool   `json:"trashed"`
			Deleted bool   `json:"deleted"`
			ID      string `json:"id"`
		}
		if err := json.Unmarshal([]byte(jsonResult.stdout), &parsed); err != nil {
			t.Fatalf("json parse: %v\nout=%q", err, jsonResult.stdout)
		}
		if !parsed.Trashed || parsed.Deleted || parsed.ID != "id1" {
			t.Fatalf("unexpected json output: %#v", parsed)
		}

		if patchCount != 2 {
			t.Fatalf("expected 2 PATCH calls, got %d", patchCount)
		}
	})

	t.Run("permanent_delete", func(t *testing.T) {
		var deleteCount int
		svc, closeSrv := newDriveTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.Contains(r.URL.Path, "/files/id1") || r.Method != http.MethodDelete {
				http.NotFound(w, r)
				return
			}
			deleteCount++
			requireSupportsAllDrives(t, r)
			w.WriteHeader(http.StatusNoContent)
		}))
		defer closeSrv()

		result := executeWithDriveTestService(t, []string{"--force", "--account", "a@b.com", "drive", "delete", "id1", "--permanent"}, svc)
		if result.err != nil {
			t.Fatalf("Execute: %v", result.err)
		}
		if !strings.Contains(result.stdout, "trashed\tfalse") || !strings.Contains(result.stdout, "deleted\ttrue") {
			t.Fatalf("unexpected text output: %q", result.stdout)
		}

		jsonResult := executeWithDriveTestService(t, []string{"--json", "--force", "--account", "a@b.com", "drive", "delete", "id1", "--permanent"}, svc)
		if jsonResult.err != nil {
			t.Fatalf("Execute: %v", jsonResult.err)
		}
		var parsed struct {
			Trashed bool   `json:"trashed"`
			Deleted bool   `json:"deleted"`
			ID      string `json:"id"`
		}
		if err := json.Unmarshal([]byte(jsonResult.stdout), &parsed); err != nil {
			t.Fatalf("json parse: %v\nout=%q", err, jsonResult.stdout)
		}
		if parsed.Trashed || !parsed.Deleted || parsed.ID != "id1" {
			t.Fatalf("unexpected json output: %#v", parsed)
		}

		if deleteCount != 2 {
			t.Fatalf("expected 2 DELETE calls, got %d", deleteCount)
		}
	})
}
