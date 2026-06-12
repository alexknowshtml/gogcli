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

func TestSheetsGet_ValidationAndNoData(t *testing.T) {
	ctx := newCmdRuntimeOutputContext(t, io.Discard, io.Discard)
	flags := &RootFlags{Account: "a@b.com"}

	if err := (&SheetsGetCmd{}).Run(ctx, flags); err == nil {
		t.Fatalf("expected missing spreadsheetId error")
	}
	if err := (&SheetsGetCmd{SpreadsheetID: "s1"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected missing range error")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/sheets/v4")
		path = strings.TrimPrefix(path, "/v4")
		if strings.Contains(path, "/spreadsheets/s1/values/") && r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"range":  "Sheet1!A1:B2",
				"values": []any{},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	svc := newSheetsServiceFromServer(t, srv)
	ctx = withSheetsTestService(ctx, svc)

	cmd := &SheetsGetCmd{SpreadsheetID: "s1", Range: "Sheet1!A1:B2", MajorDimension: "ROWS", ValueRenderOption: "FORMATTED_VALUE"}
	if err := cmd.Run(ctx, flags); err != nil {
		t.Fatalf("get: %v", err)
	}
}

func TestSheetsGet_JSONEmptyValuesArray(t *testing.T) {
	output := &bytes.Buffer{}
	ctx := newCmdRuntimeJSONOutputContext(t, output, io.Discard)
	flags := &RootFlags{Account: "a@b.com", JSON: true}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/sheets/v4")
		path = strings.TrimPrefix(path, "/v4")
		if strings.Contains(path, "/spreadsheets/s1/values/") && r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"range": "Sheet1!Z999"})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	svc := newSheetsServiceFromServer(t, srv)
	ctx = withSheetsTestService(ctx, svc)

	cmd := &SheetsGetCmd{SpreadsheetID: "s1", Range: "Sheet1!Z999"}
	if err := cmd.Run(ctx, flags); err != nil {
		t.Fatalf("get: %v", err)
	}
	out := output.String()

	var parsed struct {
		Range  string            `json:"range"`
		Values []json.RawMessage `json:"values"`
	}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Range != "Sheet1!Z999" {
		t.Fatalf("range = %q", parsed.Range)
	}
	if parsed.Values == nil {
		t.Fatalf("values must be an empty array, got nil: %s", out)
	}
	if len(parsed.Values) != 0 {
		t.Fatalf("values len = %d, want 0", len(parsed.Values))
	}
}

func TestSheetsUpdateAppend_ValidationErrors(t *testing.T) {
	ctx := newCmdRuntimeOutputContext(t, io.Discard, io.Discard)
	flags := &RootFlags{Account: "a@b.com"}
	requireUsage := func(t *testing.T, name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("expected %s error", name)
		}
		if got := ExitCode(err); got != 2 {
			t.Fatalf("%s: expected usage exit code 2, got %d (err=%v)", name, got, err)
		}
	}

	requireUsage(t, "update missing spreadsheetId", (&SheetsUpdateCmd{}).Run(ctx, flags))
	requireUsage(t, "update missing range", (&SheetsUpdateCmd{SpreadsheetID: "s1"}).Run(ctx, flags))
	requireUsage(t, "update invalid json", (&SheetsUpdateCmd{SpreadsheetID: "s1", Range: "A1", ValuesJSON: "nope"}).Run(ctx, flags))
	requireUsage(t, "update missing values", (&SheetsUpdateCmd{SpreadsheetID: "s1", Range: "A1"}).Run(ctx, flags))

	requireUsage(t, "append missing spreadsheetId", (&SheetsAppendCmd{}).Run(ctx, flags))
	requireUsage(t, "append missing range", (&SheetsAppendCmd{SpreadsheetID: "s1"}).Run(ctx, flags))
	requireUsage(t, "append invalid json", (&SheetsAppendCmd{SpreadsheetID: "s1", Range: "A1", ValuesJSON: "nope"}).Run(ctx, flags))
	requireUsage(t, "append missing values", (&SheetsAppendCmd{SpreadsheetID: "s1", Range: "A1"}).Run(ctx, flags))
}

func TestSheetsUpdateCopyValidationMissingRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/sheets/v4")
		path = strings.TrimPrefix(path, "/v4")
		if strings.Contains(path, "/spreadsheets/s1/values/") && r.Method == http.MethodPut {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"updatedRange": "",
				"updatedCells": 1,
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	svc := newSheetsServiceFromServer(t, srv)
	ctx := withSheetsTestService(newCmdRuntimeOutputContext(t, io.Discard, io.Discard), svc)
	flags := &RootFlags{Account: "a@b.com"}

	cmd := &SheetsUpdateCmd{ValueInput: ""}
	if err := runKong(t, cmd, []string{"s1", "Sheet1!A1", "--values-json", `[["a"]]`, "--copy-validation-from", "Sheet1!A2:A2"}, ctx, flags); err == nil {
		t.Fatalf("expected missing updated range error")
	}
}

func TestSheetsAppendCopyValidationMissingRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/sheets/v4")
		path = strings.TrimPrefix(path, "/v4")
		if strings.Contains(path, "/spreadsheets/s1/values/") && r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	svc := newSheetsServiceFromServer(t, srv)
	ctx := withSheetsTestService(newCmdRuntimeOutputContext(t, io.Discard, io.Discard), svc)
	flags := &RootFlags{Account: "a@b.com"}

	cmd := &SheetsAppendCmd{Insert: "INSERT_ROWS", ValueInput: ""}
	if err := runKong(t, cmd, []string{"s1", "Sheet1!A1", "--values-json", `[["a"]]`, "--copy-validation-from", "Sheet1!A2:A2"}, ctx, flags); err == nil {
		t.Fatalf("expected missing updated range error")
	}
}

func TestSheetsClearMetadataCreate_ValidationErrors(t *testing.T) {
	ctx := newCmdRuntimeOutputContext(t, io.Discard, io.Discard)
	flags := &RootFlags{Account: "a@b.com"}

	if err := (&SheetsClearCmd{}).Run(ctx, flags); err == nil {
		t.Fatalf("expected clear missing spreadsheetId error")
	}
	if err := (&SheetsClearCmd{SpreadsheetID: "s1"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected clear missing range error")
	}
	if err := (&SheetsMetadataCmd{}).Run(ctx, flags); err == nil {
		t.Fatalf("expected metadata missing spreadsheetId error")
	}
	if err := (&SheetsLinksGetCmd{}).Run(ctx, flags); err == nil {
		t.Fatalf("expected links missing spreadsheetId error")
	}
	if err := (&SheetsLinksGetCmd{SpreadsheetID: "s1"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected links missing range error")
	}
	if err := (&SheetsCreateCmd{}).Run(ctx, flags); err == nil {
		t.Fatalf("expected create missing title error")
	}
}

func TestSheetsFormat_ValidationErrors(t *testing.T) {
	ctx := newCmdRuntimeOutputContext(t, io.Discard, io.Discard)
	flags := &RootFlags{Account: "a@b.com"}

	if err := (&SheetsFormatCmd{}).Run(ctx, flags); err == nil {
		t.Fatalf("expected format missing spreadsheetId error")
	}
	if err := (&SheetsFormatCmd{SpreadsheetID: "s1"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected format missing range error")
	}
	if err := (&SheetsFormatCmd{SpreadsheetID: "s1", Range: "Sheet1!A1"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected format missing format-json error")
	}
	if err := (&SheetsFormatCmd{SpreadsheetID: "s1", Range: "Sheet1!A1", FormatJSON: "nope", FormatFields: "userEnteredFormat.textFormat.bold"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected format invalid json error")
	} else if got := ExitCode(err); got != 2 {
		t.Fatalf("expected usage exit code 2, got %d (err=%v)", got, err)
	}
	if err := (&SheetsFormatCmd{SpreadsheetID: "s1", Range: "Sheet1!A1", FormatJSON: "{\"boarders\":{\"top\":{\"style\":\"SOLID\"}}}", FormatFields: "borders.top.style"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected format unknown field error for boarders json typo")
	} else if got := ExitCode(err); got != 2 {
		t.Fatalf("expected usage exit code 2, got %d (err=%v)", got, err)
	}
	if err := (&SheetsFormatCmd{SpreadsheetID: "s1", Range: "Sheet1!A1", FormatJSON: "{\"borders\":{\"top\":{\"style\":\"SOLID\"}}}", FormatFields: "boarders.top.style"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected format typo error for boarders field mask")
	} else if got := ExitCode(err); got != 2 {
		t.Fatalf("expected usage exit code 2, got %d (err=%v)", got, err)
	}
	if err := (&SheetsFormatCmd{SpreadsheetID: "s1", Range: "Sheet1!A1", FormatJSON: "{\"textFormat\":{\"bold\":true}}", FormatFields: "userEnteredFormat.nope"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected format invalid field error")
	} else if got := ExitCode(err); got != 2 {
		t.Fatalf("expected usage exit code 2, got %d (err=%v)", got, err)
	}
	if err := (&SheetsFormatCmd{SpreadsheetID: "s1", Range: "A1:B2", FormatJSON: "{\"textFormat\":{\"bold\":true}}", FormatFields: "userEnteredFormat.textFormat.bold"}).Run(ctx, flags); err == nil {
		t.Fatalf("expected format missing sheet name error")
	}
}

func TestSheetsQualifiedRangeValidationErrors(t *testing.T) {
	ctx := newCmdRuntimeOutputContext(t, io.Discard, io.Discard)
	flags := &RootFlags{Account: "a@b.com", DryRun: true}
	note := "test"

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{
			name: "update note",
			run: func() error {
				return (&SheetsUpdateNoteCmd{SpreadsheetID: "s1", Range: "A1", Note: &note}).Run(ctx, flags)
			},
		},
		{
			name: "merge",
			run: func() error {
				return (&SheetsMergeCmd{SpreadsheetID: "s1", Range: "A1:B2"}).Run(ctx, flags)
			},
		},
		{
			name: "number format",
			run: func() error {
				return (&SheetsNumberFormatCmd{SpreadsheetID: "s1", Range: "A1", Type: "NUMBER"}).Run(ctx, flags)
			},
		},
		{
			name: "copy paste",
			run: func() error {
				return (&SheetsCopyPasteCmd{SpreadsheetID: "s1", Source: "A1", Dest: "Sheet1!B2"}).Run(ctx, flags)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if err == nil {
				t.Fatal("expected missing sheet name error")
			}
			if got := ExitCode(err); got != 2 {
				t.Fatalf("ExitCode = %d, want 2 (err=%v)", got, err)
			}
		})
	}
}

func TestParseSheetRangeAndGridRange(t *testing.T) {
	if _, err := parseSheetRange("A1:B2", "format"); err == nil {
		t.Fatalf("expected missing sheet name error")
	} else if got := ExitCode(err); got != 2 {
		t.Fatalf("ExitCode = %d, want 2 (err=%v)", got, err)
	}

	r, err := parseSheetRange("Sheet1!B2:C3", "format")
	if err != nil {
		t.Fatalf("parseSheetRange: %v", err)
	}

	grid, err := gridRangeFromMap(r, map[string]int64{"Sheet1": 9}, "format")
	if err != nil {
		t.Fatalf("gridRangeFromMap: %v", err)
	}
	if grid.SheetId != 9 {
		t.Fatalf("unexpected sheet id: %d", grid.SheetId)
	}
	if !hasStringValue(grid.ForceSendFields, "SheetId") {
		t.Fatalf("expected SheetId in ForceSendFields, got %#v", grid.ForceSendFields)
	}

	zero := toGridRange(a1Range{
		SheetName: "Sheet1",
		StartRow:  1,
		EndRow:    2,
		StartCol:  1,
		EndCol:    2,
	}, 0)
	if zero.SheetId != 0 {
		t.Fatalf("expected sheet id 0, got %d", zero.SheetId)
	}
	if !hasStringValue(zero.ForceSendFields, "SheetId") {
		t.Fatalf("expected SheetId force-send for sheet id 0, got %#v", zero.ForceSendFields)
	}
}
