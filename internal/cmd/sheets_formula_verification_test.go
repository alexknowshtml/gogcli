package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/api/sheets/v4"
)

func TestSheetsUpdateFormulaVerificationReportsStructuredErrors(t *testing.T) {
	t.Parallel()

	valuesPath := filepath.Join(t.TempDir(), "values.json")
	if err := os.WriteFile(valuesPath, []byte(`[["=1+1","#REF!","=1/0"]]`), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}

	var updatedValues [][]any
	var verifyQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/spreadsheets/s1/values/"):
			var request sheets.ValueRange
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode update: %v", err)
			}
			updatedValues = request.Values
			_ = json.NewEncoder(w).Encode(map[string]any{
				"updatedRange":   "Sheet1!A1:C1",
				"updatedRows":    1,
				"updatedColumns": 3,
				"updatedCells":   3,
			})
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/spreadsheets/s1"):
			verifyQuery = r.URL.RawQuery
			_ = json.NewEncoder(w).Encode(map[string]any{
				"spreadsheetId": "s1",
				"sheets": []any{map[string]any{
					"properties": map[string]any{"title": "Sheet1"},
					"data": []any{map[string]any{
						"startRow":    0,
						"startColumn": 0,
						"rowData": []any{map[string]any{"values": []any{
							map[string]any{"effectiveValue": map[string]any{"numberValue": 2}},
							map[string]any{"effectiveValue": map[string]any{"stringValue": "#REF!"}},
							map[string]any{"effectiveValue": map[string]any{"errorValue": map[string]any{
								"type": "DIVIDE_BY_ZERO", "message": "Function DIVIDE parameter 2 cannot be zero.",
							}}},
						}}},
					}},
				}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var output bytes.Buffer
	svc := newSheetsServiceFromServer(t, srv)
	ctx := withSheetsTestService(newCmdRuntimeJSONOutputContext(t, &output, io.Discard), svc)
	err := runKong(t, &SheetsUpdateCmd{}, []string{
		"s1", "Sheet1!A1:C1",
		"--values-json", "@" + valuesPath,
		"--fail-on-formula-error",
	}, ctx, &RootFlags{Account: "a@b.com"})
	if err == nil || !strings.Contains(err.Error(), "Sheet1!C1 (DIVIDE_BY_ZERO)") {
		t.Fatalf("verification error = %v", err)
	}
	if len(updatedValues) != 1 || len(updatedValues[0]) != 3 || updatedValues[0][0] != "=1+1" {
		t.Fatalf("updated values = %#v", updatedValues)
	}
	if !strings.Contains(verifyQuery, "includeGridData=true") || !strings.Contains(verifyQuery, "ranges=Sheet1%21A1%3AC1") {
		t.Fatalf("verification query = %q", verifyQuery)
	}

	var result struct {
		FormulaErrors []sheetsFormulaError `json:"formulaErrors"`
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if len(result.FormulaErrors) != 1 || result.FormulaErrors[0].Cell != "Sheet1!C1" || result.FormulaErrors[0].Type != "DIVIDE_BY_ZERO" {
		t.Fatalf("formula errors = %#v", result.FormulaErrors)
	}
}

func TestSheetsUpdateFormulaVerificationIgnoresLiteralErrorText(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPut:
			_ = json.NewEncoder(w).Encode(map[string]any{"updatedRange": "Sheet1!A1", "updatedCells": 1})
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{"sheets": []any{map[string]any{
				"properties": map[string]any{"title": "Sheet1"},
				"data": []any{map[string]any{"rowData": []any{map[string]any{"values": []any{
					map[string]any{"effectiveValue": map[string]any{"stringValue": "#REF!"}},
				}}}}},
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var output bytes.Buffer
	ctx := withSheetsTestService(
		newCmdRuntimeJSONOutputContext(t, &output, io.Discard),
		newSheetsServiceFromServer(t, srv),
	)
	if err := runKong(t, &SheetsUpdateCmd{}, []string{
		"s1", "Sheet1!A1", "--values-json", `[["#REF!"]]`, "--input", "RAW", "--fail-on-formula-error",
	}, ctx, &RootFlags{Account: "a@b.com"}); err != nil {
		t.Fatalf("literal error text: %v", err)
	}
	if !strings.Contains(output.String(), `"formulaErrors": []`) {
		t.Fatalf("output = %s", output.String())
	}
}

func TestSheetsUpdateFormulaVerificationReadbackFailureIsFatal(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"updatedRange": "Sheet1!A1", "updatedCells": 1})
			return
		}
		http.Error(w, "readback failed", http.StatusInternalServerError)
	}))
	defer srv.Close()

	ctx := withSheetsTestService(
		newCmdRuntimeOutputContext(t, io.Discard, io.Discard),
		newSheetsServiceFromServer(t, srv),
	)
	err := runKong(t, &SheetsUpdateCmd{}, []string{
		"s1", "Sheet1!A1", "--values-json", `[["=1+1"]]`, "--fail-on-formula-error",
	}, ctx, &RootFlags{Account: "a@b.com"})
	if err == nil || !strings.Contains(err.Error(), "verify updated formulas") {
		t.Fatalf("readback error = %v", err)
	}
}
