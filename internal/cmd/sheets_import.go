package cmd

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steipete/gogcli/internal/outfmt"
	"github.com/steipete/gogcli/internal/ui"
	"google.golang.org/api/sheets/v4"
)

func newSheetsImportCmd(flags *rootFlags) *cobra.Command {
	var sheetName string
	var startCell string
	var createTitle string
	var delimiter string
	var hasHeader bool

	cmd := &cobra.Command{
		Use:   "import <spreadsheetId|-> <csvFile>",
		Short: "Import CSV file into a Google Sheet",
		Long: `Import a CSV file into a Google Sheets spreadsheet.

The CSV file is parsed with proper handling of quoted fields, embedded commas,
and newlines within cells.

Use '-' as spreadsheetId with --create to create a new spreadsheet.

Examples:
  gog sheets import 1BxiMVs... data.csv
  gog sheets import 1BxiMVs... data.csv --sheet "Data" --start "A1"
  gog sheets import - data.csv --create "My New Spreadsheet"
  gog sheets import 1BxiMVs... data.tsv --delimiter "\t"`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}

			spreadsheetID := args[0]
			csvPath := args[1]

			// Validate args
			if spreadsheetID == "-" && createTitle == "" {
				return fmt.Errorf("when using '-' as spreadsheetId, --create is required")
			}

			// Open and parse CSV file
			file, err := os.Open(csvPath)
			if err != nil {
				return fmt.Errorf("failed to open file: %w", err)
			}
			defer file.Close()

			reader := csv.NewReader(file)

			// Handle custom delimiter
			if delimiter != "" {
				if delimiter == "\\t" || delimiter == "tab" {
					reader.Comma = '\t'
				} else if len(delimiter) == 1 {
					reader.Comma = rune(delimiter[0])
				} else {
					return fmt.Errorf("delimiter must be a single character (or '\\t' for tab)")
				}
			}

			// Read all records
			records, err := reader.ReadAll()
			if err != nil {
				return fmt.Errorf("failed to parse CSV: %w", err)
			}

			if len(records) == 0 {
				return fmt.Errorf("CSV file is empty")
			}

			// Convert to sheets values format
			values := make([][]interface{}, len(records))
			for i, row := range records {
				values[i] = make([]interface{}, len(row))
				for j, cell := range row {
					values[i][j] = cell
				}
			}

			// Get sheets service
			svc, err := newSheetsService(cmd.Context(), account)
			if err != nil {
				return err
			}

			// Create new spreadsheet if requested
			if createTitle != "" {
				spreadsheet := &sheets.Spreadsheet{
					Properties: &sheets.SpreadsheetProperties{
						Title: createTitle,
					},
				}

				if sheetName != "" {
					spreadsheet.Sheets = []*sheets.Sheet{
						{
							Properties: &sheets.SheetProperties{
								Title: sheetName,
							},
						},
					}
				}

				resp, err := svc.Spreadsheets.Create(spreadsheet).Do()
				if err != nil {
					return fmt.Errorf("failed to create spreadsheet: %w", err)
				}
				spreadsheetID = resp.SpreadsheetId

				u.Out().Printf("Created spreadsheet: %s", resp.Properties.Title)
				u.Out().Printf("ID: %s", resp.SpreadsheetId)
				u.Out().Printf("URL: %s", resp.SpreadsheetUrl)

				// If we created the spreadsheet and specified a sheet name,
				// use that name, otherwise use the default first sheet
				if sheetName == "" && len(resp.Sheets) > 0 {
					sheetName = resp.Sheets[0].Properties.Title
				}
			}

			// Build range specification
			if sheetName == "" {
				sheetName = "Sheet1"
			}
			if startCell == "" {
				startCell = "A1"
			}

			rangeSpec := fmt.Sprintf("%s!%s", sheetName, startCell)

			// Update the spreadsheet with CSV data
			vr := &sheets.ValueRange{
				Values: values,
			}

			resp, err := svc.Spreadsheets.Values.Update(spreadsheetID, rangeSpec, vr).
				ValueInputOption("USER_ENTERED").
				Do()
			if err != nil {
				return fmt.Errorf("failed to update spreadsheet: %w", err)
			}

			if outfmt.IsJSON(cmd.Context()) {
				result := map[string]any{
					"spreadsheetId":  spreadsheetID,
					"updatedRange":   resp.UpdatedRange,
					"updatedRows":    resp.UpdatedRows,
					"updatedColumns": resp.UpdatedColumns,
					"updatedCells":   resp.UpdatedCells,
				}
				if createTitle != "" {
					result["spreadsheetUrl"] = fmt.Sprintf("https://docs.google.com/spreadsheets/d/%s/edit", spreadsheetID)
				}
				return outfmt.WriteJSON(os.Stdout, result)
			}

			// Infer file type for message
			ext := strings.ToLower(filepath.Ext(csvPath))
			fileType := "CSV"
			if ext == ".tsv" || delimiter == "\\t" || delimiter == "tab" {
				fileType = "TSV"
			}

			u.Out().Printf("Imported %s file: %d rows, %d columns (%d cells)",
				fileType, resp.UpdatedRows, resp.UpdatedColumns, resp.UpdatedCells)
			u.Out().Printf("Range: %s", resp.UpdatedRange)

			if createTitle == "" {
				u.Out().Printf("URL: https://docs.google.com/spreadsheets/d/%s/edit", spreadsheetID)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&sheetName, "sheet", "", "Target sheet name (default: Sheet1)")
	cmd.Flags().StringVar(&startCell, "start", "A1", "Starting cell for import")
	cmd.Flags().StringVar(&createTitle, "create", "", "Create a new spreadsheet with this title")
	cmd.Flags().StringVar(&delimiter, "delimiter", "", "Field delimiter (default: comma, use '\\t' for tab)")
	cmd.Flags().BoolVar(&hasHeader, "header", true, "First row is header (currently informational only)")

	return cmd
}
