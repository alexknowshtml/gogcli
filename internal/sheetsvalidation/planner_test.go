package sheetsvalidation

import (
	"strings"
	"testing"

	"google.golang.org/api/sheets/v4"
)

func TestBuildCondition(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		values    []string
		wantType  string
		wantValue string
		wantErr   string
	}{
		{name: "list", kind: "one-of-list", values: []string{"red", "green"}, wantType: "ONE_OF_LIST"},
		{name: "range", kind: "ONE_OF_RANGE", values: []string{"Sheet1!A1:A3"}, wantType: "ONE_OF_RANGE", wantValue: "=Sheet1!A1:A3"},
		{name: "literal whitespace", kind: "TEXT_EQ", values: []string{" pending "}, wantType: "TEXT_EQ", wantValue: " pending "},
		{name: "list formula", kind: "ONE_OF_LIST", values: []string{"=A1"}, wantErr: "cannot be formulas"},
		{name: "custom formula syntax", kind: "CUSTOM_FORMULA", values: []string{"A1>0"}, wantErr: "must begin with"},
		{name: "unsupported", kind: "BLANK", wantErr: "unsupported validation"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			condition, err := BuildCondition(test.kind, test.values)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("BuildCondition() error = %v, want %q", err, test.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("BuildCondition() error = %v", err)
			}

			if condition.Type != test.wantType {
				t.Fatalf("condition type = %q, want %q", condition.Type, test.wantType)
			}

			if test.wantValue != "" && condition.Values[0].UserEnteredValue != test.wantValue {
				t.Fatalf("condition value = %q, want %q", condition.Values[0].UserEnteredValue, test.wantValue)
			}
		})
	}
}

func TestBuildSetAndClearRequests(t *testing.T) {
	columns := []*sheets.TableColumnProperties{
		{ColumnIndex: 0, ColumnName: "Name", ColumnType: columnTypeText},
		{
			ColumnIndex: 1,
			ColumnName:  "State",
			ColumnType:  columnTypeDropdown,
			DataValidationRule: &sheets.TableColumnDataValidationRule{
				Condition: &sheets.BooleanCondition{
					Type: conditionOneOfList,
					Values: []*sheets.ConditionValue{
						{UserEnteredValue: "old"},
					},
				},
			},
		},
	}
	spans := []Span{{
		SheetID:     7,
		TableID:     "table-1",
		ColumnIndex: 1,
		StartRow:    1,
		EndRow:      5,
		StartCol:    1,
		EndCol:      2,
		Rule: &sheets.DataValidationRule{
			Condition:    CloneCondition(columns[1].DataValidationRule.Condition),
			ShowCustomUi: true,
		},
		Columns: columns,
	}}

	target := &sheets.GridRange{
		SheetId:          7,
		StartRowIndex:    1,
		EndRowIndex:      5,
		StartColumnIndex: 1,
		EndColumnIndex:   2,
	}

	condition, err := BuildCondition(conditionOneOfList, []string{"new", "done"})
	if err != nil {
		t.Fatalf("BuildCondition() error = %v", err)
	}

	setRequests, err := BuildSetRequests(target, spans, condition)
	if err != nil {
		t.Fatalf("BuildSetRequests() error = %v", err)
	}

	if len(setRequests) != 1 {
		t.Fatalf("set request count = %d, want 1", len(setRequests))
	}

	setColumn := setRequests[0].UpdateTable.Table.ColumnProperties[1]
	if setColumn.ColumnType != columnTypeDropdown ||
		setColumn.DataValidationRule.Condition.Values[0].UserEnteredValue != "new" {
		t.Fatalf("set column = %#v", setColumn)
	}

	clearRequests, err := BuildClearRequests(target, spans)
	if err != nil {
		t.Fatalf("BuildClearRequests() error = %v", err)
	}

	clearColumn := clearRequests[0].UpdateTable.Table.ColumnProperties[1]
	if clearColumn.ColumnType != columnTypeText || clearColumn.DataValidationRule != nil {
		t.Fatalf("clear column = %#v", clearColumn)
	}
}

func TestBuildSetRequestsRejectsPartialTableColumn(t *testing.T) {
	_, err := BuildSetRequests(
		&sheets.GridRange{
			SheetId:          1,
			StartRowIndex:    2,
			EndRowIndex:      4,
			StartColumnIndex: 0,
			EndColumnIndex:   1,
		},
		[]Span{{
			SheetID:     1,
			TableID:     "table-1",
			ColumnIndex: 0,
			StartRow:    1,
			EndRow:      5,
			StartCol:    0,
			EndCol:      1,
		}},
		&sheets.BooleanCondition{Type: conditionOneOfList},
	)
	if err == nil || !strings.Contains(err.Error(), "full table data column") {
		t.Fatalf("BuildSetRequests() error = %v", err)
	}
}

func TestSubtractSpans(t *testing.T) {
	ranges := SubtractSpans(
		&sheets.GridRange{
			SheetId:          1,
			StartRowIndex:    0,
			EndRowIndex:      6,
			StartColumnIndex: 0,
			EndColumnIndex:   3,
		},
		[]Span{{
			SheetID:  1,
			StartRow: 1,
			EndRow:   5,
			StartCol: 1,
			EndCol:   2,
		}},
	)
	if len(ranges) != 4 {
		t.Fatalf("range count = %d, want 4: %#v", len(ranges), ranges)
	}

	for _, gridRange := range ranges {
		if gridRange.SheetId != 1 {
			t.Fatalf("range sheet ID = %d, want 1", gridRange.SheetId)
		}
	}
}
