package cmd

import "testing"

func TestMarkdownToDocsRequests_BaseIndex(t *testing.T) {
	elements := []MarkdownElement{{Type: MDParagraph, Content: "**bold**"}}
	requests, text, tables, bullets, hrules := MarkdownToDocsRequests(elements, 42, "")

	if text != "bold\n" {
		t.Fatalf("unexpected text: %q", text)
	}
	if len(tables) != 0 {
		t.Fatalf("unexpected tables: %d", len(tables))
	}
	if len(bullets) != 0 {
		t.Fatalf("unexpected bullets: %d", len(bullets))
	}
	if len(hrules) != 0 {
		t.Fatalf("unexpected hrules: %d", len(hrules))
	}
	if len(requests) != 1 || requests[0].UpdateTextStyle == nil {
		t.Fatalf("expected one text-style request, got %#v", requests)
	}

	rng := requests[0].UpdateTextStyle.Range
	if rng.StartIndex != 42 || rng.EndIndex != 46 {
		t.Fatalf("unexpected range: [%d,%d]", rng.StartIndex, rng.EndIndex)
	}
}

func TestMarkdownToDocsRequests_TableStartIndexUsesBase(t *testing.T) {
	elements := []MarkdownElement{
		{Type: MDParagraph, Content: "A"},
		{Type: MDTable, TableCells: [][]string{{"h1", "h2"}, {"v1", "v2"}}},
	}
	_, text, tables, _, _ := MarkdownToDocsRequests(elements, 10, "")

	if text != "A\n\n" {
		t.Fatalf("unexpected text: %q", text)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if tables[0].StartIndex != 12 {
		t.Fatalf("unexpected table start index: %d", tables[0].StartIndex)
	}
}

func TestMarkdownToDocsRequests_Checkbox(t *testing.T) {
	elements := []MarkdownElement{
		{Type: MDCheckboxUnchecked, Content: "unchecked item"},
		{Type: MDCheckboxChecked, Content: "checked item"},
	}
	_, text, _, bullets, _ := MarkdownToDocsRequests(elements, 1, "")

	if text != "unchecked item\nchecked item\n" {
		t.Fatalf("unexpected text: %q", text)
	}
	if len(bullets) != 2 {
		t.Fatalf("expected 2 bullets, got %d", len(bullets))
	}
	if bullets[0].BulletPreset != "BULLET_CHECKBOX" {
		t.Fatalf("expected BULLET_CHECKBOX, got %s", bullets[0].BulletPreset)
	}
}

func TestMarkdownToDocsRequests_HorizontalRule(t *testing.T) {
	elements := []MarkdownElement{
		{Type: MDHorizontalRule},
	}
	_, text, _, _, hrules := MarkdownToDocsRequests(elements, 1, "")

	if text != " \n" {
		t.Fatalf("unexpected text: %q", text)
	}
	if len(hrules) != 1 {
		t.Fatalf("expected 1 hrule, got %d", len(hrules))
	}
}
