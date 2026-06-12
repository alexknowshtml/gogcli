package cmd

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewFormsAddQuestionPlan(t *testing.T) {
	t.Parallel()

	plan, err := newFormsAddQuestionPlan(formsAddQuestionInput{
		FormID:      " https://docs.google.com/forms/d/form1/edit ",
		Title:       " Favorite color ",
		Type:        " RADIO ",
		Required:    true,
		Options:     []string{"Red", "Blue"},
		Index:       0,
		Description: " Choose one ",
	})
	if err != nil {
		t.Fatalf("newFormsAddQuestionPlan: %v", err)
	}
	if plan.FormID != "form1" || plan.Title != "Favorite color" || plan.Type != "radio" {
		t.Fatalf("unexpected plan: %#v", plan)
	}
	if plan.Item.Description != "Choose one" || plan.Item.QuestionItem.Question.ChoiceQuestion.Type != "RADIO" {
		t.Fatalf("unexpected item: %#v", plan.Item)
	}
	request := plan.batchRequest(99)
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if !strings.Contains(string(payload), `"location":{"index":0}`) {
		t.Fatalf("zero insertion index omitted: %s", payload)
	}
}

func TestFormsAddQuestionPlanAppend(t *testing.T) {
	t.Parallel()

	plan, err := newFormsAddQuestionPlan(formsAddQuestionInput{
		FormID: "form1",
		Title:  "Question",
		Type:   "text",
		Index:  -1,
	})
	if err != nil {
		t.Fatalf("newFormsAddQuestionPlan: %v", err)
	}
	if !plan.needsCurrentForm() {
		t.Fatal("needsCurrentForm = false")
	}
	create := plan.batchRequest(3).Requests[0].CreateItem
	if create.Location == nil || create.Location.Index != 3 {
		t.Fatalf("unexpected append location: %#v", create.Location)
	}
}

func TestNewFormsAddQuestionPlanValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		input formsAddQuestionInput
		want  string
	}{
		{
			name:  "empty form",
			input: formsAddQuestionInput{Title: "Question", Type: "text"},
			want:  "empty formId",
		},
		{
			name:  "empty title",
			input: formsAddQuestionInput{FormID: "form1", Type: "text"},
			want:  "empty --title",
		},
		{
			name:  "invalid index",
			input: formsAddQuestionInput{FormID: "form1", Title: "Question", Type: "text", Index: -2},
			want:  "--index must be >= -1",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := newFormsAddQuestionPlan(tc.input)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
