package cmd

import (
	"strings"

	formsapi "google.golang.org/api/forms/v1"
)

type formsAddQuestionInput struct {
	FormID         string
	Title          string
	Type           string
	Required       bool
	Options        []string
	Index          int
	Correct        []string
	Points         int
	ScaleLow       int
	ScaleHigh      int
	ScaleLowLabel  string
	ScaleHighLabel string
	IncludeTime    bool
	IncludeYear    bool
	Duration       bool
	Description    string
}

type formsAddQuestionPlan struct {
	FormID      string
	Title       string
	Type        string
	Required    bool
	Options     []string
	Index       int
	Correct     []string
	Points      int
	Description string
	Item        *formsapi.Item
}

func newFormsAddQuestionPlan(input formsAddQuestionInput) (formsAddQuestionPlan, error) {
	plan := formsAddQuestionPlan{
		FormID:      strings.TrimSpace(normalizeGoogleID(input.FormID)),
		Title:       strings.TrimSpace(input.Title),
		Type:        strings.ToLower(strings.TrimSpace(input.Type)),
		Required:    input.Required,
		Options:     input.Options,
		Index:       input.Index,
		Correct:     input.Correct,
		Points:      input.Points,
		Description: input.Description,
	}
	if plan.FormID == "" {
		return formsAddQuestionPlan{}, usage("empty formId")
	}
	if plan.Title == "" {
		return formsAddQuestionPlan{}, usage("empty --title")
	}
	if plan.Index < -1 {
		return formsAddQuestionPlan{}, usage("--index must be >= -1")
	}

	question, err := buildQuestion(plan.Type, &input)
	if err != nil {
		return formsAddQuestionPlan{}, err
	}
	plan.Item = &formsapi.Item{
		Title:       plan.Title,
		Description: strings.TrimSpace(input.Description),
		QuestionItem: &formsapi.QuestionItem{
			Question: question,
		},
	}
	return plan, nil
}

func (p formsAddQuestionPlan) needsCurrentForm() bool {
	return p.Index < 0
}

func (p formsAddQuestionPlan) batchRequest(currentItemCount int) *formsapi.BatchUpdateFormRequest {
	insertAt := p.Index
	if p.needsCurrentForm() {
		insertAt = currentItemCount
	}
	return &formsapi.BatchUpdateFormRequest{
		Requests: []*formsapi.Request{
			{
				CreateItem: &formsapi.CreateItemRequest{
					Item:     p.Item,
					Location: formLocationIndex(insertAt),
				},
			},
		},
		IncludeFormInResponse: true,
	}
}

func (p formsAddQuestionPlan) dryRunPayload() map[string]any {
	return map[string]any{
		"form_id":     p.FormID,
		"title":       p.Title,
		"type":        p.Type,
		"required":    p.Required,
		"options":     p.Options,
		"index":       p.Index,
		"correct":     p.Correct,
		"points":      p.Points,
		"description": p.Description,
	}
}
