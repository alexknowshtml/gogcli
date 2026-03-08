package cmd

import (
	"context"
	"strings"

	"google.golang.org/api/calendar/v3"

	"github.com/steipete/gogcli/internal/config"
)

func isAllDayEvent(e *calendar.Event) bool {
	return e != nil && e.Start != nil && e.Start.Date != ""
}

// prepareCalendarID resolves aliases before any API-backed calendar lookup.
// When defaultPrimary is true, empty input becomes the primary calendar.
func prepareCalendarID(calendarID string, defaultPrimary bool) (string, error) {
	calendarID = strings.TrimSpace(calendarID)
	if calendarID == "" {
		if defaultPrimary {
			return primaryCalendarID, nil
		}
		return "", usage("empty calendarId")
	}

	resolved, err := config.ResolveCalendarID(calendarID)
	if err != nil {
		return "", err
	}

	return resolved, nil
}

func resolveCalendarSelector(ctx context.Context, svc *calendar.Service, calendarID string, defaultPrimary bool) (string, error) {
	prepared, err := prepareCalendarID(calendarID, defaultPrimary)
	if err != nil {
		return "", err
	}
	return resolveCalendarID(ctx, svc, prepared)
}

func prepareCalendarIDs(inputs []string) ([]string, error) {
	prepared := make([]string, 0, len(inputs))
	for _, input := range inputs {
		resolved, err := prepareCalendarID(input, false)
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, resolved)
	}
	return prepared, nil
}
