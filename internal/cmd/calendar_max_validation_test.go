package cmd

import (
	"context"
	"io"
	"strings"
	"testing"

	"google.golang.org/api/calendar/v3"
	"google.golang.org/api/people/v1"
)

func TestCalendarMaxValidationFailsBeforeService(t *testing.T) {
	origCal := newCalendarService
	origPeople := newPeopleDirectoryService
	t.Cleanup(func() {
		newCalendarService = origCal
		newPeopleDirectoryService = origPeople
	})
	newCalendarService = func(context.Context, string) (*calendar.Service, error) {
		t.Fatalf("expected max validation to fail before creating calendar service")
		return nil, context.Canceled
	}
	newPeopleDirectoryService = func(context.Context, string) (*people.Service, error) {
		t.Fatalf("expected max validation to fail before creating people service")
		return nil, context.Canceled
	}

	ctx := newCmdOutputContext(t, io.Discard, io.Discard)
	flags := &RootFlags{Account: "a@example.com"}
	cases := []struct {
		name string
		run  func() error
	}{
		{name: "calendars zero", run: func() error { return (&CalendarCalendarsCmd{Max: 0}).Run(ctx, flags) }},
		{name: "calendars negative", run: func() error { return (&CalendarCalendarsCmd{Max: -1}).Run(ctx, flags) }},
		{name: "acl zero", run: func() error { return (&CalendarAclCmd{CalendarID: "primary", Max: 0}).Run(ctx, flags) }},
		{name: "acl negative", run: func() error { return (&CalendarAclCmd{CalendarID: "primary", Max: -1}).Run(ctx, flags) }},
		{name: "events zero", run: func() error { return (&CalendarEventsCmd{Max: 0}).Run(ctx, flags) }},
		{name: "events negative", run: func() error { return (&CalendarEventsCmd{Max: -1}).Run(ctx, flags) }},
		{name: "search zero", run: func() error { return (&CalendarSearchCmd{Query: "meeting", Max: 0}).Run(ctx, flags) }},
		{name: "search negative", run: func() error { return (&CalendarSearchCmd{Query: "meeting", Max: -1}).Run(ctx, flags) }},
		{name: "team zero", run: func() error { return (&CalendarTeamCmd{GroupEmail: "group@example.com", Max: 0}).Run(ctx, flags) }},
		{name: "team negative", run: func() error { return (&CalendarTeamCmd{GroupEmail: "group@example.com", Max: -1}).Run(ctx, flags) }},
		{name: "users zero", run: func() error { return (&CalendarUsersCmd{Max: 0}).Run(ctx, flags) }},
		{name: "users negative", run: func() error { return (&CalendarUsersCmd{Max: -1}).Run(ctx, flags) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if ExitCode(err) != 2 || !strings.Contains(err.Error(), "max must be > 0") {
				t.Fatalf("unexpected err: %v", err)
			}
		})
	}
}
