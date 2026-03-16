package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steipete/gogcli/internal/googleapi"
	"github.com/steipete/gogcli/internal/outfmt"
	"github.com/steipete/gogcli/internal/ui"
	"google.golang.org/api/calendar/v3"
)

var newCalendarService = googleapi.NewCalendar

func newCalendarCmd(flags *rootFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "calendar",
		Short: "Google Calendar",
	}
	cmd.AddCommand(newCalendarCalendarsCmd(flags))
	cmd.AddCommand(newCalendarCreateCalendarCmd(flags))
	cmd.AddCommand(newCalendarAclCmd(flags))
	cmd.AddCommand(newCalendarEventsCmd(flags))
	cmd.AddCommand(newCalendarEventCmd(flags))
	cmd.AddCommand(newCalendarCreateCmd(flags))
	cmd.AddCommand(newCalendarUpdateCmd(flags))
	cmd.AddCommand(newCalendarDeleteCmd(flags))
	cmd.AddCommand(newCalendarFreeBusyCmd(flags))
	cmd.AddCommand(newCalendarRespondCmd(flags))
	cmd.AddCommand(newCalendarColorsCmd(flags))
	cmd.AddCommand(newCalendarConflictsCmd(flags))
	cmd.AddCommand(newCalendarSearchCmd(flags))
	cmd.AddCommand(newCalendarTimeCmd(flags))
	return cmd
}

func newCalendarCalendarsCmd(flags *rootFlags) *cobra.Command {
	var max int64
	var page string

	cmd := &cobra.Command{
		Use:   "calendars",
		Short: "List calendars",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}

			svc, err := newCalendarService(cmd.Context(), account)
			if err != nil {
				return err
			}

			resp, err := svc.CalendarList.List().MaxResults(max).PageToken(page).Do()
			if err != nil {
				return err
			}
			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{
					"calendars":     resp.Items,
					"nextPageToken": resp.NextPageToken,
				})
			}
			if len(resp.Items) == 0 {
				u.Err().Println("No calendars")
				return nil
			}

			w, flush := tableWriter(cmd.Context())
			defer flush()
			fmt.Fprintln(w, "ID\tNAME\tROLE")
			for _, c := range resp.Items {
				fmt.Fprintf(w, "%s\t%s\t%s\n", c.Id, c.Summary, c.AccessRole)
			}
			printNextPageHint(u, resp.NextPageToken)
			return nil
		},
	}

	cmd.Flags().Int64Var(&max, "max", 100, "Max results")
	cmd.Flags().StringVar(&page, "page", "", "Page token")
	return cmd
}

func newCalendarAclCmd(flags *rootFlags) *cobra.Command {
	var max int64
	var page string

	cmd := &cobra.Command{
		Use:   "acl <calendarId>",
		Short: "List access control rules for a calendar",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}
			calendarID := args[0]

			svc, err := newCalendarService(cmd.Context(), account)
			if err != nil {
				return err
			}

			resp, err := svc.Acl.List(calendarID).MaxResults(max).PageToken(page).Do()
			if err != nil {
				return err
			}
			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{
					"rules":         resp.Items,
					"nextPageToken": resp.NextPageToken,
				})
			}
			if len(resp.Items) == 0 {
				u.Err().Println("No ACL rules")
				return nil
			}

			w, flush := tableWriter(cmd.Context())
			defer flush()
			fmt.Fprintln(w, "SCOPE_TYPE\tSCOPE_VALUE\tROLE")
			for _, rule := range resp.Items {
				scopeType := ""
				scopeValue := ""
				if rule.Scope != nil {
					scopeType = rule.Scope.Type
					scopeValue = rule.Scope.Value
				}
				fmt.Fprintf(w, "%s\t%s\t%s\n", scopeType, scopeValue, rule.Role)
			}
			printNextPageHint(u, resp.NextPageToken)
			return nil
		},
	}

	cmd.Flags().Int64Var(&max, "max", 100, "Max results")
	cmd.Flags().StringVar(&page, "page", "", "Page token")
	return cmd
}

func newCalendarEventsCmd(flags *rootFlags) *cobra.Command {
	var from string
	var to string
	var max int64
	var page string
	var query string
	var all bool

	cmd := &cobra.Command{
		Use:   "events [<calendarId>]",
		Short: "List events from a calendar or all calendars",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}

			// Validate args
			if !all && len(args) == 0 {
				return usage("calendarId required unless --all is specified")
			}
			if all && len(args) > 0 {
				return usage("calendarId not allowed with --all flag")
			}

			now := time.Now().UTC()
			oneWeekLater := now.Add(7 * 24 * time.Hour)
			if strings.TrimSpace(from) == "" {
				from = now.Format(time.RFC3339)
			}
			if strings.TrimSpace(to) == "" {
				to = oneWeekLater.Format(time.RFC3339)
			}

			svc, err := newCalendarService(cmd.Context(), account)
			if err != nil {
				return err
			}

			if all {
				return listAllCalendarsEvents(cmd, svc, from, to, max, page, query)
			}
			calendarID := args[0]
			return listCalendarEvents(cmd, svc, calendarID, from, to, max, page, query)
		},
	}

	cmd.Flags().StringVar(&from, "from", "", "Start time (RFC3339; default: now)")
	cmd.Flags().StringVar(&to, "to", "", "End time (RFC3339; default: +7d)")
	cmd.Flags().Int64Var(&max, "max", 10, "Max results")
	cmd.Flags().StringVar(&page, "page", "", "Page token")
	cmd.Flags().StringVar(&query, "query", "", "Free text search")
	cmd.Flags().BoolVar(&all, "all", false, "Fetch events from all calendars")
	return cmd
}

func listCalendarEvents(cmd *cobra.Command, svc *calendar.Service, calendarID, from, to string, max int64, page, query string) error {
	u := ui.FromContext(cmd.Context())

	call := svc.Events.List(calendarID).
		TimeMin(from).
		TimeMax(to).
		MaxResults(max).
		PageToken(page).
		SingleEvents(true).
		OrderBy("startTime")
	if strings.TrimSpace(query) != "" {
		call = call.Q(query)
	}
	resp, err := call.Context(cmd.Context()).Do()
	if err != nil {
		return err
	}
	if outfmt.IsJSON(cmd.Context()) {
		return outfmt.WriteJSON(os.Stdout, map[string]any{
			"events":        resp.Items,
			"nextPageToken": resp.NextPageToken,
		})
	}

	if len(resp.Items) == 0 {
		u.Err().Println("No events")
		return nil
	}

	w, flush := tableWriter(cmd.Context())
	defer flush()

	fmt.Fprintln(w, "ID\tSTART\tEND\tSUMMARY")
	for _, e := range resp.Items {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", e.Id, eventStart(e), eventEnd(e), e.Summary)
	}
	printNextPageHint(u, resp.NextPageToken)
	return nil
}

type eventWithCalendar struct {
	*calendar.Event
	CalendarID string
}

func listAllCalendarsEvents(cmd *cobra.Command, svc *calendar.Service, from, to string, max int64, page, query string) error {
	u := ui.FromContext(cmd.Context())

	// Get all calendars
	calResp, err := svc.CalendarList.List().Context(cmd.Context()).Do()
	if err != nil {
		return err
	}

	if len(calResp.Items) == 0 {
		u.Err().Println("No calendars")
		return nil
	}

	// Collect events from all calendars
	var allEvents []*eventWithCalendar
	for _, cal := range calResp.Items {
		call := svc.Events.List(cal.Id).
			TimeMin(from).
			TimeMax(to).
			MaxResults(max).
			PageToken(page).
			SingleEvents(true).
			OrderBy("startTime")
		if strings.TrimSpace(query) != "" {
			call = call.Q(query)
		}
		resp, err := call.Context(cmd.Context()).Do()
		if err != nil {
			// Skip calendars that fail (e.g., due to permissions)
			continue
		}
		for _, e := range resp.Items {
			allEvents = append(allEvents, &eventWithCalendar{
				Event:      e,
				CalendarID: cal.Id,
			})
		}
	}

	if len(allEvents) == 0 {
		u.Err().Println("No events")
		return nil
	}

	// Sort events by start time
	sortEventsByStartTime(allEvents)

	if outfmt.IsJSON(cmd.Context()) {
		events := make([]map[string]any, 0, len(allEvents))
		for _, e := range allEvents {
			eventMap := map[string]any{
				"id":         e.Id,
				"calendarId": e.CalendarID,
				"summary":    e.Summary,
				"start":      e.Start,
				"end":        e.End,
				"status":     e.Status,
			}
			if e.Description != "" {
				eventMap["description"] = e.Description
			}
			if e.Location != "" {
				eventMap["location"] = e.Location
			}
			if len(e.Attendees) > 0 {
				eventMap["attendees"] = e.Attendees
			}
			events = append(events, eventMap)
		}
		return outfmt.WriteJSON(os.Stdout, map[string]any{"events": events})
	}

	w, flush := tableWriter(cmd.Context())
	defer flush()

	fmt.Fprintln(w, "CALENDAR\tID\tSTART\tEND\tSUMMARY")
	for _, e := range allEvents {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", e.CalendarID, e.Id, eventStart(e.Event), eventEnd(e.Event), e.Summary)
	}
	return nil
}

func sortEventsByStartTime(events []*eventWithCalendar) {
	// Simple insertion sort since we expect relatively small lists
	for i := 1; i < len(events); i++ {
		key := events[i]
		keyStart := eventStart(key.Event)
		j := i - 1
		for j >= 0 && eventStart(events[j].Event) > keyStart {
			events[j+1] = events[j]
			j--
		}
		events[j+1] = key
	}
}

func newCalendarEventCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "event <calendarId> <eventId>",
		Short: "Get event details",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}
			calendarID := args[0]
			eventID := args[1]

			svc, err := newCalendarService(cmd.Context(), account)
			if err != nil {
				return err
			}

			e, err := svc.Events.Get(calendarID, eventID).Do()
			if err != nil {
				return err
			}
			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{"event": e})
			}

			u.Out().Printf("id\t%s", e.Id)
			u.Out().Printf("summary\t%s", orEmpty(e.Summary, "(no title)"))
			u.Out().Printf("start\t%s", eventStart(e))
			u.Out().Printf("end\t%s", eventEnd(e))
			if e.Location != "" {
				u.Out().Printf("location\t%s", e.Location)
			}
			if e.Description != "" {
				u.Out().Printf("description\t%s", e.Description)
			}
			if len(e.Attendees) > 0 {
				addrs := make([]string, 0, len(e.Attendees))
				for _, a := range e.Attendees {
					if a != nil && a.Email != "" {
						addrs = append(addrs, a.Email)
					}
				}
				if len(addrs) > 0 {
					u.Out().Printf("attendees\t%s", strings.Join(addrs, ", "))
				}
			}
			if e.Status != "" {
				u.Out().Printf("status\t%s", e.Status)
			}
			if e.HtmlLink != "" {
				u.Out().Printf("link\t%s", e.HtmlLink)
			}
			return nil
		},
	}
}

func newCalendarCreateCmd(flags *rootFlags) *cobra.Command {
	var summary string
	var from string
	var to string
	var description string
	var location string
	var attendees string
	var allDay bool

	cmd := &cobra.Command{
		Use:   "create <calendarId>",
		Short: "Create a new event",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}
			calendarID := args[0]

			if strings.TrimSpace(summary) == "" || strings.TrimSpace(from) == "" || strings.TrimSpace(to) == "" {
				return usage("required: --summary, --from, --to")
			}

			svc, err := newCalendarService(cmd.Context(), account)
			if err != nil {
				return err
			}

			event := &calendar.Event{
				Summary:     summary,
				Description: description,
				Location:    location,
				Start:       buildEventDateTime(from, allDay),
				End:         buildEventDateTime(to, allDay),
				Attendees:   buildAttendees(attendees),
			}

			created, err := svc.Events.Insert(calendarID, event).Do()
			if err != nil {
				return err
			}
			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{"event": created})
			}
			u.Out().Printf("id\t%s", created.Id)
			if created.HtmlLink != "" {
				u.Out().Printf("link\t%s", created.HtmlLink)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&summary, "summary", "", "Event title (required)")
	cmd.Flags().StringVar(&from, "from", "", "Start time/date (required)")
	cmd.Flags().StringVar(&to, "to", "", "End time/date (required)")
	cmd.Flags().StringVar(&description, "description", "", "Event description")
	cmd.Flags().StringVar(&location, "location", "", "Event location")
	cmd.Flags().StringVar(&attendees, "attendees", "", "Attendees (comma-separated)")
	cmd.Flags().BoolVar(&allDay, "all-day", false, "Create all-day event (use YYYY-MM-DD for from/to)")
	return cmd
}

func newCalendarUpdateCmd(flags *rootFlags) *cobra.Command {
	var summary string
	var from string
	var to string
	var description string
	var location string
	var attendees string
	var allDay bool

	cmd := &cobra.Command{
		Use:   "update <calendarId> <eventId>",
		Short: "Update an existing event",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}
			calendarID := args[0]
			eventID := args[1]

			svc, err := newCalendarService(cmd.Context(), account)
			if err != nil {
				return err
			}

			existing, err := svc.Events.Get(calendarID, eventID).Do()
			if err != nil {
				return err
			}

			targetAllDay := isAllDayEvent(existing)
			if cmd.Flags().Changed("all-day") {
				targetAllDay = allDay
				// Converting between all-day and timed needs explicit start/end.
				if !cmd.Flags().Changed("from") || !cmd.Flags().Changed("to") {
					return usage("when changing --all-day, also provide --from and --to")
				}
			}

			changed := false

			if cmd.Flags().Changed("summary") {
				existing.Summary = summary
				changed = true
			}
			if cmd.Flags().Changed("description") {
				existing.Description = description
				changed = true
			}
			if cmd.Flags().Changed("location") {
				existing.Location = location
				changed = true
			}

			if cmd.Flags().Changed("from") {
				existing.Start = buildEventDateTime(from, targetAllDay)
				changed = true
			}
			if cmd.Flags().Changed("to") {
				existing.End = buildEventDateTime(to, targetAllDay)
				changed = true
			}

			if cmd.Flags().Changed("attendees") {
				existing.Attendees = buildAttendees(attendees)
				changed = true
			}

			if !changed {
				return usage("no updates provided")
			}

			updated, err := svc.Events.Update(calendarID, eventID, existing).Do()
			if err != nil {
				return err
			}
			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{"event": updated})
			}
			u.Out().Printf("id\t%s", updated.Id)
			if updated.HtmlLink != "" {
				u.Out().Printf("link\t%s", updated.HtmlLink)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&summary, "summary", "", "Event title")
	cmd.Flags().StringVar(&from, "from", "", "Start time/date (RFC3339 or YYYY-MM-DD)")
	cmd.Flags().StringVar(&to, "to", "", "End time/date (RFC3339 or YYYY-MM-DD)")
	cmd.Flags().StringVar(&description, "description", "", "Event description")
	cmd.Flags().StringVar(&location, "location", "", "Event location")
	cmd.Flags().StringVar(&attendees, "attendees", "", "Attendees (comma-separated)")
	cmd.Flags().BoolVar(&allDay, "all-day", false, "Treat from/to as all-day (YYYY-MM-DD)")
	return cmd
}

func newCalendarDeleteCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <calendarId> <eventId>",
		Short: "Delete an event",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}
			calendarID := args[0]
			eventID := args[1]

			if confirmErr := confirmDestructive(cmd, flags, fmt.Sprintf("delete calendar event %s/%s", calendarID, eventID)); confirmErr != nil {
				return confirmErr
			}

			svc, err := newCalendarService(cmd.Context(), account)
			if err != nil {
				return err
			}

			if err := svc.Events.Delete(calendarID, eventID).Do(); err != nil {
				return err
			}
			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{
					"deleted":    true,
					"calendarId": calendarID,
					"eventId":    eventID,
				})
			}
			u.Out().Printf("deleted\ttrue")
			u.Out().Printf("calendar_id\t%s", calendarID)
			u.Out().Printf("event_id\t%s", eventID)
			return nil
		},
	}
}

func newCalendarFreeBusyCmd(flags *rootFlags) *cobra.Command {
	var from string
	var to string

	cmd := &cobra.Command{
		Use:   "freebusy <calendarIds>",
		Short: "Check free/busy status for calendars (comma-separated IDs)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}
			calendarIDs := splitCSV(args[0])
			if len(calendarIDs) == 0 {
				return usage("no calendar IDs provided")
			}
			if strings.TrimSpace(from) == "" || strings.TrimSpace(to) == "" {
				return usage("required: --from and --to")
			}

			svc, err := newCalendarService(cmd.Context(), account)
			if err != nil {
				return err
			}

			items := make([]*calendar.FreeBusyRequestItem, 0, len(calendarIDs))
			for _, id := range calendarIDs {
				items = append(items, &calendar.FreeBusyRequestItem{Id: id})
			}

			resp, err := svc.Freebusy.Query(&calendar.FreeBusyRequest{
				TimeMin: from,
				TimeMax: to,
				Items:   items,
			}).Do()
			if err != nil {
				return err
			}
			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{"calendars": resp.Calendars})
			}
			if len(resp.Calendars) == 0 {
				u.Err().Println("No data")
				return nil
			}

			w, flush := tableWriter(cmd.Context())
			defer flush()
			fmt.Fprintln(w, "CALENDAR\tSTART\tEND")
			for id, data := range resp.Calendars {
				for _, b := range data.Busy {
					fmt.Fprintf(w, "%s\t%s\t%s\n", id, b.Start, b.End)
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&from, "from", "", "Start time (RFC3339, required)")
	cmd.Flags().StringVar(&to, "to", "", "End time (RFC3339, required)")
	return cmd
}

func buildEventDateTime(value string, allDay bool) *calendar.EventDateTime {
	value = strings.TrimSpace(value)
	if allDay {
		return &calendar.EventDateTime{Date: value}
	}
	return &calendar.EventDateTime{DateTime: value}
}

func buildAttendees(csv string) []*calendar.EventAttendee {
	addrs := splitCSV(csv)
	if len(addrs) == 0 {
		return nil
	}
	out := make([]*calendar.EventAttendee, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, &calendar.EventAttendee{Email: a})
	}
	return out
}

func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func eventStart(e *calendar.Event) string {
	if e == nil || e.Start == nil {
		return ""
	}
	if e.Start.DateTime != "" {
		return e.Start.DateTime
	}
	return e.Start.Date
}

func eventEnd(e *calendar.Event) string {
	if e == nil || e.End == nil {
		return ""
	}
	if e.End.DateTime != "" {
		return e.End.DateTime
	}
	return e.End.Date
}

func isAllDayEvent(e *calendar.Event) bool {
	return e != nil && e.Start != nil && e.Start.Date != ""
}

func orEmpty(s string, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}
