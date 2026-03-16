package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steipete/gogcli/internal/outfmt"
	"github.com/steipete/gogcli/internal/ui"
	"google.golang.org/api/calendar/v3"
)

func newCalendarCreateCalendarCmd(flags *rootFlags) *cobra.Command {
	var description string
	var timeZone string
	var location string

	cmd := &cobra.Command{
		Use:   "create-calendar <name>",
		Short: "Create a new secondary calendar",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u := ui.FromContext(cmd.Context())
			account, err := requireAccount(flags)
			if err != nil {
				return err
			}

			name := strings.TrimSpace(args[0])
			if name == "" {
				return usage("required: calendar name")
			}

			svc, err := newCalendarService(cmd.Context(), account)
			if err != nil {
				return err
			}

			cal := &calendar.Calendar{
				Summary:     name,
				Description: strings.TrimSpace(description),
				TimeZone:    strings.TrimSpace(timeZone),
				Location:    strings.TrimSpace(location),
			}

			created, err := svc.Calendars.Insert(cal).Context(cmd.Context()).Do()
			if err != nil {
				return fmt.Errorf("create calendar: %w", err)
			}

			if outfmt.IsJSON(cmd.Context()) {
				return outfmt.WriteJSON(os.Stdout, map[string]any{"calendar": created})
			}

			u.Out().Printf("id\t%s", created.Id)
			u.Out().Printf("summary\t%s", created.Summary)
			if created.TimeZone != "" {
				u.Out().Printf("timezone\t%s", created.TimeZone)
			}
			if created.Description != "" {
				u.Out().Printf("description\t%s", created.Description)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&description, "description", "", "Calendar description")
	cmd.Flags().StringVar(&timeZone, "timezone", "", "IANA timezone (e.g., America/New_York)")
	cmd.Flags().StringVar(&location, "location", "", "Calendar location")
	return cmd
}
