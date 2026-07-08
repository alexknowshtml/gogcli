package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"google.golang.org/api/calendar/v3"

	"github.com/steipete/gogcli/internal/outfmt"
	"github.com/steipete/gogcli/internal/ui"
)

type CalendarWatchCmd struct {
	CalendarID string `arg:"" name:"calendarId" help:"Calendar ID to watch"`
	WebhookURL string `arg:"" name:"webhookUrl" help:"HTTPS URL to receive push notifications"`
	Token      string `name:"token" help:"Verification token sent in X-Goog-Channel-Token header"`
	TTL        int64  `name:"ttl" help:"Channel TTL in seconds (Google caps at 7 days regardless)"`
}

func (c *CalendarWatchCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)

	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	svc, err := newCalendarService(ctx, account)
	if err != nil {
		return err
	}

	calendarID, err := resolveCalendarSelector(ctx, svc, c.CalendarID, false)
	if err != nil {
		return err
	}

	channelID := uuid.New().String()

	channel := &calendar.Channel{
		Id:      channelID,
		Type:    "web_hook",
		Address: c.WebhookURL,
		Token:   c.Token,
	}
	if c.TTL > 0 {
		channel.Params = map[string]string{"ttl": fmt.Sprintf("%d", c.TTL)}
	}

	if dryRunErr := dryRunExit(ctx, flags, "calendar.watch", map[string]any{
		"calendarId": calendarID,
		"channelId":  channelID,
		"address":    c.WebhookURL,
		"token":      c.Token,
	}); dryRunErr != nil {
		return dryRunErr
	}

	result, err := svc.Events.Watch(calendarID, channel).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(ctx, os.Stdout, map[string]any{"channel": result})
	}

	u.Out().Printf("channelId\t%s", result.Id)
	u.Out().Printf("resourceId\t%s", result.ResourceId)
	if result.Expiration > 0 {
		u.Out().Printf("expiration\t%s", time.UnixMilli(result.Expiration).UTC().Format(time.RFC3339))
	}
	return nil
}

type CalendarUnwatchCmd struct {
	ChannelID  string `arg:"" name:"channelId" help:"Channel ID returned by 'calendar watch'"`
	ResourceID string `arg:"" name:"resourceId" help:"Resource ID returned by 'calendar watch'"`
}

func (c *CalendarUnwatchCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)

	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	svc, err := newCalendarService(ctx, account)
	if err != nil {
		return err
	}

	channel := &calendar.Channel{
		Id:         c.ChannelID,
		ResourceId: c.ResourceID,
	}

	if dryRunErr := dryRunExit(ctx, flags, "calendar.unwatch", map[string]any{
		"channelId":  c.ChannelID,
		"resourceId": c.ResourceID,
	}); dryRunErr != nil {
		return dryRunErr
	}

	if err := svc.Channels.Stop(channel).Context(ctx).Do(); err != nil {
		return fmt.Errorf("unwatch: %w", err)
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(ctx, os.Stdout, map[string]any{
			"stopped":   true,
			"channelId": c.ChannelID,
		})
	}

	u.Out().Printf("stopped\tchannel %s", c.ChannelID)
	return nil
}
