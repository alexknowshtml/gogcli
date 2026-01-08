package cmd

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"github.com/steipete/gogcli/internal/tracking"
	"github.com/steipete/gogcli/internal/ui"
)

type GmailTrackSetupCmd struct {
	WorkerURL   string `name:"worker-url" aliases:"domain" help:"Tracking worker base URL (e.g. https://gog-email-tracker.<acct>.workers.dev)"`
	TrackingKey string `name:"tracking-key" help:"Tracking key (base64; generates one if omitted)"`
	AdminKey    string `name:"admin-key" help:"Admin key for /opens (generates one if omitted)"`
}

func (c *GmailTrackSetupCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)

	cfg, err := tracking.LoadConfig()
	if err != nil {
		return fmt.Errorf("load tracking config: %w", err)
	}

	if c.WorkerURL == "" {
		c.WorkerURL = strings.TrimSpace(cfg.WorkerURL)
	}
	if c.WorkerURL == "" && !flags.NoInput {
		u.Err().Println("Tracking worker base URL (e.g. https://...workers.dev): ")
		line, readErr := bufio.NewReader(os.Stdin).ReadString('\n')
		if readErr == nil {
			c.WorkerURL = strings.TrimSpace(line)
		}
	}
	c.WorkerURL = strings.TrimSpace(c.WorkerURL)
	if c.WorkerURL == "" {
		return usage("required: --worker-url")
	}

	key := strings.TrimSpace(c.TrackingKey)
	if key == "" {
		key = strings.TrimSpace(cfg.TrackingKey)
	}
	if key == "" {
		key, err = tracking.GenerateKey()
		if err != nil {
			return fmt.Errorf("generate tracking key: %w", err)
		}
	}

	adminKey := strings.TrimSpace(c.AdminKey)
	if adminKey == "" {
		adminKey = strings.TrimSpace(cfg.AdminKey)
	}
	if adminKey == "" {
		adminKey, err = generateAdminKey()
		if err != nil {
			return fmt.Errorf("generate admin key: %w", err)
		}
	}

	if err := tracking.SaveSecrets(key, adminKey); err != nil {
		return fmt.Errorf("save tracking secrets: %w", err)
	}

	cfg.Enabled = true
	cfg.WorkerURL = c.WorkerURL
	cfg.SecretsInKeyring = true
	cfg.TrackingKey = ""
	cfg.AdminKey = ""

	if err := tracking.SaveConfig(cfg); err != nil {
		return fmt.Errorf("save tracking config: %w", err)
	}

	path, _ := tracking.ConfigPath()
	u.Out().Printf("configured\ttrue")
	if path != "" {
		u.Out().Printf("config_path\t%s", path)
	}
	u.Out().Printf("worker_url\t%s", cfg.WorkerURL)

	u.Err().Println("")
	u.Err().Println("Next steps (manual worker deploy):")
	u.Err().Println("  - cd internal/tracking/worker")
	u.Err().Println("  - use these values when prompted:")
	u.Err().Printf("    TRACKING_KEY=%s", key)
	u.Err().Printf("    ADMIN_KEY=%s", adminKey)
	u.Err().Println("  - wrangler secret put TRACKING_KEY")
	u.Err().Println("  - wrangler secret put ADMIN_KEY")
	u.Err().Println("  - wrangler d1 create gog-email-tracker (or choose a name)")
	u.Err().Println("  - wrangler d1 execute <db> --file schema.sql")
	u.Err().Println("  - update wrangler.toml with the D1 database_id")
	u.Err().Println("  - wrangler deploy")

	return nil
}

func generateAdminKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
