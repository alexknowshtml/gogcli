package tracking

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/steipete/gogcli/internal/config"
)

// Config holds tracking configuration
type Config struct {
	Enabled          bool   `json:"enabled"`
	WorkerURL        string `json:"worker_url"`
	SecretsInKeyring bool   `json:"secrets_in_keyring,omitempty"`
	TrackingKey      string `json:"tracking_key,omitempty"`
	AdminKey         string `json:"admin_key,omitempty"`
}

// ConfigPath returns the path to the tracking config file
func ConfigPath() (string, error) {
	dir, err := config.Dir()
	if err != nil {
		return "", fmt.Errorf("config dir: %w", err)
	}

	return filepath.Join(dir, "tracking.json"), nil
}

func legacyConfigPath() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("user config dir: %w", err)
	}

	return filepath.Join(configDir, "gog", "tracking.json"), nil
}

func readConfigBytes(path string) ([]byte, bool, error) {
	// #nosec G304 -- path is derived from user config dir
	data, readErr := os.ReadFile(path)
	if readErr == nil {
		return data, true, nil
	}

	if !os.IsNotExist(readErr) {
		return nil, false, fmt.Errorf("read tracking config: %w", readErr)
	}

	legacyPath, legacyErr := legacyConfigPath()
	if legacyErr != nil {
		return nil, false, fmt.Errorf("legacy config path: %w", legacyErr)
	}

	// #nosec G304 -- path is derived from user config dir
	legacyData, legacyReadErr := os.ReadFile(legacyPath)
	if legacyReadErr == nil {
		return legacyData, true, nil
	}

	if os.IsNotExist(legacyReadErr) {
		return nil, false, nil
	}

	return nil, false, fmt.Errorf("read legacy tracking config: %w", legacyReadErr)
}

// LoadConfig loads tracking configuration from disk
func LoadConfig() (*Config, error) {
	path, err := ConfigPath()
	if err != nil {
		return nil, err
	}

	data, ok, err := readConfigBytes(path)
	if err != nil {
		return nil, err
	}

	if !ok {
		return &Config{Enabled: false}, nil
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse tracking config: %w", err)
	}

	if strings.TrimSpace(cfg.TrackingKey) == "" || strings.TrimSpace(cfg.AdminKey) == "" || cfg.SecretsInKeyring {
		trackingKey, adminKey, secretErr := LoadSecrets()
		if secretErr != nil {
			return nil, secretErr
		}

		if strings.TrimSpace(cfg.TrackingKey) == "" {
			cfg.TrackingKey = trackingKey
		}

		if strings.TrimSpace(cfg.AdminKey) == "" {
			cfg.AdminKey = adminKey
		}
	}

	return &cfg, nil
}

// SaveConfig saves tracking configuration to disk
func SaveConfig(cfg *Config) error {
	path, err := ConfigPath()
	if err != nil {
		return err
	}

	// Ensure directory exists
	if _, mkErr := config.EnsureDir(); mkErr != nil {
		return fmt.Errorf("ensure config dir: %w", mkErr)
	}

	toSave := cfg
	if cfg.SecretsInKeyring {
		s := *cfg
		s.TrackingKey = ""
		s.AdminKey = ""
		toSave = &s
	}

	data, err := json.MarshalIndent(toSave, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal tracking config: %w", err)
	}

	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		return fmt.Errorf("write tracking config: %w", writeErr)
	}

	return nil
}

// IsConfigured returns true if tracking is set up
func (c *Config) IsConfigured() bool {
	return c.Enabled && c.WorkerURL != "" && c.TrackingKey != ""
}
