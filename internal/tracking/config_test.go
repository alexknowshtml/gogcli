package tracking

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigRoundTrip(t *testing.T) {
	// Use temp dir
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmpDir, "xdg-config"))
	t.Setenv("GOG_KEYRING_BACKEND", "file")
	t.Setenv("GOG_KEYRING_PASSWORD", "test-password")

	if err := SaveSecrets("testkey123", "adminkey456"); err != nil {
		t.Fatalf("SaveSecrets failed: %v", err)
	}

	cfg := &Config{
		Enabled:          true,
		WorkerURL:        "https://test.workers.dev",
		SecretsInKeyring: true,
	}

	if err := SaveConfig(cfg); err != nil {
		t.Fatalf("SaveConfig failed: %v", err)
	}

	loaded, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if loaded.WorkerURL != cfg.WorkerURL {
		t.Errorf("WorkerURL mismatch: got %q, want %q", loaded.WorkerURL, cfg.WorkerURL)
	}

	if loaded.TrackingKey != "testkey123" {
		t.Errorf("TrackingKey mismatch: got %q, want %q", loaded.TrackingKey, "testkey123")
	}

	if loaded.AdminKey != "adminkey456" {
		t.Errorf("AdminKey mismatch: got %q, want %q", loaded.AdminKey, "adminkey456")
	}

	if !loaded.IsConfigured() {
		t.Error("IsConfigured should return true")
	}

	path, pathErr := ConfigPath()
	if pathErr != nil {
		t.Fatalf("ConfigPath: %v", pathErr)
	}

	b, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("ReadFile: %v", readErr)
	}

	s := string(b)
	if strings.Contains(s, "tracking_key") || strings.Contains(s, "admin_key") {
		t.Fatalf("expected secrets omitted from config file, got:\n%s", s)
	}
}

func TestLoadConfigMissing(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmpDir, "xdg-config"))

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Enabled {
		t.Error("Expected Enabled to be false for missing config")
	}

	if cfg.IsConfigured() {
		t.Error("IsConfigured should return false for missing config")
	}
}
