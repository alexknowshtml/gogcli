package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureDirAndPaths(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	dir, err := EnsureDir()
	if err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}
	if _, statErr := os.Stat(dir); statErr != nil {
		t.Fatalf("stat dir: %v", statErr)
	}
	if !strings.Contains(dir, AppName) {
		t.Fatalf("expected app name in path: %q", dir)
	}

	credPath, err := ClientCredentialsPath()
	if err != nil {
		t.Fatalf("ClientCredentialsPath: %v", err)
	}
	if filepath.Base(credPath) != "credentials.json" {
		t.Fatalf("unexpected credentials base: %q", filepath.Base(credPath))
	}

	dd, err := EnsureDriveDownloadsDir()
	if err != nil {
		t.Fatalf("EnsureDriveDownloadsDir: %v", err)
	}
	if _, statErr := os.Stat(dd); statErr != nil {
		t.Fatalf("stat drive downloads dir: %v", statErr)
	}

	ad, err := EnsureGmailAttachmentsDir()
	if err != nil {
		t.Fatalf("EnsureGmailAttachmentsDir: %v", err)
	}
	if _, statErr := os.Stat(ad); statErr != nil {
		t.Fatalf("stat gmail attachments dir: %v", statErr)
	}
}
