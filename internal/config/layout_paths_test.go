package config

import (
	"path/filepath"
	"testing"
)

func TestLayoutDerivedPaths(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	layout := Layout{
		ConfigDir: filepath.Join(root, "config"),
		DataDir:   filepath.Join(root, "data"),
		StateDir:  filepath.Join(root, "state"),
		CacheDir:  filepath.Join(root, "cache"),
	}

	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "config", got: layout.ConfigPath(), want: filepath.Join(root, "config", "config.json")},
		{name: "batch", got: layout.BatchDir(), want: filepath.Join(root, "state", "batches")},
		{name: "keyring", got: layout.PrimaryKeyringDir(), want: filepath.Join(root, "data", "keyring")},
		{name: "legacy keyring", got: layout.LegacyKeyringDir(), want: filepath.Join(root, "config", "keyring")},
		{name: "downloads", got: layout.DriveDownloadsDir(), want: filepath.Join(root, "config", "drive-downloads")},
		{name: "attachments", got: layout.GmailAttachmentsDir(), want: filepath.Join(root, "config", "gmail-attachments")},
		{name: "watch", got: layout.PrimaryGmailWatchDir(), want: filepath.Join(root, "state", "gmail-watch")},
		{name: "legacy watch", got: layout.LegacyGmailWatchDir(), want: filepath.Join(root, "config", "state", "gmail-watch")},
		{name: "keep service account", got: layout.KeepServiceAccountPath("A@Example.com"), want: filepath.Join(root, "data", "keep-sa-YUBleGFtcGxlLmNvbQ.json")},
		{name: "legacy safe keep service account", got: layout.KeepServiceAccountLegacySafePath("A@Example.com"), want: filepath.Join(root, "config", "keep-sa-YUBleGFtcGxlLmNvbQ.json")},
		{name: "legacy raw keep service account", got: layout.KeepServiceAccountLegacyPath("A@Example.com"), want: filepath.Join(root, "config", "keep-sa-A@Example.com.json")},
		{name: "service account", got: layout.ServiceAccountPath("A@Example.com"), want: filepath.Join(root, "data", "sa-YUBleGFtcGxlLmNvbQ.json")},
		{name: "legacy service account", got: layout.ServiceAccountLegacyPath("A@Example.com"), want: filepath.Join(root, "config", "sa-YUBleGFtcGxlLmNvbQ.json")},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if test.got != test.want {
				t.Fatalf("got %q, want %q", test.got, test.want)
			}
		})
	}
}

func TestLayoutClientCredentialsPaths(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	layout := Layout{
		ConfigDir: filepath.Join(root, "config"),
		DataDir:   filepath.Join(root, "data"),
	}

	tests := []struct {
		name   string
		client string
		legacy bool
		want   string
	}{
		{name: "default", client: DefaultClientName, want: filepath.Join(root, "data", "credentials.json")},
		{name: "named", client: "work", want: filepath.Join(root, "data", "credentials-work.json")},
		{name: "legacy default", client: DefaultClientName, legacy: true, want: filepath.Join(root, "config", "credentials.json")},
		{name: "legacy named", client: "work", legacy: true, want: filepath.Join(root, "config", "credentials-work.json")},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var (
				got string
				err error
			)
			if test.legacy {
				got, err = layout.LegacyClientCredentialsPathFor(test.client)
			} else {
				got, err = layout.ClientCredentialsPathFor(test.client)
			}

			if err != nil {
				t.Fatalf("credentials path: %v", err)
			}

			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}
