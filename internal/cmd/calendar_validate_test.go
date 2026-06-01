package cmd

import "testing"

func requireUsageError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error")
	}
	if got := ExitCode(err); got != 2 {
		t.Fatalf("ExitCode = %d, want 2 (err=%v)", got, err)
	}
}

func TestValidateColorID(t *testing.T) {
	if got, err := validateColorId(""); err != nil || got != "" {
		t.Fatalf("expected empty ok, got %q %v", got, err)
	}
	if got, err := validateColorId("3"); err != nil || got != "3" {
		t.Fatalf("expected valid id, got %q %v", got, err)
	}
	_, err := validateColorId("12")
	requireUsageError(t, err)
	_, err = validateColorId("nope")
	requireUsageError(t, err)
}

func TestValidateCalendarColorID(t *testing.T) {
	if got, err := validateCalendarColorId(""); err != nil || got != "" {
		t.Fatalf("expected empty ok, got %q %v", got, err)
	}
	if got, err := validateCalendarColorId("24"); err != nil || got != "24" {
		t.Fatalf("expected valid id, got %q %v", got, err)
	}
	_, err := validateCalendarColorId("25")
	requireUsageError(t, err)
	_, err = validateCalendarColorId("nope")
	requireUsageError(t, err)
}

func TestValidateVisibilityMore(t *testing.T) {
	if got, err := validateVisibility(""); err != nil || got != "" {
		t.Fatalf("expected empty ok, got %q %v", got, err)
	}
	if got, err := validateVisibility("Public"); err != nil || got != "public" {
		t.Fatalf("expected public, got %q %v", got, err)
	}
	_, err := validateVisibility("nope")
	requireUsageError(t, err)
}

func TestValidateTransparencyMore(t *testing.T) {
	if got, err := validateTransparency(""); err != nil || got != "" {
		t.Fatalf("expected empty ok, got %q %v", got, err)
	}
	if got, err := validateTransparency("busy"); err != nil || got != transparencyOpaque {
		t.Fatalf("expected opaque, got %q %v", got, err)
	}
	if got, err := validateTransparency("free"); err != nil || got != transparencyTransparent {
		t.Fatalf("expected transparent, got %q %v", got, err)
	}
	_, err := validateTransparency("nope")
	requireUsageError(t, err)
}

func TestValidateSendUpdatesMore(t *testing.T) {
	if got, err := validateSendUpdates(""); err != nil || got != "" {
		t.Fatalf("expected empty ok, got %q %v", got, err)
	}
	if got, err := validateSendUpdates("all"); err != nil || got != scopeAll {
		t.Fatalf("expected all, got %q %v", got, err)
	}
	if got, err := validateSendUpdates("externalonly"); err != nil || got != "externalOnly" {
		t.Fatalf("expected externalOnly, got %q %v", got, err)
	}
	if got, err := validateSendUpdates("none"); err != nil || got != "none" {
		t.Fatalf("expected none, got %q %v", got, err)
	}
	_, err := validateSendUpdates("nope")
	requireUsageError(t, err)
}
