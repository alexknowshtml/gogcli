package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveBodyInput_File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "body.txt")
	if err := os.WriteFile(path, []byte("Line 1\nLine 2\n"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := resolveBodyInput("", path)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "Line 1\nLine 2\n" {
		t.Fatalf("unexpected body: %q", got)
	}
}

func TestResolveBodyInput_Stdin(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stdin
	t.Cleanup(func() { os.Stdin = old })
	os.Stdin = r

	if _, err := w.Write([]byte("stdin body")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got, err := resolveBodyInput("", "-")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "stdin body" {
		t.Fatalf("unexpected body: %q", got)
	}
}

func TestResolveBodyInput_Conflict(t *testing.T) {
	_, err := resolveBodyInput("body", "/tmp/body.txt")
	if err == nil {
		t.Fatalf("expected conflict error")
	}
	if !strings.Contains(err.Error(), "--body") {
		t.Fatalf("unexpected error: %v", err)
	}
}
