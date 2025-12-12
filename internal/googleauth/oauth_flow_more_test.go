package googleauth

import (
	"net/url"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

func TestAuthURLParams(t *testing.T) {
	t.Parallel()

	cfg := oauth2.Config{
		ClientID:    "id",
		Endpoint:    oauth2.Endpoint{AuthURL: "https://example.com/auth"},
		RedirectURL: "http://localhost",
		Scopes:      []string{"s1"},
	}

	u1 := cfg.AuthCodeURL("state", authURLParams(false)...)
	parsed1, err := url.Parse(u1)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q1 := parsed1.Query()
	if q1.Get("access_type") != "offline" {
		t.Fatalf("expected offline, got: %q", q1.Get("access_type"))
	}
	if q1.Get("include_granted_scopes") != "true" {
		t.Fatalf("expected include_granted_scopes=true, got: %q", q1.Get("include_granted_scopes"))
	}
	if q1.Get("prompt") != "" {
		t.Fatalf("expected no prompt, got: %q", q1.Get("prompt"))
	}

	u2 := cfg.AuthCodeURL("state", authURLParams(true)...)
	parsed2, err := url.Parse(u2)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed2.Query().Get("prompt") != "consent" {
		t.Fatalf("expected consent prompt, got: %q", parsed2.Query().Get("prompt"))
	}
}

func TestRandomState(t *testing.T) {
	t.Parallel()

	s1, err := randomState()
	if err != nil {
		t.Fatalf("randomState: %v", err)
	}
	s2, err := randomState()
	if err != nil {
		t.Fatalf("randomState: %v", err)
	}
	if s1 == "" || s2 == "" || s1 == s2 {
		t.Fatalf("expected two non-empty distinct states")
	}
	// base64 RawURLEncoding charset should not include '+' or '/' or '='.
	if strings.ContainsAny(s1, "+/=") || strings.ContainsAny(s2, "+/=") {
		t.Fatalf("unexpected charset: %q %q", s1, s2)
	}
}
