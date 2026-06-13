package authclient

import (
	"context"
	"testing"
)

func TestWithAccessToken_EmptyToken(t *testing.T) {
	ctx := context.Background()
	if got := AccessTokenFromContext(WithAccessToken(ctx, "")); got != "" {
		t.Fatalf("expected empty token, got %q", got)
	}
}

func TestWithAccessToken_TrimsWhitespace(t *testing.T) {
	ctx := context.Background()
	if got := AccessTokenFromContext(WithAccessToken(ctx, "  ya29.test-token  ")); got != "ya29.test-token" {
		t.Fatalf("expected trimmed token, got %q", got)
	}
}

func TestAccessTokenFromContext_NilContext(t *testing.T) {
	//nolint:staticcheck // intentional nil for regression coverage
	if got := AccessTokenFromContext(nil); got != "" {
		t.Fatalf("expected empty token, got %q", got)
	}
}

func TestResolveClientUsesContextResolver(t *testing.T) {
	t.Parallel()

	ctx := WithClientResolver(context.Background(), func(email string, override string) (string, error) {
		if email != "user@example.com" || override != "work" {
			t.Fatalf("resolver args = %q, %q", email, override)
		}

		return "resolved", nil
	})
	ctx = WithClient(ctx, "work")

	client, err := ResolveClient(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("ResolveClient: %v", err)
	}

	if client != "resolved" {
		t.Fatalf("client = %q, want resolved", client)
	}
}

func TestResolveClientWithOverrideContextUsesResolver(t *testing.T) {
	t.Parallel()

	ctx := WithClientResolver(context.Background(), func(_ string, override string) (string, error) {
		return override, nil
	})

	client, err := ResolveClientWithOverrideContext(ctx, "user@example.com", "custom")
	if err != nil {
		t.Fatalf("ResolveClientWithOverrideContext: %v", err)
	}

	if client != "custom" {
		t.Fatalf("client = %q, want custom", client)
	}
}
