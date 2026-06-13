package authclient

import (
	"context"
	"fmt"
	"strings"

	"github.com/steipete/gogcli/internal/config"
)

type (
	contextKey     struct{}
	accessTokenKey struct{}
	resolverKey    struct{}
)

type ClientResolver func(email string, override string) (string, error)

func WithClient(ctx context.Context, client string) context.Context {
	client = strings.TrimSpace(client)
	if client == "" {
		return ctx
	}

	return context.WithValue(ctx, contextKey{}, client)
}

func ClientOverrideFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if v := ctx.Value(contextKey{}); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}

	return ""
}

func WithAccessToken(ctx context.Context, token string) context.Context {
	token = strings.TrimSpace(token)
	if token == "" {
		return ctx
	}

	return context.WithValue(ctx, accessTokenKey{}, token)
}

func AccessTokenFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if v := ctx.Value(accessTokenKey{}); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}

	return ""
}

func WithClientResolver(ctx context.Context, resolver ClientResolver) context.Context {
	if resolver == nil {
		return ctx
	}

	return context.WithValue(ctx, resolverKey{}, resolver)
}

func ResolveClient(ctx context.Context, email string) (string, error) {
	override := ClientOverrideFromContext(ctx)
	if resolver := clientResolverFromContext(ctx); resolver != nil {
		client, err := resolver(email, override)
		if err != nil {
			return "", fmt.Errorf("resolve client: %w", err)
		}

		return client, nil
	}

	return ResolveClientWithOverride(email, override)
}

func ResolveClientWithOverrideContext(ctx context.Context, email string, override string) (string, error) {
	if resolver := clientResolverFromContext(ctx); resolver != nil {
		client, err := resolver(email, override)
		if err != nil {
			return "", fmt.Errorf("resolve client: %w", err)
		}

		return client, nil
	}

	return ResolveClientWithOverride(email, override)
}

func clientResolverFromContext(ctx context.Context) ClientResolver {
	if ctx == nil {
		return nil
	}

	resolver, _ := ctx.Value(resolverKey{}).(ClientResolver)

	return resolver
}

func ResolveClientWithOverride(email string, override string) (string, error) {
	cfg, err := config.ReadConfig()
	if err != nil {
		return "", fmt.Errorf("read config: %w", err)
	}

	client, err := config.ResolveClientForAccount(cfg, email, override)
	if err != nil {
		return "", fmt.Errorf("resolve client: %w", err)
	}

	return client, nil
}
