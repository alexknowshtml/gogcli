package cmd

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/api/gmail/v1"

	"github.com/steipete/gogcli/internal/app"
)

func newGmailServiceForTest(t *testing.T, h http.HandlerFunc) (*gmail.Service, func()) {
	t.Helper()

	return newGoogleTestService(t, h, gmail.NewService)
}

func stubGmailServiceForTest(t *testing.T, svc *gmail.Service) {
	t.Helper()
	stubGoogleTestService(t, &newGmailService, svc)
}

func withGmailTestService(ctx context.Context, svc *gmail.Service) context.Context {
	return withGmailTestServiceFactory(ctx, func(context.Context, string) (*gmail.Service, error) {
		return svc, nil
	})
}

func withGmailTestServiceFactory(ctx context.Context, factory app.GmailServiceFactory) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	runtime := &app.Runtime{}
	if existing, ok := app.FromContext(ctx); ok {
		*runtime = *existing
	}
	runtime.Services.Gmail = factory
	return app.WithRuntime(ctx, runtime)
}
