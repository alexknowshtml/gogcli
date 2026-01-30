package googleapi

import (
	"context"
	"log/slog"

	"google.golang.org/api/docs/v1"

	"github.com/steipete/gogcli/internal/googleauth"
)

func NewDocs(ctx context.Context, email string) (*docs.Service, error) {
	slog.Debug("creating docs service", "email", email)

	opts, err := optionsForAccount(ctx, googleauth.ServiceDocs, email)
	if err != nil {
		return nil, err
	}

	svc, err := docs.NewService(ctx, opts...)
	if err != nil {
		slog.Error("failed to create docs service", "email", email, "error", err)
		return nil, err
	}

	slog.Debug("docs service created successfully", "email", email)
	return svc, nil
}
