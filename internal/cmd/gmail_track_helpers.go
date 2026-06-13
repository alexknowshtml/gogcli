package cmd

import (
	"context"
	"fmt"

	"github.com/steipete/gogcli/internal/config"
	"github.com/steipete/gogcli/internal/tracking"
)

func newTrackingConfigStore(ctx context.Context) (*tracking.ConfigStore, error) {
	layout, err := commandLayout(ctx, config.PathKindConfig, config.PathKindState)
	if err != nil {
		return nil, err
	}
	legacyConfigBase := ""
	if !layout.ExplicitState {
		legacyConfigBase, err = config.ResolveUserConfigBase()
		if err != nil {
			return nil, err
		}
	}
	return tracking.NewConfigStore(layout, legacyConfigBase)
}

func loadTrackingConfigForAccount(ctx context.Context, flags *RootFlags) (string, *tracking.Config, *tracking.ConfigStore, error) {
	return loadTrackingConfigForAccountWith(ctx, flags, (*tracking.ConfigStore).Load)
}

func loadTrackingConfigMetadataForAccount(ctx context.Context, flags *RootFlags) (string, *tracking.Config, *tracking.ConfigStore, error) {
	return loadTrackingConfigForAccountWith(ctx, flags, (*tracking.ConfigStore).LoadMetadata)
}

func loadTrackingConfigForAccountWith(
	ctx context.Context,
	flags *RootFlags,
	loadConfig func(*tracking.ConfigStore, string) (*tracking.Config, error),
) (string, *tracking.Config, *tracking.ConfigStore, error) {
	account, err := requireAccount(flags)
	if err != nil {
		return "", nil, nil, err
	}
	store, err := newTrackingConfigStore(ctx)
	if err != nil {
		return "", nil, nil, err
	}

	cfg, err := loadConfig(store, account)
	if err != nil {
		return "", nil, nil, fmt.Errorf("load tracking config: %w", err)
	}

	return account, cfg, store, nil
}
