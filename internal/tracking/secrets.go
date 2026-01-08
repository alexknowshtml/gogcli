package tracking

import (
	"errors"
	"fmt"

	"github.com/99designs/keyring"

	"github.com/steipete/gogcli/internal/secrets"
)

var (
	errMissingTrackingKey = errors.New("missing tracking key")
	errMissingAdminKey    = errors.New("missing admin key")
)

const (
	trackingKeySecretKey = "tracking/tracking_key"
	adminKeySecretKey    = "tracking/admin_key"
)

func SaveSecrets(trackingKey, adminKey string) error {
	if trackingKey == "" {
		return errMissingTrackingKey
	}

	if adminKey == "" {
		return errMissingAdminKey
	}

	if err := secrets.SetSecret(trackingKeySecretKey, []byte(trackingKey)); err != nil {
		return fmt.Errorf("store tracking key: %w", err)
	}

	if err := secrets.SetSecret(adminKeySecretKey, []byte(adminKey)); err != nil {
		return fmt.Errorf("store admin key: %w", err)
	}

	return nil
}

func LoadSecrets() (trackingKey, adminKey string, err error) {
	b, err := secrets.GetSecret(trackingKeySecretKey)
	if err != nil {
		if errors.Is(err, keyring.ErrKeyNotFound) {
			return "", "", nil
		}

		return "", "", fmt.Errorf("read tracking key: %w", err)
	}

	ab, err := secrets.GetSecret(adminKeySecretKey)
	if err != nil {
		if errors.Is(err, keyring.ErrKeyNotFound) {
			return "", "", nil
		}

		return "", "", fmt.Errorf("read admin key: %w", err)
	}

	return string(b), string(ab), nil
}
