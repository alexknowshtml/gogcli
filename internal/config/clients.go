package config

import (
	"errors"
	"fmt"
	"strings"
)

const DefaultClientName = "default"

var (
	errInvalidClientName = errors.New("invalid client name")
	errInvalidDomainName = errors.New("invalid domain name")
	errMissingEmail      = errors.New("missing email")
)

func NormalizeClientName(raw string) (string, error) {
	name := strings.ToLower(strings.TrimSpace(raw))
	if name == "" {
		return "", fmt.Errorf("%w: empty", errInvalidClientName)
	}

	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}

		return "", fmt.Errorf("%w: %q", errInvalidClientName, raw)
	}

	return name, nil
}

func NormalizeClientNameOrDefault(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return DefaultClientName, nil
	}

	return NormalizeClientName(raw)
}

func NormalizeDomain(raw string) (string, error) {
	domain := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(raw)), "@")
	if domain == "" {
		return "", fmt.Errorf("%w: empty", errInvalidDomainName)
	}

	if !strings.Contains(domain, ".") {
		return "", fmt.Errorf("%w: %q", errInvalidDomainName, raw)
	}

	for _, r := range domain {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}

		return "", fmt.Errorf("%w: %q", errInvalidDomainName, raw)
	}

	return domain, nil
}

func DomainFromEmail(email string) string {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return ""
	}

	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return ""
	}

	return strings.TrimSpace(parts[1])
}

func ResolveClientForAccount(cfg File, email string, override string) (string, error) {
	return ResolveClientForAccountWithCredentials(cfg, email, override, ClientCredentialsExists)
}

func ResolveClientForAccountWithCredentials(
	cfg File,
	email string,
	override string,
	credentialsExist func(string) (bool, error),
) (string, error) {
	if strings.TrimSpace(override) != "" {
		return NormalizeClientNameOrDefault(override)
	}

	email = strings.ToLower(strings.TrimSpace(email))
	if email != "" {
		if client, ok := cfg.AccountClients[email]; ok && strings.TrimSpace(client) != "" {
			return NormalizeClientNameOrDefault(client)
		}
	}

	domain := DomainFromEmail(email)
	if domain != "" {
		if client, ok := cfg.ClientDomains[domain]; ok && strings.TrimSpace(client) != "" {
			return NormalizeClientNameOrDefault(client)
		}

		if credentialsExist != nil {
			ok, err := credentialsExist(domain)
			if err == nil && ok {
				if normalized, err := NormalizeClientName(domain); err == nil {
					return normalized, nil
				}
			}
		}
	}

	return DefaultClientName, nil
}

func SetAccountClient(cfg *File, email string, client string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return errMissingEmail
	}

	normalized, err := NormalizeClientNameOrDefault(client)
	if err != nil {
		return err
	}

	if cfg.AccountClients == nil {
		cfg.AccountClients = make(map[string]string)
	}
	cfg.AccountClients[email] = normalized

	return nil
}

func AccountClient(cfg File, email string) (string, bool) {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return "", false
	}

	client, ok := cfg.AccountClients[email]
	if !ok || strings.TrimSpace(client) == "" {
		return "", false
	}

	normalized, err := NormalizeClientNameOrDefault(client)
	if err != nil {
		return "", false
	}

	return normalized, true
}

func SetClientDomain(cfg *File, domain string, client string) error {
	normalizedDomain, err := NormalizeDomain(domain)
	if err != nil {
		return err
	}

	normalizedClient, err := NormalizeClientNameOrDefault(client)
	if err != nil {
		return err
	}

	if cfg.ClientDomains == nil {
		cfg.ClientDomains = make(map[string]string)
	}
	cfg.ClientDomains[normalizedDomain] = normalizedClient

	return nil
}

func ClientForDomain(cfg File, domain string) (string, bool) {
	normalizedDomain, err := NormalizeDomain(domain)
	if err != nil {
		return "", false
	}

	client, ok := cfg.ClientDomains[normalizedDomain]
	if !ok || strings.TrimSpace(client) == "" {
		return "", false
	}

	normalizedClient, err := NormalizeClientNameOrDefault(client)
	if err != nil {
		return "", false
	}

	return normalizedClient, true
}

type ClientCredentialsInfo struct {
	Client  string `json:"client"`
	Path    string `json:"path"`
	Default bool   `json:"default"`
}

func ListClientCredentials() ([]ClientCredentialsInfo, error) {
	store, err := DefaultClientCredentialsStore()
	if err != nil {
		return nil, err
	}

	return store.List()
}
