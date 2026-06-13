package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/steipete/gogcli/internal/config"
	"github.com/steipete/gogcli/internal/filelock"
)

const gmailWatchLockTimeout = 5 * time.Second

type gmailWatchStore struct {
	path  string
	lock  *filelock.Lock
	mu    sync.Mutex
	state gmailWatchState
}

func gmailWatchStatePath(layout config.Layout, account string) (string, error) {
	dir := layout.GmailWatchDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("ensure gmail watch dir: %w", err)
	}
	name := sanitizeAccountForPath(account)
	path := filepath.Join(dir, name+".json")
	if _, statErr := os.Stat(path); statErr == nil {
		return path, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", statErr
	}

	if !layout.ExplicitState {
		legacyDir := layout.LegacyGmailWatchDir()
		legacyPath := filepath.Join(legacyDir, name+".json")
		if legacyPath != path {
			if _, statErr := os.Stat(legacyPath); statErr == nil {
				return legacyPath, nil
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return "", statErr
			}
		}
	}

	return path, nil
}

func sanitizeAccountForPath(account string) string {
	clean := strings.TrimSpace(strings.ToLower(account))
	if clean == "" {
		return "unknown"
	}
	var b strings.Builder
	b.Grow(len(clean))
	for _, r := range clean {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.' || r == '-' || r == '_' || r == '@':
			b.WriteRune('_')
		case r > unicode.MaxASCII:
			b.WriteRune('_')
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

func newGmailWatchStore(ctx context.Context, account string) (*gmailWatchStore, error) {
	layout, err := commandLayout(ctx, config.PathKindConfig, config.PathKindState)
	if err != nil {
		return nil, err
	}
	return newGmailWatchStoreForLayout(layout, account)
}

func newGmailWatchStoreForLayout(layout config.Layout, account string) (*gmailWatchStore, error) {
	path, err := gmailWatchStatePath(layout, account)
	if err != nil {
		return nil, err
	}
	return &gmailWatchStore{
		path: path,
		lock: filelock.Shared(filepath.Join(filepath.Dir(path), ".lock"), gmailWatchLockTimeout),
	}, nil
}

func loadGmailWatchStore(ctx context.Context, account string) (*gmailWatchStore, error) {
	layout, err := commandLayout(ctx, config.PathKindConfig, config.PathKindState)
	if err != nil {
		return nil, err
	}
	return loadGmailWatchStoreForLayout(layout, account)
}

func loadGmailWatchStoreForLayout(layout config.Layout, account string) (*gmailWatchStore, error) {
	store, err := newGmailWatchStoreForLayout(layout, account)
	if err != nil {
		return nil, err
	}
	err = store.lock.WithExclusive(func() error {
		data, readErr := os.ReadFile(store.path)
		if readErr != nil {
			if errors.Is(readErr, os.ErrNotExist) {
				return errors.New("watch state not found; run gmail watch start")
			}
			return readErr
		}
		if decodeErr := json.Unmarshal(data, &store.state); decodeErr != nil {
			return decodeErr
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return store, nil
}

func readGmailWatchStateOptional(ctx context.Context, account string) (gmailWatchState, bool, error) {
	layout, err := commandLayout(ctx, config.PathKindConfig, config.PathKindState)
	if err != nil {
		return gmailWatchState{}, false, err
	}
	return readGmailWatchStateOptionalForLayout(layout, account)
}

func readGmailWatchStateOptionalForLayout(layout config.Layout, account string) (gmailWatchState, bool, error) {
	name := sanitizeAccountForPath(account) + ".json"
	paths := []string{filepath.Join(layout.GmailWatchDir(), name)}
	if !layout.ExplicitState {
		legacyPath := filepath.Join(layout.LegacyGmailWatchDir(), name)
		if legacyPath != paths[0] {
			paths = append(paths, legacyPath)
		}
	}

	for _, path := range paths {
		data, err := os.ReadFile(path) //nolint:gosec // path is derived from the configured state directory and sanitized account.
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return gmailWatchState{}, false, err
		}
		var state gmailWatchState
		if err := json.Unmarshal(data, &state); err != nil {
			return gmailWatchState{}, false, err
		}
		return state, true, nil
	}
	return gmailWatchState{}, false, nil
}

func (s *gmailWatchStore) Get() gmailWatchState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *gmailWatchStore) Update(fn func(*gmailWatchState) error) error {
	return s.lock.WithExclusive(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := s.reloadLocked(); err != nil {
			return err
		}
		if err := fn(&s.state); err != nil {
			return err
		}
		return s.saveLocked()
	})
}

func (s *gmailWatchStore) reloadLocked() error {
	if s.path == "" {
		return nil
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("reload watch state: %w", err)
	}
	var state gmailWatchState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("reload watch state: %w", err)
	}
	s.state = state
	return nil
}

func (s *gmailWatchStore) Save() error {
	return s.lock.WithExclusive(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.saveLocked()
	})
}

func (s *gmailWatchStore) saveLocked() error {
	if s.path == "" {
		return errors.New("missing watch state path")
	}
	payload, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	return config.WriteFileAtomic(s.path, append(payload, '\n'), 0o600)
}

func (s *gmailWatchStore) Remove() error {
	return s.lock.WithExclusive(func() error {
		if s.path == "" {
			return nil
		}
		if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove watch state: %w", err)
		}
		return nil
	})
}

func (s *gmailWatchStore) StartHistoryID(pushHistory string) (uint64, error) {
	var startID uint64
	err := s.lock.WithExclusive(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := s.reloadLocked(); err != nil {
			return err
		}

		pushID, pushOK, pushErr := parseHistoryIDOptional(pushHistory)

		// If no stored state, use push historyId.
		if s.state.HistoryID == "" {
			if !pushOK {
				if pushErr != nil {
					return pushErr
				}
				return nil
			}
			if pushErr != nil {
				return pushErr
			}
			s.state.HistoryID = formatHistoryID(pushID)
			s.state.UpdatedAtMs = time.Now().UnixMilli()
			if err := s.saveLocked(); err != nil {
				return err
			}
			startID = pushID
			return nil
		}

		storedID, storedOK, parseErr := parseHistoryIDOptional(s.state.HistoryID)
		if parseErr != nil {
			return parseErr
		}
		if !storedOK {
			return nil
		}
		if pushErr != nil || !pushOK {
			startID = storedID
			return nil
		}
		if pushID > storedID {
			startID = storedID
		}
		return nil
	})

	return startID, err
}

func parseHistoryIDOptional(raw string) (uint64, bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, false, nil
	}
	id, err := parseHistoryID(trimmed)
	if err != nil {
		return 0, true, err
	}
	return id, true, nil
}

func compareHistoryIDs(storedRaw, candidateRaw string) (storedID, candidateID uint64, storedOK, candidateOK bool, err error) {
	storedID, storedOK, err = parseHistoryIDOptional(storedRaw)
	if err != nil {
		return 0, 0, false, false, err
	}
	candidateID, candidateOK, err = parseHistoryIDOptional(candidateRaw)
	if err != nil {
		return storedID, 0, storedOK, true, err
	}
	return storedID, candidateID, storedOK, candidateOK, nil
}

func shouldUpdateHistoryID(currentRaw, candidateRaw string) (bool, error) {
	currentID, candidateID, currentOK, candidateOK, err := compareHistoryIDs(currentRaw, candidateRaw)
	if err != nil {
		return false, err
	}
	if !candidateOK {
		return false, nil
	}
	if !currentOK {
		return true, nil
	}
	return candidateID >= currentID, nil
}

func isStaleHistoryID(currentRaw, candidateRaw string) (bool, error) {
	currentID, candidateID, currentOK, candidateOK, err := compareHistoryIDs(currentRaw, candidateRaw)
	if err != nil {
		return false, err
	}
	if !currentOK || !candidateOK {
		return false, nil
	}
	return candidateID <= currentID, nil
}

func parseHistoryID(raw string) (uint64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, errors.New("historyId is required")
	}
	id, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid historyId %q", trimmed)
	}
	return id, nil
}

func formatHistoryID(id uint64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatUint(id, 10)
}
