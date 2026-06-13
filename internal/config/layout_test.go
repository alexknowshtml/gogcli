//nolint:wsl_v5
package config

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

var (
	errUnexpectedDirectoryLookup = errors.New("unexpected directory lookup")
	errHomeUnavailable           = errors.New("home unavailable")
)

func TestResolveLayout(t *testing.T) {
	t.Parallel()

	userDirs := func(goos, home string) UserDirs {
		return UserDirs{
			GOOS:      goos,
			HomeDir:   func() (string, error) { return home, nil },
			ConfigDir: func() (string, error) { return filepath.Join(home, "system-config"), nil },
			CacheDir:  func() (string, error) { return filepath.Join(home, "system-cache"), nil },
		}
	}

	t.Run("GOG home splits all roots", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()

		layout, err := ResolveLayout(Env{GOGHome: filepath.Join(home, "gog")}, userDirs("darwin", home))
		if err != nil {
			t.Fatalf("ResolveLayout: %v", err)
		}
		if layout.Home != filepath.Join(home, "gog") {
			t.Fatalf("home = %q", layout.Home)
		}
		if layout.ConfigDir != filepath.Join(home, "gog", "config") ||
			layout.DataDir != filepath.Join(home, "gog", "data") ||
			layout.StateDir != filepath.Join(home, "gog", "state") ||
			layout.CacheDir != filepath.Join(home, "gog", "cache") {
			t.Fatalf("layout = %#v", layout)
		}
		if !layout.ExplicitConfig || !layout.ExplicitData || !layout.ExplicitState || !layout.ExplicitCache {
			t.Fatalf("explicit flags = %#v", layout)
		}
	})

	t.Run("per-kind override wins", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()
		data := filepath.Join(home, "direct-data")

		layout, err := ResolveLayout(Env{
			GOGHome:    filepath.Join(home, "gog"),
			GOGDataDir: data,
		}, userDirs("darwin", home))
		if err != nil {
			t.Fatalf("ResolveLayout: %v", err)
		}
		if layout.DataDir != data {
			t.Fatalf("data = %q, want %q", layout.DataDir, data)
		}
	})

	t.Run("absolute XDG roots", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()
		env := Env{
			XDGConfigHome: filepath.Join(home, "xdg-config"),
			XDGDataHome:   filepath.Join(home, "xdg-data"),
			XDGStateHome:  filepath.Join(home, "xdg-state"),
			XDGCacheHome:  filepath.Join(home, "xdg-cache"),
		}

		layout, err := ResolveLayout(env, userDirs("linux", home))
		if err != nil {
			t.Fatalf("ResolveLayout: %v", err)
		}
		if layout.ConfigDir != filepath.Join(env.XDGConfigHome, AppName) ||
			layout.DataDir != filepath.Join(env.XDGDataHome, AppName) ||
			layout.StateDir != filepath.Join(env.XDGStateHome, AppName) ||
			layout.CacheDir != filepath.Join(env.XDGCacheHome, AppName) {
			t.Fatalf("layout = %#v", layout)
		}
	})

	t.Run("relative XDG roots use Linux defaults", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()

		layout, err := ResolveLayout(Env{
			XDGConfigHome: "relative-config",
			XDGCacheHome:  "relative-cache",
		}, userDirs("linux", home))
		if err != nil {
			t.Fatalf("ResolveLayout: %v", err)
		}
		if layout.ConfigDir != filepath.Join(home, ".config", AppName) ||
			layout.DataDir != filepath.Join(home, ".local", "share", AppName) ||
			layout.StateDir != filepath.Join(home, ".local", "state", AppName) ||
			layout.CacheDir != filepath.Join(home, ".cache", AppName) {
			t.Fatalf("layout = %#v", layout)
		}
	})

	t.Run("non-XDG platforms share config default for data and state", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()
		layout, err := ResolveLayout(Env{}, userDirs("darwin", home))
		if err != nil {
			t.Fatalf("ResolveLayout: %v", err)
		}
		want := filepath.Join(home, "system-config", AppName)
		if layout.ConfigDir != want || layout.DataDir != want || layout.StateDir != want {
			t.Fatalf("layout = %#v, want shared %q", layout, want)
		}
		if layout.CacheDir != filepath.Join(home, "system-cache", AppName) {
			t.Fatalf("cache = %q", layout.CacheDir)
		}
	})

	t.Run("tilde overrides use injected home", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()
		layout, err := ResolveLayout(Env{GOGHome: "~/gog"}, userDirs("linux", home))
		if err != nil {
			t.Fatalf("ResolveLayout: %v", err)
		}
		if layout.Home != filepath.Join(home, "gog") {
			t.Fatalf("home = %q", layout.Home)
		}
	})

	t.Run("relative GOG override rejected", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()

		_, err := ResolveLayout(Env{GOGDataDir: "relative"}, userDirs("linux", home))
		if err == nil || !strings.Contains(err.Error(), "GOG_DATA_DIR") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestLayoutResolverIsLazy(t *testing.T) {
	t.Parallel()

	resolver := newLayoutResolver(
		Env{GOGConfigDir: t.TempDir()},
		UserDirs{
			GOOS:      "linux",
			HomeDir:   func() (string, error) { return "", errUnexpectedDirectoryLookup },
			ConfigDir: func() (string, error) { return "", errUnexpectedDirectoryLookup },
			CacheDir:  func() (string, error) { return "", errUnexpectedDirectoryLookup },
		},
	)
	if _, err := resolver.resolveKind(PathKindConfig); err != nil {
		t.Fatalf("resolve config override: %v", err)
	}
}

func TestResolveUserConfigBase(t *testing.T) {
	t.Parallel()

	userDirs := func(goos, home string) UserDirs {
		return UserDirs{
			GOOS:      goos,
			HomeDir:   func() (string, error) { return home, nil },
			ConfigDir: func() (string, error) { return filepath.Join(home, "system-config"), nil },
			CacheDir:  func() (string, error) { return filepath.Join(home, "system-cache"), nil },
		}
	}

	t.Run("absolute XDG ignores app override", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()
		xdg := filepath.Join(home, "xdg")
		got, err := resolveUserConfigBase(Env{
			GOGConfigDir:  filepath.Join(home, "explicit"),
			XDGConfigHome: xdg,
		}, userDirs("darwin", home))
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got != xdg {
			t.Fatalf("got %q, want %q", got, xdg)
		}
	})

	t.Run("Linux default", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()
		got, err := resolveUserConfigBase(Env{}, userDirs("linux", home))
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		want := filepath.Join(home, ".config")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})

	t.Run("platform config", func(t *testing.T) {
		t.Parallel()
		home := t.TempDir()
		got, err := resolveUserConfigBase(Env{}, userDirs("darwin", home))
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		want := filepath.Join(home, "system-config")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}

func TestResolveLayoutMemoizesUserHome(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	calls := 0
	layout, err := ResolveLayout(
		Env{GOGHome: "~/gog"},
		UserDirs{
			GOOS: "linux",
			HomeDir: func() (string, error) {
				calls++
				return home, nil
			},
		},
	)
	if err != nil {
		t.Fatalf("ResolveLayout: %v", err)
	}
	if calls != 1 {
		t.Fatalf("home resolver calls = %d, want 1", calls)
	}
	if layout.Home != filepath.Join(home, "gog") {
		t.Fatalf("home = %q", layout.Home)
	}
}

func TestLayoutResolverRejectsUnknownKindBeforeOverrides(t *testing.T) {
	t.Parallel()

	resolver := newLayoutResolver(
		Env{GOGHome: t.TempDir()},
		UserDirs{GOOS: "linux"},
	)
	_, err := resolver.resolveKind(PathKind(99))
	if err == nil || !strings.Contains(err.Error(), "unknown path kind") {
		t.Fatalf("error = %v", err)
	}
}

func TestLayoutResolverWrapsHomeExpansionError(t *testing.T) {
	t.Parallel()

	resolver := newLayoutResolver(
		Env{GOGHome: "~"},
		UserDirs{
			GOOS:    "linux",
			HomeDir: func() (string, error) { return "", errHomeUnavailable },
		},
	)

	_, err := resolver.resolveKind(PathKindConfig)
	if err == nil || !strings.Contains(err.Error(), "expand home dir") || !errors.Is(err, errHomeUnavailable) {
		t.Fatalf("error = %v", err)
	}
}

func TestLayoutDirRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	_, err := (Layout{}).Dir(PathKind(99))
	if err == nil || !strings.Contains(err.Error(), "unknown path kind") {
		t.Fatalf("error = %v", err)
	}
}
