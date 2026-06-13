//nolint:wsl_v5
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var (
	errPathMustBeAbsolute   = errors.New("path must be absolute")
	errUnknownPathKind      = errors.New("unknown path kind")
	errNilLayoutResolver    = errors.New("layout resolver is nil")
	errNilHomeDirResolver   = errors.New("home directory resolver is nil")
	errNilConfigDirResolver = errors.New("config directory resolver is nil")
	errNilCacheDirResolver  = errors.New("cache directory resolver is nil")
)

type PathKind int

const (
	PathKindConfig PathKind = iota
	PathKindData
	PathKindState
	PathKindCache
)

type Layout struct {
	Home           string
	ConfigDir      string
	DataDir        string
	StateDir       string
	CacheDir       string
	ExplicitConfig bool
	ExplicitData   bool
	ExplicitState  bool
	ExplicitCache  bool
	UsesXDG        bool
	UsesXDGState   bool
}

type Env struct {
	HomeOverride  string
	GOGHome       string
	GOGConfigDir  string
	GOGDataDir    string
	GOGStateDir   string
	GOGCacheDir   string
	XDGConfigHome string
	XDGDataHome   string
	XDGStateHome  string
	XDGCacheHome  string
}

type UserDirs struct {
	GOOS      string
	HomeDir   func() (string, error)
	ConfigDir func() (string, error)
	CacheDir  func() (string, error)
}

// Resolver captures path-related environment and platform directory lookups
// for one application runtime. Resolution stays lazy and is safe for
// concurrent use.
type Resolver struct {
	mu       sync.Mutex
	resolver *layoutResolver
}

func NewResolver(env Env, dirs UserDirs) *Resolver {
	return &Resolver{resolver: newLayoutResolver(env, dirs)}
}

func NewSystemResolver(homeOverride string) *Resolver {
	env := currentLayoutEnv()
	if strings.TrimSpace(homeOverride) != "" {
		env.HomeOverride = homeOverride
	}
	return NewResolver(env, systemUserDirs())
}

func (r *Resolver) Resolve(kinds ...PathKind) (Layout, error) {
	if r == nil || r.resolver == nil {
		return Layout{}, errNilLayoutResolver
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.resolver.resolveLayoutFor(kinds...)
}

func ResolveLayout(env Env, dirs UserDirs) (Layout, error) {
	resolver := NewResolver(env, dirs)
	layout, err := resolver.Resolve(PathKindConfig, PathKindData, PathKindState, PathKindCache)
	if err != nil {
		return Layout{}, err
	}

	resolver.mu.Lock()
	home, _, err := resolver.resolver.homeOverride()
	resolver.mu.Unlock()
	if err != nil {
		return Layout{}, err
	}

	layout.Home = home
	return layout, nil
}

func (l Layout) Dir(kind PathKind) (string, error) {
	switch kind {
	case PathKindConfig:
		return l.ConfigDir, nil
	case PathKindData:
		return l.DataDir, nil
	case PathKindState:
		return l.StateDir, nil
	case PathKindCache:
		return l.CacheDir, nil
	default:
		return "", fmt.Errorf("%w: %d", errUnknownPathKind, kind)
	}
}

type layoutResolver struct {
	env     Env
	dirs    UserDirs
	usesXDG bool

	home       string
	homeErr    error
	homeLoaded bool

	config       string
	configErr    error
	configLoaded bool

	cache       string
	cacheErr    error
	cacheLoaded bool
}

func newLayoutResolver(env Env, dirs UserDirs) *layoutResolver {
	return &layoutResolver{
		env:     env,
		dirs:    dirs,
		usesXDG: usesXDGDefaultsFor(dirs.GOOS),
	}
}

func (r *layoutResolver) resolveKind(kind PathKind) (string, error) {
	if !kind.valid() {
		return "", fmt.Errorf("%w: %d", errUnknownPathKind, kind)
	}

	if override, ok, err := r.kindOverride(kind); ok || err != nil {
		return override, err
	}

	if home, ok, err := r.homeOverride(); ok || err != nil {
		return filepath.Join(home, kindName(kind)), err
	}

	if xdg := strings.TrimSpace(r.env.xdg(kind)); filepath.IsAbs(xdg) {
		return filepath.Join(xdg, AppName), nil
	}

	base, err := r.defaultBase(kind)
	if err != nil {
		return "", err
	}

	return filepath.Join(base, AppName), nil
}

func (r *layoutResolver) kindOverride(kind PathKind) (string, bool, error) {
	raw := strings.TrimSpace(r.env.gogKind(kind))
	if raw == "" {
		return "", false, nil
	}

	expanded, err := r.expandPath(raw)
	if err != nil {
		return "", true, err
	}

	if !filepath.IsAbs(expanded) {
		return "", true, fmt.Errorf("%w: %s=%s", errPathMustBeAbsolute, gogKindEnvVar(kind), raw)
	}

	return expanded, true, nil
}

func (r *layoutResolver) homeOverride() (string, bool, error) {
	raw := strings.TrimSpace(r.env.HomeOverride)
	if raw == "" {
		raw = strings.TrimSpace(r.env.GOGHome)
	}
	if raw == "" {
		return "", false, nil
	}

	expanded, err := r.expandPath(raw)
	if err != nil {
		return "", true, err
	}

	if !filepath.IsAbs(expanded) {
		return "", true, fmt.Errorf("%w: GOG_HOME=%s", errPathMustBeAbsolute, raw)
	}

	return expanded, true, nil
}

func (r *layoutResolver) defaultBase(kind PathKind) (string, error) {
	switch kind {
	case PathKindConfig:
		return r.configDefaultBase()
	case PathKindCache:
		return r.cacheDefaultBase()
	case PathKindData:
		if r.usesXDG {
			return r.homeJoin(".local", "share")
		}

		return r.configDefaultBase()
	case PathKindState:
		if r.usesXDG {
			return r.homeJoin(".local", "state")
		}

		return r.configDefaultBase()
	default:
		return "", fmt.Errorf("%w: %d", errUnknownPathKind, kind)
	}
}

func (r *layoutResolver) configDefaultBase() (string, error) {
	if xdg := strings.TrimSpace(r.env.XDGConfigHome); filepath.IsAbs(xdg) {
		return xdg, nil
	}

	if strings.TrimSpace(r.env.XDGConfigHome) != "" && r.usesXDG {
		return r.homeJoin(".config")
	}

	return r.userConfigDir()
}

func (r *layoutResolver) cacheDefaultBase() (string, error) {
	if strings.TrimSpace(r.env.XDGCacheHome) != "" && r.usesXDG {
		return r.homeJoin(".cache")
	}

	return r.userCacheDir()
}

func (r *layoutResolver) expandPath(path string) (string, error) {
	if path == "~" {
		home, err := r.userHomeDir()
		if err != nil {
			return "", fmt.Errorf("expand home dir: %w", err)
		}

		return home, nil
	}

	if strings.HasPrefix(path, "~/") || strings.HasPrefix(path, "~\\") {
		home, err := r.userHomeDir()
		if err != nil {
			return "", fmt.Errorf("expand home dir: %w", err)
		}

		return filepath.Join(home, strings.TrimLeft(path[2:], `/\`)), nil
	}

	return path, nil
}

func (r *layoutResolver) homeJoin(parts ...string) (string, error) {
	home, err := r.userHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home dir: %w", err)
	}
	return filepath.Join(append([]string{home}, parts...)...), nil
}

func (r *layoutResolver) userHomeDir() (string, error) {
	if !r.homeLoaded {
		r.homeLoaded = true
		if r.dirs.HomeDir == nil {
			r.homeErr = errNilHomeDirResolver
		} else {
			r.home, r.homeErr = r.dirs.HomeDir()
		}
	}
	return r.home, r.homeErr
}

func (r *layoutResolver) userConfigDir() (string, error) {
	if !r.configLoaded {
		r.configLoaded = true
		if r.dirs.ConfigDir == nil {
			r.configErr = errNilConfigDirResolver
		} else {
			r.config, r.configErr = r.dirs.ConfigDir()
		}
	}

	if r.configErr != nil {
		return "", fmt.Errorf("resolve user config dir: %w", r.configErr)
	}
	return r.config, nil
}

func (r *layoutResolver) userCacheDir() (string, error) {
	if !r.cacheLoaded {
		r.cacheLoaded = true
		if r.dirs.CacheDir == nil {
			r.cacheErr = errNilCacheDirResolver
		} else {
			r.cache, r.cacheErr = r.dirs.CacheDir()
		}
	}

	if r.cacheErr != nil {
		return "", fmt.Errorf("resolve user cache dir: %w", r.cacheErr)
	}
	return r.cache, nil
}

func (env Env) hasExplicit(kind PathKind) bool {
	return strings.TrimSpace(env.HomeOverride) != "" ||
		strings.TrimSpace(env.GOGHome) != "" ||
		strings.TrimSpace(env.gogKind(kind)) != ""
}

func (env Env) gogKind(kind PathKind) string {
	switch kind {
	case PathKindConfig:
		return env.GOGConfigDir
	case PathKindData:
		return env.GOGDataDir
	case PathKindState:
		return env.GOGStateDir
	case PathKindCache:
		return env.GOGCacheDir
	default:
		return ""
	}
}

func (env Env) xdg(kind PathKind) string {
	switch kind {
	case PathKindConfig:
		return env.XDGConfigHome
	case PathKindData:
		return env.XDGDataHome
	case PathKindState:
		return env.XDGStateHome
	case PathKindCache:
		return env.XDGCacheHome
	default:
		return ""
	}
}

func kindName(kind PathKind) string {
	switch kind {
	case PathKindConfig:
		return "config"
	case PathKindData:
		return "data"
	case PathKindState:
		return "state"
	case PathKindCache:
		return "cache"
	default:
		return ""
	}
}

func (kind PathKind) valid() bool {
	return kind >= PathKindConfig && kind <= PathKindCache
}

func gogKindEnvVar(kind PathKind) string {
	switch kind {
	case PathKindConfig:
		return "GOG_CONFIG_DIR"
	case PathKindData:
		return "GOG_DATA_DIR"
	case PathKindState:
		return "GOG_STATE_DIR"
	case PathKindCache:
		return "GOG_CACHE_DIR"
	default:
		return ""
	}
}

func usesXDGDefaultsFor(goos string) bool {
	switch goos {
	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly":
		return true
	default:
		return false
	}
}

func currentLayoutEnv() Env {
	return Env{
		HomeOverride:  homeOverride,
		GOGHome:       os.Getenv("GOG_HOME"),
		GOGConfigDir:  os.Getenv("GOG_CONFIG_DIR"),
		GOGDataDir:    os.Getenv("GOG_DATA_DIR"),
		GOGStateDir:   os.Getenv("GOG_STATE_DIR"),
		GOGCacheDir:   os.Getenv("GOG_CACHE_DIR"),
		XDGConfigHome: os.Getenv("XDG_CONFIG_HOME"),
		XDGDataHome:   os.Getenv("XDG_DATA_HOME"),
		XDGStateHome:  os.Getenv("XDG_STATE_HOME"),
		XDGCacheHome:  os.Getenv("XDG_CACHE_HOME"),
	}
}

func systemUserDirs() UserDirs {
	return UserDirs{
		GOOS:      runtime.GOOS,
		HomeDir:   os.UserHomeDir,
		ConfigDir: os.UserConfigDir,
		CacheDir:  os.UserCacheDir,
	}
}

func currentLayoutDir(kind PathKind) (string, error) {
	layout, err := NewSystemResolver(homeOverride).Resolve(kind)
	if err != nil {
		return "", err
	}
	return layout.Dir(kind)
}

func currentLayoutFor(kinds ...PathKind) (Layout, error) {
	return NewSystemResolver(homeOverride).Resolve(kinds...)
}

func ResolveSystemLayoutFor(homeOverride string, kinds ...PathKind) (Layout, error) {
	return NewSystemResolver(homeOverride).Resolve(kinds...)
}

func ResolveUserConfigBase() (string, error) {
	return resolveUserConfigBase(currentLayoutEnv(), systemUserDirs())
}

func resolveUserConfigBase(env Env, dirs UserDirs) (string, error) {
	if xdg := strings.TrimSpace(env.XDGConfigHome); filepath.IsAbs(xdg) {
		return xdg, nil
	}
	if usesXDGDefaultsFor(dirs.GOOS) {
		home, err := dirs.HomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home config dir: %w", err)
		}
		return filepath.Join(home, ".config"), nil
	}

	configDir, err := dirs.ConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	if filepath.IsAbs(configDir) {
		return configDir, nil
	}
	home, err := dirs.HomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home config dir: %w", err)
	}
	return filepath.Join(home, ".config"), nil
}

func resolveLayoutFor(env Env, dirs UserDirs, kinds ...PathKind) (Layout, error) {
	return NewResolver(env, dirs).Resolve(kinds...)
}

func (r *layoutResolver) resolveLayoutFor(kinds ...PathKind) (Layout, error) {
	layout := Layout{
		ExplicitConfig: r.env.hasExplicit(PathKindConfig),
		ExplicitData:   r.env.hasExplicit(PathKindData),
		ExplicitState:  r.env.hasExplicit(PathKindState),
		ExplicitCache:  r.env.hasExplicit(PathKindCache),
		UsesXDG:        r.usesXDG,
		UsesXDGState:   filepath.IsAbs(strings.TrimSpace(r.env.XDGStateHome)),
	}

	for _, kind := range kinds {
		dir, err := r.resolveKind(kind)
		if err != nil {
			return Layout{}, err
		}
		layout.setDir(kind, dir)
	}

	return layout, nil
}

func (l *Layout) setDir(kind PathKind, dir string) {
	switch kind {
	case PathKindConfig:
		l.ConfigDir = dir
	case PathKindData:
		l.DataDir = dir
	case PathKindState:
		l.StateDir = dir
	case PathKindCache:
		l.CacheDir = dir
	}
}
