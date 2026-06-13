package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/steipete/gogcli/internal/backup"
	gmailbackup "github.com/steipete/gogcli/internal/backup/gmail"
	"github.com/steipete/gogcli/internal/config"
	"github.com/steipete/gogcli/internal/ui"
)

type gmailBackupOptions struct {
	Query            string
	Max              int64
	IncludeSpamTrash bool
	ShardMaxRows     int
	AccountHash      string
	CacheMessages    bool
	RefreshCache     bool
	Checkpoints      bool
	CheckpointRows   int
	CheckpointEvery  time.Duration
	CheckpointRunID  string
	BackupOptions    backup.Options
	Cache            gmailbackup.Cache
}

type gmailBackupMessage = gmailbackup.Message

type gmailBackupLabel = gmailbackup.Label

func buildGmailBackupSnapshot(ctx context.Context, flags *RootFlags, opts gmailBackupOptions) (backup.Snapshot, error) {
	if opts.ShardMaxRows <= 0 {
		opts.ShardMaxRows = 1000
	}
	account, err := requireAccount(flags)
	if err != nil {
		return backup.Snapshot{}, err
	}
	svc, err := gmailService(ctx, account)
	if err != nil {
		return backup.Snapshot{}, err
	}
	source, err := gmailbackup.NewServiceSource(svc)
	if err != nil {
		return backup.Snapshot{}, err
	}
	accountHash := backupAccountHash(account)
	opts.AccountHash = accountHash
	labels, err := source.Labels(ctx)
	if err != nil {
		return backup.Snapshot{}, err
	}
	if opts.CacheMessages {
		if cacheErr := configureGmailBackupCache(ctx, &opts); cacheErr != nil {
			return backup.Snapshot{}, cacheErr
		}
	}
	shards := make([]backup.PlainShard, 0, 1)
	labelShard, err := backup.NewJSONLShard(backupServiceGmail, "labels", accountHash, fmt.Sprintf("data/gmail/%s/labels.jsonl.gz.age", accountHash), labels)
	if err != nil {
		return backup.Snapshot{}, err
	}
	shards = append(shards, labelShard)
	var messageCount int
	ids, err := gmailbackup.ListMessageIDs(ctx, source, gmailbackup.ListOptions{
		Selection: gmailBackupSelection(opts),
		Cache:     opts.Cache,
		UseCache:  opts.CacheMessages,
		Refresh:   opts.RefreshCache,
		Progress:  gmailBackupFetchProgress(ctx),
	})
	if err != nil {
		return backup.Snapshot{}, err
	}
	if opts.CacheMessages {
		opts.CheckpointRunID = gmailBackupResolvedCheckpointRunID(ctx, opts, ids)
		checkpointer := newGmailBackupCheckpointer(ctx, opts, len(ids))
		if _, cacheErr := gmailbackup.EnsureMessageCache(ctx, source, ids, gmailbackup.FetchOptions{
			AccountHash: opts.AccountHash,
			Cache:       opts.Cache,
			UseCache:    true,
			Refresh:     opts.RefreshCache,
			Progress:    gmailBackupFetchProgress(ctx),
			AfterMessage: func(ctx context.Context, messageID string, event gmailbackup.Event) error {
				return checkpointer.record(ctx, messageID, event.Done, event.Fetched, event.CacheHits)
			},
			ReleaseMemory: debug.FreeOSMemory,
		}); cacheErr != nil {
			return backup.Snapshot{}, cacheErr
		}
		messageShards, promoted, shardErr := buildGmailMessageShardsFromCheckpoint(ctx, opts, ids)
		if shardErr != nil {
			return backup.Snapshot{}, shardErr
		}
		if !promoted {
			messageShards, shardErr = gmailbackup.BuildMessageShards(ctx, opts.Cache, ids, gmailbackup.ShardOptions{
				AccountHash: opts.AccountHash,
				MaxRows:     opts.ShardMaxRows,
				Progress: func(event gmailbackup.ShardEvent) {
					switch event.Phase {
					case "index":
						gmailBackupProgressf(ctx, "backup gmail shard-index\t%d/%d", event.Done, event.Total)
					case "build":
						gmailBackupProgressf(ctx, "backup gmail shard-build\tshards=%d\tmessages=%d/%d", event.Shards, event.Done, event.Total)
					}
				},
			})
			if shardErr != nil {
				return backup.Snapshot{}, shardErr
			}
		}
		shards = append(shards, messageShards...)
		messageCount = len(ids)
	} else {
		messages, _, err := gmailbackup.FetchMessages(ctx, source, ids, gmailbackup.FetchOptions{
			Progress: gmailBackupFetchProgress(ctx),
		})
		if err != nil {
			return backup.Snapshot{}, err
		}
		messageShards, err := gmailbackup.BuildMessageShardsFromMessages(ctx, messages, gmailbackup.ShardOptions{
			AccountHash: accountHash,
			MaxRows:     opts.ShardMaxRows,
		})
		if err != nil {
			return backup.Snapshot{}, err
		}
		shards = append(shards, messageShards...)
		messageCount = len(messages)
	}
	return backup.Snapshot{
		Services: []string{backupServiceGmail},
		Accounts: []string{accountHash},
		Counts: map[string]int{
			"gmail.labels":   len(labels),
			"gmail.messages": messageCount,
		},
		Shards: shards,
	}, nil
}

func configureGmailBackupCache(ctx context.Context, opts *gmailBackupOptions) error {
	if opts == nil || !opts.CacheMessages || opts.Cache.Configured() {
		return nil
	}
	layout, err := commandLayout(ctx, config.PathKindCache)
	if err != nil {
		return err
	}
	cache, err := gmailbackup.NewCache(layout.CacheDir)
	if err != nil {
		return err
	}
	opts.Cache = cache
	return nil
}

type gmailBackupCheckpointer struct {
	enabled bool
	opts    gmailBackupOptions
	total   int
	part    int
	last    time.Time
	pending []string
}

const (
	gmailBackupShardKindMessages = gmailbackup.MessageShardKind
)

func newGmailBackupCheckpointer(ctx context.Context, opts gmailBackupOptions, total int) *gmailBackupCheckpointer {
	enabled := opts.Checkpoints &&
		opts.CacheMessages &&
		strings.TrimSpace(opts.AccountHash) != "" &&
		strings.TrimSpace(opts.CheckpointRunID) != "" &&
		(opts.CheckpointRows > 0 || opts.CheckpointEvery > 0)
	cp := &gmailBackupCheckpointer{
		enabled: enabled,
		opts:    opts,
		total:   total,
		last:    time.Now(),
	}
	if enabled {
		gmailBackupProgressf(ctx, "backup gmail checkpoint\trun=%s\trows=%d\tinterval=%s", opts.CheckpointRunID, opts.CheckpointRows, opts.CheckpointEvery)
	}
	return cp
}

func (c *gmailBackupCheckpointer) record(ctx context.Context, messageID string, done, fetched, cacheHits int) error {
	if c == nil || !c.enabled || strings.TrimSpace(messageID) == "" {
		return nil
	}
	c.pending = append(c.pending, messageID)
	if c.shouldFlush(done) {
		return c.flush(ctx, done, fetched, cacheHits)
	}
	return nil
}

func (c *gmailBackupCheckpointer) shouldFlush(done int) bool {
	if len(c.pending) == 0 {
		return false
	}
	if c.opts.CheckpointRows > 0 && len(c.pending) >= c.opts.CheckpointRows {
		return true
	}
	if c.opts.CheckpointEvery > 0 && time.Since(c.last) >= c.opts.CheckpointEvery {
		return true
	}
	return done == c.total
}

func (c *gmailBackupCheckpointer) flush(ctx context.Context, done, fetched, cacheHits int) error {
	if c == nil || !c.enabled || len(c.pending) == 0 {
		return nil
	}
	c.part++
	ids := append([]string(nil), c.pending...)
	c.pending = c.pending[:0]
	shards, err := gmailbackup.BuildCheckpointShards(ctx, c.opts.Cache, ids, gmailbackup.CheckpointShardOptions{
		AccountHash: c.opts.AccountHash,
		RunID:       c.opts.CheckpointRunID,
		FirstPart:   c.part,
	})
	if err != nil {
		return err
	}
	c.part += len(shards) - 1
	snapshot := backup.Snapshot{
		Services: []string{backupServiceGmail},
		Accounts: []string{c.opts.AccountHash},
		Counts:   map[string]int{"gmail.messages": len(ids)},
		Shards:   shards,
	}
	result, err := backup.PushCheckpoint(ctx, snapshot, backup.Checkpoint{
		RunID:     c.opts.CheckpointRunID,
		Service:   backupServiceGmail,
		Account:   c.opts.AccountHash,
		Done:      done,
		Total:     c.total,
		Fetched:   fetched,
		CacheHits: cacheHits,
	}, c.opts.BackupOptions)
	if err != nil {
		return err
	}
	c.last = time.Now()
	gmailBackupProgressf(ctx, "backup gmail checkpoint\t%d/%d\tparts=%d\trows=%d\tchanged=%t", done, c.total, len(shards), len(ids), result.Changed)
	return nil
}

func gmailBackupSelection(opts gmailBackupOptions) gmailbackup.Selection {
	return gmailbackup.Selection{
		AccountHash:      opts.AccountHash,
		Query:            opts.Query,
		Max:              opts.Max,
		IncludeSpamTrash: opts.IncludeSpamTrash,
	}
}

func gmailBackupFetchProgress(ctx context.Context) func(gmailbackup.Event) {
	return func(event gmailbackup.Event) {
		switch event.Phase {
		case gmailbackup.EventPhaseList:
			switch event.Resume {
			case "complete", "partial":
				gmailBackupProgressf(ctx, "backup gmail list\tresume=%s\tmessages=%d", event.Resume, event.Done)
			case "start":
				gmailBackupProgressf(ctx, "backup gmail list\tstart\tmessages=%d", event.Done)
			default:
				gmailBackupProgressf(ctx, "backup gmail list\tmessages=%d", event.Done)
			}
		case gmailbackup.EventPhaseFetch:
			if event.Done == 0 {
				gmailBackupProgressf(ctx, "backup gmail fetch\tqueued=%d", event.Total)
				return
			}
			gmailBackupProgressf(ctx, "backup gmail fetch\t%d/%d\tfetched=%d\tcache=%d", event.Done, event.Total, event.Fetched, event.CacheHits)
		}
	}
}

func gmailBackupProgressf(ctx context.Context, format string, args ...any) {
	u := ui.FromContext(ctx)
	if u == nil {
		return
	}
	u.Err().Linef(format, args...)
}

func buildGmailMessageShardsFromCheckpoint(ctx context.Context, opts gmailBackupOptions, ids []string) ([]backup.PlainShard, bool, error) {
	if !opts.CacheMessages || !opts.Checkpoints || strings.TrimSpace(opts.AccountHash) == "" || strings.TrimSpace(opts.CheckpointRunID) == "" {
		return nil, false, nil
	}
	cfg, err := backup.ResolveOptions(opts.BackupOptions)
	if err != nil {
		return nil, false, err
	}
	if len(cfg.Recipients) == 0 {
		recipient, recipientErr := backup.RecipientFromIdentity(cfg.Identity)
		if recipientErr != nil {
			return nil, false, recipientErr
		}
		cfg.Recipients = []string{recipient}
	}
	manifest, err := backup.ReadCheckpointManifest(cfg.Repo, gmailBackupCheckpointManifestRel(opts.AccountHash, opts.CheckpointRunID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !gmailBackupCheckpointCompleteForSelection(manifest, opts, ids) {
		return nil, false, nil
	}
	if !sameBackupRecipients(manifest.Recipients, cfg.Recipients) {
		gmailBackupProgressf(ctx, "backup gmail checkpoint-promote\tskip=recipients-changed\trun=%s", opts.CheckpointRunID)
		return nil, false, nil
	}
	shards := make([]backup.PlainShard, 0, len(manifest.Shards))
	rows := 0
	for _, entry := range manifest.Shards {
		if entry.Service != backupServiceGmail || entry.Kind != gmailBackupShardKindMessages || entry.Account != opts.AccountHash {
			return nil, false, fmt.Errorf("gmail checkpoint %s contains unexpected shard %s/%s/%s", opts.CheckpointRunID, entry.Service, entry.Kind, entry.Account)
		}
		shards = append(shards, backup.ExistingShard(entry, manifest.Recipients))
		rows += entry.Rows
	}
	if rows != len(ids) {
		return nil, false, fmt.Errorf("gmail checkpoint %s row count = %d, want %d", opts.CheckpointRunID, rows, len(ids))
	}
	gmailBackupProgressf(ctx, "backup gmail checkpoint-promote\trun=%s\tshards=%d\tmessages=%d", opts.CheckpointRunID, len(shards), rows)
	return shards, true, nil
}

func gmailBackupCheckpointRunID(opts gmailBackupOptions, ids []string) string {
	return time.Now().UTC().Format("20060102T150405Z") + "-" + gmailBackupCheckpointRunIDSuffix(opts, ids)
}

func gmailBackupCheckpointRunIDSuffix(opts gmailBackupOptions, ids []string) string {
	key := struct {
		AccountHash      string `json:"accountHash"`
		Query            string `json:"query,omitempty"`
		Max              int64  `json:"max,omitempty"`
		IncludeSpamTrash bool   `json:"includeSpamTrash"`
		IDs              int    `json:"ids"`
	}{
		AccountHash:      opts.AccountHash,
		Query:            strings.TrimSpace(opts.Query),
		Max:              opts.Max,
		IncludeSpamTrash: opts.IncludeSpamTrash,
		IDs:              len(ids),
	}
	data, _ := json.Marshal(key)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:6])
}

func gmailBackupResolvedCheckpointRunID(ctx context.Context, opts gmailBackupOptions, ids []string) string {
	generated := gmailBackupCheckpointRunID(opts, ids)
	if !opts.Checkpoints || !opts.CacheMessages || strings.TrimSpace(opts.AccountHash) == "" {
		return generated
	}
	suffix := gmailBackupCheckpointRunIDSuffix(opts, ids)
	cfg, err := backup.ResolveOptions(opts.BackupOptions)
	if err != nil {
		return generated
	}
	root := filepath.Join(cfg.Repo, "checkpoints", "gmail", opts.AccountHash)
	entries, err := os.ReadDir(root)
	if err != nil {
		return generated
	}
	runIDs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && strings.HasSuffix(entry.Name(), "-"+suffix) {
			runIDs = append(runIDs, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(runIDs)))
	for _, runID := range runIDs {
		manifest, err := backup.ReadCheckpointManifest(cfg.Repo, gmailBackupCheckpointManifestRel(opts.AccountHash, runID))
		if err != nil {
			continue
		}
		if !gmailBackupCheckpointMatchesSelection(manifest, opts, ids) {
			continue
		}
		gmailBackupProgressf(ctx, "backup gmail checkpoint\treuse=%s\tdone=%d/%d", runID, manifest.Done, manifest.Total)
		return runID
	}
	return generated
}

func gmailBackupCheckpointManifestRel(accountHash, runID string) string {
	return fmt.Sprintf("checkpoints/gmail/%s/%s/manifest.json", accountHash, runID)
}

func gmailBackupCheckpointMatchesSelection(manifest backup.CheckpointManifest, opts gmailBackupOptions, ids []string) bool {
	return manifest.Service == backupServiceGmail &&
		manifest.Account == opts.AccountHash &&
		manifest.Total == len(ids) &&
		strings.TrimSpace(manifest.RunID) != ""
}

func gmailBackupCheckpointCompleteForSelection(manifest backup.CheckpointManifest, opts gmailBackupOptions, ids []string) bool {
	return gmailBackupCheckpointMatchesSelection(manifest, opts, ids) &&
		manifest.Done == len(ids) &&
		manifest.Total == len(ids)
}

func sameBackupRecipients(a, b []string) bool {
	a = normalizedBackupStrings(a)
	b = normalizedBackupStrings(b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func normalizedBackupStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
