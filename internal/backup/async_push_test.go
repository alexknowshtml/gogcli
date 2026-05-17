package backup

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitAsyncPushRetryCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitAsyncPushRetry(ctx, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestAsyncPusherProgressCanBeUpdated(t *testing.T) {
	var got []string
	p := &asyncRepoPusher{
		progress: func(format string, _ ...any) {
			got = append(got, "old:"+format)
		},
	}

	p.setProgress(func(format string, _ ...any) {
		got = append(got, "new:"+format)
	})
	p.progressf("event")

	if len(got) != 1 || got[0] != "new:event" {
		t.Fatalf("unexpected progress calls: %#v", got)
	}
}

func TestAsyncPushErrorEvictsFailedPusher(t *testing.T) {
	repo := t.TempDir()
	wantErr := errors.New("push failed")
	p := &asyncRepoPusher{err: wantErr}

	asyncPushers.mu.Lock()
	oldPushers := asyncPushers.m
	asyncPushers.m = map[string]*asyncRepoPusher{repo: p}
	asyncPushers.mu.Unlock()
	t.Cleanup(func() {
		asyncPushers.mu.Lock()
		asyncPushers.m = oldPushers
		asyncPushers.mu.Unlock()
	})

	err := asyncPushError(repo)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected push error, got %v", err)
	}
	if existingAsyncPusher(repo, nil) != nil {
		t.Fatalf("expected failed pusher to be evicted")
	}
	if err := asyncPushError(repo); err != nil {
		t.Fatalf("expected second error check to be clear, got %v", err)
	}
}

func TestEnqueueAsyncPushEvictsFailedPusher(t *testing.T) {
	repo := t.TempDir()
	wantErr := errors.New("push failed")
	p := &asyncRepoPusher{err: wantErr}

	asyncPushers.mu.Lock()
	oldPushers := asyncPushers.m
	asyncPushers.m = map[string]*asyncRepoPusher{repo: p}
	asyncPushers.mu.Unlock()
	t.Cleanup(func() {
		asyncPushers.mu.Lock()
		asyncPushers.m = oldPushers
		asyncPushers.mu.Unlock()
	})

	err := enqueueAsyncPush(context.Background(), Config{Repo: repo}, Options{}, "abc", "msg")
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected push error, got %v", err)
	}
	if existingAsyncPusher(repo, nil) != nil {
		t.Fatalf("expected failed pusher to be evicted")
	}
}
