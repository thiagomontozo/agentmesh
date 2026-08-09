package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

type fakeCache struct {
	mu       sync.Mutex
	values   map[string][]byte
	failGet  bool
	failPing bool
}

func newFakeCache() *fakeCache { return &fakeCache{values: make(map[string][]byte)} }

func (f *fakeCache) Get(_ context.Context, key string, destination any) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failGet {
		return false, errors.New("cache unavailable")
	}
	value, ok := f.values[key]
	if !ok {
		return false, nil
	}
	return true, json.Unmarshal(value, destination)
}

func (f *fakeCache) Set(_ context.Context, key string, value any, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	payload, err := json.Marshal(value)
	if err == nil {
		f.values[key] = payload
	}
	return err
}

func (f *fakeCache) Delete(_ context.Context, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range keys {
		delete(f.values, key)
	}
	return nil
}

func (f *fakeCache) Ping(context.Context) error {
	if f.failPing {
		return errors.New("cache unavailable")
	}
	return nil
}

func (*fakeCache) Close() error { return nil }

func TestCachedRepositoryReadsCachedAgent(t *testing.T) {
	ctx := context.Background()
	inner := NewMemory()
	cache := newFakeCache()
	repository := NewCached(inner, cache, time.Minute)
	original := domain.Agent{ID: "agt_1", Name: "cached"}
	if _, err := repository.CreateAgent(ctx, original); err != nil {
		t.Fatal(err)
	}
	if _, err := inner.CreateAgent(ctx, domain.Agent{ID: "agt_1", Name: "database"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := repository.GetAgent(ctx, "agt_1")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Name != original.Name {
		t.Fatalf("expected cached agent %q, got %q", original.Name, loaded.Name)
	}
}

func TestCachedRepositoryFallsBackWhenCacheReadFails(t *testing.T) {
	ctx := context.Background()
	inner := NewMemory()
	cache := newFakeCache()
	cache.failGet = true
	repository := NewCached(inner, cache, time.Minute)
	want := domain.Agent{ID: "agt_1", Name: "database"}
	if _, err := inner.CreateAgent(ctx, want); err != nil {
		t.Fatal(err)
	}
	loaded, err := repository.GetAgent(ctx, want.ID)
	if err != nil || loaded != want {
		t.Fatalf("unexpected fallback: agent=%+v err=%v", loaded, err)
	}
}
