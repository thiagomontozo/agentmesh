package coordination

import (
	"context"
	"sync"
	"time"
)

type Memory struct {
	mu    sync.Mutex
	locks map[string]struct{}
}

func NewMemory() *Memory {
	return &Memory{locks: make(map[string]struct{})}
}

func (m *Memory) Acquire(_ context.Context, key string, _ time.Duration) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.locks[key]; exists {
		return nil, false, nil
	}
	m.locks[key] = struct{}{}
	return &memoryLease{coordinator: m, key: key}, true, nil
}

func (m *Memory) Ping(context.Context) error { return nil }
func (m *Memory) Close() error               { return nil }

type memoryLease struct {
	once        sync.Once
	coordinator *Memory
	key         string
}

func (l *memoryLease) Release(context.Context) error {
	l.once.Do(func() {
		l.coordinator.mu.Lock()
		delete(l.coordinator.locks, l.key)
		l.coordinator.mu.Unlock()
	})
	return nil
}

var _ Coordinator = (*Memory)(nil)
