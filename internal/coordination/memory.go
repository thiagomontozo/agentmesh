package coordination

import (
	"context"
	"sync"
	"time"
)

type Memory struct {
	mu    sync.Mutex
	locks map[string]memoryLock
	next  uint64
}

type memoryLock struct {
	token     uint64
	expiresAt time.Time
}

func NewMemory() *Memory {
	return &Memory{locks: make(map[string]memoryLock)}
}

func (m *Memory) Acquire(ctx context.Context, key string, ttl time.Duration) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if ttl <= 0 {
		return nil, false, ErrInvalidLeaseTTL
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if current, exists := m.locks[key]; exists && now.Before(current.expiresAt) {
		return nil, false, nil
	}
	m.next++
	token := m.next
	m.locks[key] = memoryLock{token: token, expiresAt: now.Add(ttl)}
	return &memoryLease{coordinator: m, key: key, token: token}, true, nil
}

func (m *Memory) Ping(context.Context) error { return nil }
func (m *Memory) Close() error               { return nil }

type memoryLease struct {
	once        sync.Once
	coordinator *Memory
	key         string
	token       uint64
}

func (l *memoryLease) Renew(ctx context.Context, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		return ErrInvalidLeaseTTL
	}
	l.coordinator.mu.Lock()
	defer l.coordinator.mu.Unlock()
	current, exists := l.coordinator.locks[l.key]
	if !exists || current.token != l.token || !time.Now().Before(current.expiresAt) {
		if exists && current.token == l.token {
			delete(l.coordinator.locks, l.key)
		}
		return ErrLeaseLost
	}
	current.expiresAt = time.Now().Add(ttl)
	l.coordinator.locks[l.key] = current
	return nil
}

func (l *memoryLease) Release(context.Context) error {
	l.once.Do(func() {
		l.coordinator.mu.Lock()
		if current, exists := l.coordinator.locks[l.key]; exists && current.token == l.token {
			delete(l.coordinator.locks, l.key)
		}
		l.coordinator.mu.Unlock()
	})
	return nil
}

var _ Coordinator = (*Memory)(nil)
