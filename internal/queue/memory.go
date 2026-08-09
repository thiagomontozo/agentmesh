package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var ErrFull = errors.New("run queue is full")

type Memory struct {
	jobs chan string

	mu          sync.RWMutex
	deadLetters []DeadLetter
}

type DeadLetter struct {
	RunID string
	Cause string
	At    time.Time
}

func NewMemory(size int) *Memory {
	return &Memory{jobs: make(chan string, size)}
}

func (m *Memory) Enqueue(ctx context.Context, runID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.jobs <- runID:
		return nil
	default:
		return ErrFull
	}
}

func (m *Memory) Consume(ctx context.Context, workers int, handler Handler) error {
	var wg sync.WaitGroup
	for workerID := 1; workerID <= workers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case runID := <-m.jobs:
					if err := handler(ctx, runID); err != nil && ctx.Err() == nil {
						slog.Error("memory queue handler failed", "worker", id, "run_id", runID, "error", err)
						select {
						case <-ctx.Done():
							return
						case <-time.After(100 * time.Millisecond):
						}
						if err := m.Enqueue(ctx, runID); err != nil {
							slog.Error("memory queue could not requeue run", "run_id", runID, "error", err)
						}
					}
				}
			}
		}(workerID)
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (m *Memory) DeadLetter(_ context.Context, runID string, cause error) error {
	if cause == nil {
		return fmt.Errorf("dead letter cause is required")
	}
	m.mu.Lock()
	m.deadLetters = append(m.deadLetters, DeadLetter{RunID: runID, Cause: cause.Error(), At: time.Now().UTC()})
	m.mu.Unlock()
	return nil
}

func (m *Memory) DeadLetters() []DeadLetter {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]DeadLetter, len(m.deadLetters))
	copy(result, m.deadLetters)
	return result
}

func (m *Memory) Ping(context.Context) error { return nil }
func (m *Memory) Close() error               { return nil }

var _ Queue = (*Memory)(nil)
