package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryQueueReportsFull(t *testing.T) {
	q := NewMemory(1)
	if err := q.Enqueue(context.Background(), "run_1"); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(context.Background(), "run_2"); !errors.Is(err, ErrFull) {
		t.Fatalf("expected ErrFull, got %v", err)
	}
}

func TestMemoryQueueConsumesAndDeadLetters(t *testing.T) {
	q := NewMemory(1)
	ctx, cancel := context.WithCancel(context.Background())
	processed := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- q.Consume(ctx, 1, func(_ context.Context, runID string) error {
			processed <- runID
			return nil
		})
	}()
	if err := q.Enqueue(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	select {
	case runID := <-processed:
		if runID != "run_1" {
			t.Fatalf("expected run_1, got %s", runID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for run")
	}
	if err := q.DeadLetter(ctx, "run_1", errors.New("failed")); err != nil {
		t.Fatal(err)
	}
	if got := q.DeadLetters(); len(got) != 1 || got[0].RunID != "run_1" {
		t.Fatalf("unexpected dead letters: %+v", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
