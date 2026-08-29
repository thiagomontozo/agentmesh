package coordination

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryLeaseRenewalExtendsOwnership(t *testing.T) {
	coordinator := NewMemory()
	lease, acquired, err := coordinator.Acquire(context.Background(), "run:1", 60*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("acquire lease: acquired=%v err=%v", acquired, err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := lease.Renew(context.Background(), 100*time.Millisecond); err != nil {
		t.Fatalf("renew lease: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, acquired, err := coordinator.Acquire(context.Background(), "run:1", time.Second); err != nil || acquired {
		t.Fatalf("renewed lease was not retained: acquired=%v err=%v", acquired, err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, acquired, err := coordinator.Acquire(context.Background(), "run:1", time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire after release: acquired=%v err=%v", acquired, err)
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryLeaseRejectsRenewalAfterOwnershipChanges(t *testing.T) {
	coordinator := NewMemory()
	stale, acquired, err := coordinator.Acquire(context.Background(), "run:1", 20*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("acquire stale lease: acquired=%v err=%v", acquired, err)
	}
	time.Sleep(40 * time.Millisecond)
	owner, acquired, err := coordinator.Acquire(context.Background(), "run:1", time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire replacement lease: acquired=%v err=%v", acquired, err)
	}
	if err := stale.Renew(context.Background(), time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost, got %v", err)
	}
	if err := stale.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := coordinator.Acquire(context.Background(), "run:1", time.Second); err != nil || acquired {
		t.Fatalf("stale release removed current owner: acquired=%v err=%v", acquired, err)
	}
	if err := owner.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryLeaseValidatesTTLAndContext(t *testing.T) {
	coordinator := NewMemory()
	if _, _, err := coordinator.Acquire(context.Background(), "run:1", 0); !errors.Is(err, ErrInvalidLeaseTTL) {
		t.Fatalf("expected invalid TTL error, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := coordinator.Acquire(ctx, "run:1", time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}
