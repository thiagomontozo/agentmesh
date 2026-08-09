package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

func TestMemoryAgentLifecycle(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	agent := domain.Agent{ID: "agt_1", Name: "test", CreatedAt: time.Now().UTC()}
	if _, err := memory.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	loaded, err := memory.GetAgent(ctx, agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Name != agent.Name {
		t.Fatalf("expected %q, got %q", agent.Name, loaded.Name)
	}
	agents, err := memory.ListAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(agents); got != 1 {
		t.Fatalf("expected 1 agent, got %d", got)
	}
}

func TestMemoryRunLifecycle(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	run := domain.Run{ID: "run_1", Status: domain.RunQueued, MaxAttempts: 3, CreatedAt: time.Now().UTC()}
	if _, _, err := memory.CreateRun(ctx, run, ""); err != nil {
		t.Fatal(err)
	}

	run.Status = domain.RunRunning
	if err := memory.UpdateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	loaded, err := memory.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != domain.RunRunning {
		t.Fatalf("expected %q, got %q", domain.RunRunning, loaded.Status)
	}
	runs, err := memory.ListRuns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runs); got != 1 {
		t.Fatalf("expected 1 run, got %d", got)
	}
}

func TestMemoryUpdateMissingRun(t *testing.T) {
	err := NewMemory().UpdateRun(context.Background(), domain.Run{ID: "missing"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryCreateRunIsIdempotent(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	first := domain.Run{ID: "run_1", Status: domain.RunQueued, MaxAttempts: 3}
	created, isNew, err := memory.CreateRun(ctx, first, "request-1")
	if err != nil || !isNew || created.ID != first.ID {
		t.Fatalf("unexpected first creation: run=%+v new=%v err=%v", created, isNew, err)
	}
	replayed, isNew, err := memory.CreateRun(ctx, domain.Run{ID: "run_2"}, "request-1")
	if err != nil || isNew || replayed.ID != first.ID {
		t.Fatalf("unexpected replay: run=%+v new=%v err=%v", replayed, isNew, err)
	}
}
