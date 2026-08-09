package store

import (
	"errors"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

func TestMemoryAgentLifecycle(t *testing.T) {
	memory := NewMemory()
	agent := domain.Agent{ID: "agt_1", Name: "test", CreatedAt: time.Now().UTC()}
	memory.CreateAgent(agent)

	loaded, err := memory.GetAgent(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Name != agent.Name {
		t.Fatalf("expected %q, got %q", agent.Name, loaded.Name)
	}
	if got := len(memory.ListAgents()); got != 1 {
		t.Fatalf("expected 1 agent, got %d", got)
	}
}

func TestMemoryRunLifecycle(t *testing.T) {
	memory := NewMemory()
	run := domain.Run{ID: "run_1", Status: domain.RunQueued, CreatedAt: time.Now().UTC()}
	memory.CreateRun(run)

	run.Status = domain.RunRunning
	if err := memory.UpdateRun(run); err != nil {
		t.Fatal(err)
	}
	loaded, err := memory.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != domain.RunRunning {
		t.Fatalf("expected %q, got %q", domain.RunRunning, loaded.Status)
	}
	if got := len(memory.ListRuns()); got != 1 {
		t.Fatalf("expected 1 run, got %d", got)
	}
}

func TestMemoryUpdateMissingRun(t *testing.T) {
	err := NewMemory().UpdateRun(domain.Run{ID: "missing"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
