package store

import (
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
