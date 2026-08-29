package router

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/agenthealth"
	"github.com/thiagomontozo/agentmesh/internal/discovery"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

func TestSelectPrefersHealthyAndMatchesEveryCapability(t *testing.T) {
	repository := store.NewMemory()
	now := time.Now().UTC()
	for _, agent := range []domain.Agent{
		{ID: "agt_unknown", Name: "Unknown", Capabilities: []string{"legal-analysis", "summarization"}, CreatedAt: now.Add(-time.Hour)},
		{ID: "agt_partial", Name: "Partial", Capabilities: []string{"legal-analysis"}, CreatedAt: now.Add(-time.Minute)},
		{ID: "agt_healthy", Name: "Healthy", Capabilities: []string{"legal-analysis", "summarization"}, CreatedAt: now},
	} {
		if _, err := repository.CreateAgent(context.Background(), agent); err != nil {
			t.Fatal(err)
		}
	}
	health := routerHealth{
		"agt_partial": agenthealth.StatusHealthy,
		"agt_healthy": agenthealth.StatusHealthy,
	}
	decision, err := New(discovery.New(repository, health)).Select(context.Background(), []string{
		"Legal Analysis", "SUMMARIZATION", "legal_analysis",
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Agent.ID != "agt_healthy" || decision.Health != agenthealth.StatusHealthy ||
		decision.Strategy != "healthy-load-priority-created-at-id" || len(decision.RequiredCapabilities) != 2 {
		t.Fatalf("unexpected routing decision: %+v", decision)
	}
}

func TestSelectIsDeterministicWithinTier(t *testing.T) {
	repository := store.NewMemory()
	now := time.Now().UTC()
	for _, id := range []string{"agt_b", "agt_a"} {
		if _, err := repository.CreateAgent(context.Background(), domain.Agent{
			ID: id, Name: id, Capabilities: []string{"testing"}, CreatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	health := routerHealth{"agt_a": agenthealth.StatusHealthy, "agt_b": agenthealth.StatusHealthy}
	decision, err := New(discovery.New(repository, health)).Select(context.Background(), []string{"testing"})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Agent.ID != "agt_a" || decision.CandidateCount != 2 {
		t.Fatalf("unexpected deterministic selection: %+v", decision)
	}
}

func TestSelectUsesUnknownFallbackAndExcludesUnhealthy(t *testing.T) {
	repository := store.NewMemory()
	for _, agent := range []domain.Agent{
		{ID: "agt_unhealthy", Name: "Unhealthy", Capabilities: []string{"testing"}},
		{ID: "agt_unknown", Name: "Unknown", Capabilities: []string{"testing"}},
	} {
		if _, err := repository.CreateAgent(context.Background(), agent); err != nil {
			t.Fatal(err)
		}
	}
	health := routerHealth{"agt_unhealthy": agenthealth.StatusUnhealthy}
	decision, err := New(discovery.New(repository, health)).Select(context.Background(), []string{"testing"})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Agent.ID != "agt_unknown" || decision.Strategy != "unknown-fallback-load-priority-created-at-id" {
		t.Fatalf("unexpected fallback: %+v", decision)
	}

	health["agt_unknown"] = agenthealth.StatusUnhealthy
	if _, err := New(discovery.New(repository, health)).Select(context.Background(), []string{"testing"}); !errors.Is(err, ErrNoCandidate) {
		t.Fatalf("expected no eligible candidate, got %v", err)
	}
}

func TestSelectPrefersFreeAgent(t *testing.T) {
	repository := store.NewMemory()
	now := time.Now().UTC()
	for _, agent := range []domain.Agent{
		{ID: "agt_busy", Name: "Busy", Capabilities: []string{"testing"}, MaxConcurrency: 2, CreatedAt: now.Add(-time.Hour)},
		{ID: "agt_free", Name: "Free", Capabilities: []string{"testing"}, MaxConcurrency: 2, CreatedAt: now},
	} {
		if _, err := repository.CreateAgent(context.Background(), agent); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := repository.CreateRun(context.Background(), domain.Run{
		ID: "run_busy", AgentID: "agt_busy", Status: domain.RunRunning, MaxAttempts: 1,
	}, ""); err != nil {
		t.Fatal(err)
	}
	health := routerHealth{"agt_busy": agenthealth.StatusHealthy, "agt_free": agenthealth.StatusHealthy}
	decision, err := NewWithLoad(discovery.New(repository, health), repository).Select(context.Background(), []string{"testing"})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Agent.ID != "agt_free" || decision.ActiveRuns != 0 || decision.EffectiveCapacity != 2 {
		t.Fatalf("expected free Agent, got %+v", decision)
	}
}

func TestSelectUsesNormalizedLoadThenPriority(t *testing.T) {
	repository := store.NewMemory()
	now := time.Now().UTC()
	for _, agent := range []domain.Agent{
		{ID: "agt_half", Name: "Half", Capabilities: []string{"testing"}, MaxConcurrency: 2, Priority: 100, CreatedAt: now},
		{ID: "agt_quarter", Name: "Quarter", Capabilities: []string{"testing"}, MaxConcurrency: 4, CreatedAt: now},
	} {
		if _, err := repository.CreateAgent(context.Background(), agent); err != nil {
			t.Fatal(err)
		}
	}
	for index, agentID := range []string{"agt_half", "agt_quarter"} {
		if _, _, err := repository.CreateRun(context.Background(), domain.Run{
			ID: fmt.Sprintf("run_%d", index), AgentID: agentID, Status: domain.RunQueued, MaxAttempts: 1,
		}, ""); err != nil {
			t.Fatal(err)
		}
	}
	health := routerHealth{"agt_half": agenthealth.StatusHealthy, "agt_quarter": agenthealth.StatusHealthy}
	router := NewWithLoad(discovery.New(repository, health), repository)
	decision, err := router.Select(context.Background(), []string{"testing"})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Agent.ID != "agt_quarter" {
		t.Fatalf("lower utilization must win before priority: %+v", decision)
	}

	for _, runID := range []string{"run_0", "run_1"} {
		completed, err := repository.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		completed.Status = domain.RunSucceeded
		if err := repository.UpdateRun(context.Background(), completed); err != nil {
			t.Fatal(err)
		}
	}
	decision, err = router.Select(context.Background(), []string{"testing"})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Agent.ID != "agt_half" || decision.Priority != 100 {
		t.Fatalf("priority must break equal-load ties: %+v", decision)
	}
}

func TestSelectReturnsCapacityErrorWhenAllMatchesAreSaturated(t *testing.T) {
	repository := store.NewMemory()
	if _, err := repository.CreateAgent(context.Background(), domain.Agent{
		ID: "agt_full", Name: "Full", Capabilities: []string{"testing"}, MaxConcurrency: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.CreateRun(context.Background(), domain.Run{
		ID: "run_full", AgentID: "agt_full", Status: domain.RunQueued, MaxAttempts: 1,
	}, ""); err != nil {
		t.Fatal(err)
	}
	health := routerHealth{"agt_full": agenthealth.StatusHealthy}
	_, err := NewWithLoad(discovery.New(repository, health), repository).Select(context.Background(), []string{"testing"})
	if !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("expected capacity error, got %v", err)
	}
}

func TestSelectRejectsInvalidRequirements(t *testing.T) {
	router := New(discovery.New(store.NewMemory(), nil))
	for _, required := range [][]string{nil, {"legal/analysis"}, {" "}} {
		if _, err := router.Select(context.Background(), required); !errors.Is(err, ErrInvalidRequirements) {
			t.Fatalf("expected invalid requirements for %#v, got %v", required, err)
		}
	}
}

type routerHealth map[string]agenthealth.Status

func (h routerHealth) State(agentID string) agenthealth.State {
	status := h[agentID]
	if status == "" {
		status = agenthealth.StatusUnknown
	}
	return agenthealth.State{AgentID: agentID, Status: status}
}
func (routerHealth) Refresh(domain.Agent) {}
func (routerHealth) Forget(string)        {}
