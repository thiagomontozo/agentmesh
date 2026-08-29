package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/agenthealth"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

func TestSearchFiltersConfigurationAndDerivedHealth(t *testing.T) {
	repository := store.NewMemory()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	for _, agent := range []domain.Agent{
		{ID: "agt_a", Name: "unhealthy", Runtime: "remote", Protocol: "http", Endpoint: "http://a", Capabilities: []string{"legal-analysis"}, CreatedAt: now},
		{ID: "agt_b", Name: "healthy", Runtime: "remote", Protocol: "http", Endpoint: "http://b", Capabilities: []string{"legal-analysis", "summarization"}, CreatedAt: now},
		{ID: "agt_c", Name: "code", Capabilities: []string{"code-review"}, CreatedAt: now},
	} {
		if _, err := repository.CreateAgent(context.Background(), agent); err != nil {
			t.Fatal(err)
		}
	}
	service := New(repository, healthMap{
		"agt_a": agenthealth.StatusUnhealthy,
		"agt_b": agenthealth.StatusHealthy,
	})
	result, err := service.Search(context.Background(), Query{
		Capability: "LEGAL_ANALYSIS", Runtime: "REMOTE", Protocol: "HTTP", Health: agenthealth.StatusHealthy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || len(result.Items) != 1 || result.Items[0].ID != "agt_b" {
		t.Fatalf("unexpected discovery result: %+v", result)
	}
}

func TestSearchPaginationIsDeterministic(t *testing.T) {
	repository := store.NewMemory()
	now := time.Now().UTC()
	for _, id := range []string{"agt_c", "agt_a", "agt_b"} {
		if _, err := repository.CreateAgent(context.Background(), domain.Agent{ID: id, Name: id, CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := New(repository, nil).Search(context.Background(), Query{Limit: 1, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 3 || len(result.Items) != 1 || result.Items[0].ID != "agt_b" {
		t.Fatalf("unexpected deterministic page: %+v", result)
	}
}

func TestSearchRejectsInvalidFilters(t *testing.T) {
	service := New(store.NewMemory(), nil)
	for _, query := range []Query{
		{Capability: "legal/analysis"}, {Runtime: "remote http"}, {Health: "offline"},
		{Limit: MaxPageSize + 1}, {Offset: -1},
	} {
		if _, err := service.Search(context.Background(), query); err == nil {
			t.Fatalf("expected invalid query rejection: %+v", query)
		}
	}
}

type healthMap map[string]agenthealth.Status

func (h healthMap) State(agentID string) agenthealth.State {
	status := h[agentID]
	if status == "" {
		status = agenthealth.StatusUnknown
	}
	return agenthealth.State{AgentID: agentID, Status: status}
}
func (healthMap) Refresh(domain.Agent) {}
func (healthMap) Forget(string)        {}
