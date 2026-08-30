package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/metrics"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

func TestRegistryExportsBoundedOperationalMetrics(t *testing.T) {
	repository := store.NewMemory()
	_, _ = repository.CreateAgent(context.Background(), domain.Agent{ID: "agt_1", Name: "test"})
	_, _, _ = repository.CreateRun(context.Background(), domain.Run{
		ID: "run_queued", AgentID: "agt_1", Status: domain.RunQueued, CreatedAt: time.Now().UTC(),
	}, "")
	registry := metrics.New()
	broker := metrics.WrapBroker(events.NewBus(), registry)
	broker.Publish(domain.RunEvent{RunID: "run_queued", Type: "run.started"})
	broker.Publish(domain.RunEvent{RunID: "run_queued", Type: "unbounded.user.value"})
	registry.ObserveRouting("healthy-load-priority-created-at-id")

	handler := registry.HTTPMiddleware(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusAccepted)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/v1/runs", nil))
	var output strings.Builder
	if err := registry.WritePrometheus(context.Background(), &output, repository); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`agentmesh_queue_depth 1`,
		`agentmesh_http_requests_total{method="POST",status="202"} 1`,
		`agentmesh_run_events_total{type="run.started"} 1`,
		`agentmesh_routing_decisions_total{strategy="healthy-load-priority-created-at-id"} 1`,
	} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("missing metric %q:\n%s", expected, output.String())
		}
	}
	if strings.Contains(output.String(), "unbounded.user.value") {
		t.Fatal("unknown event created an unbounded metric label")
	}
}
