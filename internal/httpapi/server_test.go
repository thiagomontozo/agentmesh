package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/coordination"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

func newTestServer(t *testing.T) (*Server, context.CancelFunc) {
	t.Helper()
	s := store.NewMemory()
	bus := events.NewBus()
	q := queue.NewMemory(8)
	e := engine.New(s, bus, engine.DemoExecutor{Delay: time.Millisecond}, q, coordination.NewMemory(), 1, engine.RetryPolicy{
		MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	e.Start(ctx)
	t.Cleanup(func() {
		cancel()
		e.Stop()
	})
	return New(s, e, bus), cancel
}

func TestHealth(t *testing.T) {
	server, _ := newTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
}

func TestCreateAgentAndRun(t *testing.T) {
	server, _ := newTestServer(t)

	agentBody := bytes.NewBufferString(`{"name":"Researcher","system_prompt":"Be concise"}`)
	agentRequest := httptest.NewRequest(http.MethodPost, "/api/v1/agents", agentBody)
	agentRequest.Header.Set("Content-Type", "application/json")
	agentResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(agentResponse, agentRequest)

	if agentResponse.Code != http.StatusCreated {
		t.Fatalf("expected agent status 201, got %d: %s", agentResponse.Code, agentResponse.Body.String())
	}

	var agent map[string]any
	if err := json.Unmarshal(agentResponse.Body.Bytes(), &agent); err != nil {
		t.Fatal(err)
	}
	agentID, _ := agent["id"].(string)
	if agentID == "" {
		t.Fatal("expected agent id")
	}

	runPayload, _ := json.Marshal(map[string]string{"agent_id": agentID, "input": "hello"})
	runRequest := httptest.NewRequest(http.MethodPost, "/api/v1/runs", bytes.NewReader(runPayload))
	runRequest.Header.Set("Content-Type", "application/json")
	runResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(runResponse, runRequest)

	if runResponse.Code != http.StatusAccepted {
		t.Fatalf("expected run status 202, got %d: %s", runResponse.Code, runResponse.Body.String())
	}
}

func TestRunEventsReplaysLifecycle(t *testing.T) {
	server, _ := newTestServer(t)
	if _, err := server.store.CreateAgent(context.Background(), domain.Agent{ID: "agt_1", Name: "test"}); err != nil {
		t.Fatal(err)
	}

	runRequest := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"agent_id":"agt_1","input":"hello"}`))
	runResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(runResponse, runRequest)
	if runResponse.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", runResponse.Code, runResponse.Body.String())
	}

	var run domain.Run
	if err := json.Unmarshal(runResponse.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, server.store, run.ID, domain.RunSucceeded)

	eventsRequest := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+run.ID+"/events", nil)
	eventsResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(eventsResponse, eventsRequest)
	if eventsResponse.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", eventsResponse.Code)
	}
	body := eventsResponse.Body.String()
	for _, eventType := range []string{"run.queued", "run.started", "run.succeeded"} {
		if !strings.Contains(body, "event: "+eventType) {
			t.Fatalf("expected %s event in %q", eventType, body)
		}
	}
}

func TestQueueFullPublishesFailedEvent(t *testing.T) {
	memory := store.NewMemory()
	bus := events.NewBus()
	memoryQueue := queue.NewMemory(1)
	runEngine := engine.New(memory, bus, engine.DemoExecutor{}, memoryQueue, coordination.NewMemory(), 1, engine.RetryPolicy{
		MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute,
	})
	server := New(memory, runEngine, bus)
	if _, err := memory.CreateAgent(context.Background(), domain.Agent{ID: "agt_1", Name: "test"}); err != nil {
		t.Fatal(err)
	}

	for i, wantStatus := range []int{http.StatusAccepted, http.StatusServiceUnavailable} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"agent_id":"agt_1","input":"hello"}`))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != wantStatus {
			t.Fatalf("request %d: expected %d, got %d: %s", i+1, wantStatus, response.Code, response.Body.String())
		}
	}

	runs, err := memory.ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var failed domain.Run
	for _, run := range runs {
		if run.Status == domain.RunFailed {
			failed = run
			break
		}
	}
	if failed.ID == "" {
		t.Fatalf("expected one failed run, got %+v", runs)
	}
	eventCh, unsubscribe := bus.Subscribe(failed.ID)
	defer unsubscribe()
	for _, want := range []string{"run.queued", "run.failed"} {
		select {
		case event := <-eventCh:
			if event.Type != want {
				t.Fatalf("expected %s, got %s", want, event.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", want)
		}
	}
}

func TestCreateRunIsIdempotent(t *testing.T) {
	server, _ := newTestServer(t)
	if _, err := server.store.CreateAgent(context.Background(), domain.Agent{ID: "agt_1", Name: "test"}); err != nil {
		t.Fatal(err)
	}
	var first domain.Run
	for attempt := 0; attempt < 2; attempt++ {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"agent_id":"agt_1","input":"hello"}`))
		request.Header.Set("Idempotency-Key", "request-1")
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		wantStatus := http.StatusAccepted
		if attempt == 1 {
			wantStatus = http.StatusOK
			if response.Header().Get("Idempotency-Replayed") != "true" {
				t.Fatal("expected replay header")
			}
		}
		if response.Code != wantStatus {
			t.Fatalf("attempt %d: expected %d, got %d: %s", attempt+1, wantStatus, response.Code, response.Body.String())
		}
		var run domain.Run
		if err := json.Unmarshal(response.Body.Bytes(), &run); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			first = run
		} else if run.ID != first.ID {
			t.Fatalf("expected run %s, got %s", first.ID, run.ID)
		}
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"agent_id":"agt_1","input":"different"}`))
	request.Header.Set("Idempotency-Key", "request-1")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("expected 409 for conflicting replay, got %d: %s", response.Code, response.Body.String())
	}
}

func TestRejectsMultipleJSONObjects(t *testing.T) {
	server, _ := newTestServer(t)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strings.NewReader(`{"name":"one"}{"name":"two"}`))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", response.Code, response.Body.String())
	}
}

func waitForRunStatus(t *testing.T, repository store.Repository, runID string, want domain.RunStatus) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		run, err := repository.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for run status %s", want)
}
