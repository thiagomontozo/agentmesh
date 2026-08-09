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

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

func newTestServer(t *testing.T) (*Server, context.CancelFunc) {
	t.Helper()
	s := store.NewMemory()
	bus := events.NewBus()
	e := engine.New(s, bus, engine.DemoExecutor{Delay: time.Millisecond}, 1, 8)
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
	server.store.CreateAgent(domain.Agent{ID: "agt_1", Name: "test"})

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
	runEngine := engine.New(memory, bus, engine.DemoExecutor{}, 1, 1)
	server := New(memory, runEngine, bus)
	memory.CreateAgent(domain.Agent{ID: "agt_1", Name: "test"})

	for i, wantStatus := range []int{http.StatusAccepted, http.StatusServiceUnavailable} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"agent_id":"agt_1","input":"hello"}`))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != wantStatus {
			t.Fatalf("request %d: expected %d, got %d: %s", i+1, wantStatus, response.Code, response.Body.String())
		}
	}

	runs := memory.ListRuns()
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

func waitForRunStatus(t *testing.T, repository store.Repository, runID string, want domain.RunStatus) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		run, err := repository.GetRun(runID)
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
