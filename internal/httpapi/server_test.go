package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
