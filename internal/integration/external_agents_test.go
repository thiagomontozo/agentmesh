package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/coordination"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/httpapi"
	protocolv1 "github.com/thiagomontozo/agentmesh/internal/protocol/v1"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

func TestTwoExternalAgentsUseTheSameProtocolAndRuntime(t *testing.T) {
	legalEndpoint := newProtocolAgent(t, "legal analysis complete")
	defer legalEndpoint.Close()
	codeEndpoint := newProtocolAgent(t, "code review complete")
	defer codeEndpoint.Close()

	repository := store.NewMemory()
	runQueue := queue.NewMemory(8)
	bus := events.NewBus()
	resolver := agentruntime.NewRegistry(agentruntime.AdaptLegacy(engine.DemoExecutor{}))
	if err := resolver.Register(
		agentruntime.RemoteRuntime,
		agentruntime.NewHTTPRuntime(&http.Client{Timeout: time.Second}, 0),
	); err != nil {
		t.Fatal(err)
	}
	runEngine := engine.NewWithResolver(
		repository,
		bus,
		resolver,
		runQueue,
		coordination.NewMemory(),
		2,
		engine.RetryPolicy{
			MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, LeaseTTL: time.Minute,
		},
	)
	engineContext, stopEngine := context.WithCancel(context.Background())
	runEngine.Start(engineContext)
	defer func() {
		stopEngine()
		runEngine.Stop()
	}()

	apiHandler := httpapi.New(repository, runEngine, bus).Handler()

	legalAgent := createRemoteAgent(t, apiHandler, agentDefinition{
		Name: "legal-agent", Endpoint: legalEndpoint.URL(),
		Capabilities: []string{"legal-search", "legal-analysis", "summarization"},
	})
	codeAgent := createRemoteAgent(t, apiHandler, agentDefinition{
		Name: "code-agent", Endpoint: codeEndpoint.URL(),
		Capabilities: []string{"code-review", "testing", "debugging"},
	})
	if legalAgent.ID == codeAgent.ID {
		t.Fatalf("expected independent Agent IDs, got %q", legalAgent.ID)
	}

	legalRun := createRemoteRun(t, apiHandler, legalAgent.ID, "Analyze this legal case")
	completedLegalRun := waitForRemoteRun(t, repository, legalRun.ID)
	if completedLegalRun.Output != "legal analysis complete" {
		t.Fatalf("unexpected legal output: %+v", completedLegalRun)
	}
	if legalEndpoint.Calls() != 1 || codeEndpoint.Calls() != 0 {
		t.Fatalf("legal run reached wrong endpoints: legal=%d code=%d", legalEndpoint.Calls(), codeEndpoint.Calls())
	}
	assertProtocolRequest(t, legalEndpoint.SingleRequest(t), legalRun.ID, legalAgent.ID, "Analyze this legal case")

	codeRun := createRemoteRun(t, apiHandler, codeAgent.ID, "Review this code")
	completedCodeRun := waitForRemoteRun(t, repository, codeRun.ID)
	if completedCodeRun.Output != "code review complete" {
		t.Fatalf("unexpected code output: %+v", completedCodeRun)
	}
	if legalEndpoint.Calls() != 1 || codeEndpoint.Calls() != 1 {
		t.Fatalf("code run reached wrong endpoints: legal=%d code=%d", legalEndpoint.Calls(), codeEndpoint.Calls())
	}
	assertProtocolRequest(t, codeEndpoint.SingleRequest(t), codeRun.ID, codeAgent.ID, "Review this code")
}

type agentDefinition struct {
	Name         string
	Endpoint     string
	Capabilities []string
}

func createRemoteAgent(t *testing.T, handler http.Handler, definition agentDefinition) domain.Agent {
	t.Helper()
	payload := map[string]any{
		"name":         definition.Name,
		"runtime":      agentruntime.RemoteRuntime,
		"protocol":     agentruntime.HTTPProtocol,
		"endpoint":     definition.Endpoint,
		"capabilities": definition.Capabilities,
	}
	var agent domain.Agent
	postJSON(t, handler, "/api/v1/agents", payload, http.StatusCreated, &agent)
	if agent.Name != definition.Name || agent.Runtime != agentruntime.RemoteRuntime || agent.Protocol != agentruntime.HTTPProtocol ||
		agent.Endpoint != definition.Endpoint || !slices.Equal(agent.Capabilities, definition.Capabilities) {
		t.Fatalf("Agent registration changed execution metadata: got=%+v want=%+v", agent, definition)
	}
	return agent
}

func createRemoteRun(t *testing.T, handler http.Handler, agentID, input string) domain.Run {
	t.Helper()
	var run domain.Run
	postJSON(t, handler, "/api/v1/runs", map[string]string{
		"agent_id": agentID,
		"input":    input,
	}, http.StatusAccepted, &run)
	return run
}

func postJSON(t *testing.T, handler http.Handler, path string, payload any, expectedStatus int, target any) {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != expectedStatus {
		t.Fatalf("POST %s returned %d, expected %d: %s", path, response.Code, expectedStatus, response.Body.String())
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}

func waitForRemoteRun(t *testing.T, repository store.Repository, runID string) domain.Run {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		run, err := repository.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == domain.RunSucceeded {
			return run
		}
		if run.Status == domain.RunFailed {
			t.Fatalf("remote run failed: %+v", run)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for run %s", runID)
	return domain.Run{}
}

func assertProtocolRequest(t *testing.T, request protocolv1.RunRequest, runID, agentID, input string) {
	t.Helper()
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	if request.ProtocolVersion != protocolv1.Version || request.RunID != runID || request.AgentID != agentID ||
		request.Attempt != 1 || request.IdempotencyKey != protocolv1.AttemptIdempotencyKey(runID, 1) || request.Input != input {
		t.Fatalf("unexpected Agent Protocol V1 request: %+v", request)
	}
}

type protocolAgent struct {
	server   *httptest.Server
	calls    atomic.Int32
	mu       sync.Mutex
	requests []protocolv1.RunRequest
}

func newProtocolAgent(t *testing.T, output string) *protocolAgent {
	t.Helper()
	agent := &protocolAgent{}
	agent.server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/runs" {
			t.Errorf("unexpected Agent request: %s %s", request.Method, request.URL.Path)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		var protocolRequest protocolv1.RunRequest
		if err := json.NewDecoder(request.Body).Decode(&protocolRequest); err != nil {
			t.Errorf("decode Agent Protocol request: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := protocolRequest.Validate(); err != nil {
			t.Errorf("invalid Agent Protocol request: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		agent.mu.Lock()
		agent.requests = append(agent.requests, protocolRequest)
		agent.mu.Unlock()
		agent.calls.Add(1)

		response.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(response).Encode(protocolv1.RunResponse{
			ProtocolVersion: protocolv1.Version,
			RunID:           protocolRequest.RunID,
			Status:          protocolv1.StatusSucceeded,
			Output:          output,
		}); err != nil {
			t.Errorf("encode Agent Protocol response: %v", err)
		}
	}))
	return agent
}

func (a *protocolAgent) URL() string {
	return a.server.URL
}

func (a *protocolAgent) Close() {
	a.server.Close()
}

func (a *protocolAgent) Calls() int {
	return int(a.calls.Load())
}

func (a *protocolAgent) SingleRequest(t *testing.T) protocolv1.RunRequest {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.requests) != 1 {
		t.Fatalf("expected one protocol request, got %d: %+v", len(a.requests), a.requests)
	}
	return a.requests[0]
}
