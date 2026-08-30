package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/agenthealth"
	"github.com/thiagomontozo/agentmesh/internal/apiauth"
	"github.com/thiagomontozo/agentmesh/internal/coordination"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
	"github.com/thiagomontozo/agentmesh/internal/store"
	workflowengine "github.com/thiagomontozo/agentmesh/internal/workflow"
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

func TestInboundAuthenticationRBACAndAuditLog(t *testing.T) {
	server, _ := newTestServer(t)
	secrets := map[string]string{"READER": "read-token", "OPERATOR": "operate-token", "ADMIN": "admin-token"}
	authenticator, err := apiauth.New(`{
		"reader":{"secret_env":"READER","roles":["reader"]},
		"operator":{"secret_env":"OPERATOR","roles":["operator"]},
		"admin":{"secret_env":"ADMIN","roles":["admin"]}
	}`, func(name string) (string, bool) { value, ok := secrets[name]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	server.SetAPISecurity(authenticator, time.Hour, 100)
	handler := server.Handler()
	request := func(method, path, token, body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response
	}

	if got := request(http.MethodGet, "/api/v1/agents", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("expected authentication challenge, got %d", got.Code)
	}
	if got := request(http.MethodPost, "/api/v1/agents", "read-token", `{"name":"denied"}`); got.Code != http.StatusForbidden {
		t.Fatalf("expected reader mutation rejection, got %d", got.Code)
	}
	created := request(http.MethodPost, "/api/v1/agents", "operate-token", `{"name":"secured"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("operator create failed: %d %s", created.Code, created.Body.String())
	}
	if got := request(http.MethodGet, "/api/v1/audit-events", "read-token", ""); got.Code != http.StatusForbidden {
		t.Fatalf("expected audit log to require admin, got %d", got.Code)
	}
	audit := request(http.MethodGet, "/api/v1/audit-events", "admin-token", "")
	if audit.Code != http.StatusOK || !strings.Contains(audit.Body.String(), `"subject":"operator"`) ||
		!strings.Contains(audit.Body.String(), `"method":"POST"`) {
		t.Fatalf("unexpected audit history: %d %s", audit.Code, audit.Body.String())
	}
}

func TestAuthenticatedAgentIdentityCannotBeSpoofedByHeader(t *testing.T) {
	repository := store.NewMemory()
	for _, id := range []string{"agt_parent", "agt_child", "agt_other"} {
		_, _ = repository.CreateAgent(context.Background(), domain.Agent{ID: id, Name: id})
	}
	parent := domain.Run{ID: "run_secured_parent", AgentID: "agt_parent", Status: domain.RunRunning, CreatedAt: time.Now().UTC()}
	_, _, _ = repository.CreateRun(context.Background(), parent, "")
	bus := events.NewBus()
	runEngine := engine.New(repository, bus, engine.DemoExecutor{}, queue.NewMemory(2), coordination.NewMemory(), 1, engine.RetryPolicy{MaxAttempts: 1})
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	defer func() { cancel(); runEngine.Stop() }()
	server := New(repository, runEngine, bus)
	authenticator, err := apiauth.New(`{
		"parent":{"secret_env":"PARENT","roles":["agent"],"agent_id":"agt_parent"},
		"other":{"secret_env":"OTHER","roles":["agent"],"agent_id":"agt_other"}
	}`, func(name string) (string, bool) {
		return map[string]string{"PARENT": "parent-token", "OTHER": "other-token"}[name], true
	})
	if err != nil {
		t.Fatal(err)
	}
	server.SetAPISecurity(authenticator, time.Hour, 100)
	call := func(token, claimed string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+parent.ID+"/children", strings.NewReader(`{"agent_id":"agt_child","input":"task"}`))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-AgentMesh-Caller-Agent-ID", claimed)
		request.Header.Set("Idempotency-Key", token)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		return response
	}
	if response := call("other-token", "agt_parent"); response.Code != http.StatusForbidden {
		t.Fatalf("spoofed caller was accepted: %d %s", response.Code, response.Body.String())
	}
	if response := call("parent-token", "agt_other"); response.Code != http.StatusAccepted {
		t.Fatalf("authenticated owner was rejected: %d %s", response.Code, response.Body.String())
	}
}

func TestWorkflowDefinitionHTTP(t *testing.T) {
	server, _ := newTestServer(t)
	agentIDs := make([]string, 0, 2)
	for _, name := range []string{"extractor", "reviewer"} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/agents", bytes.NewBufferString(`{"name":"`+name+`"}`))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("create Agent: %d %s", response.Code, response.Body.String())
		}
		var agent domain.Agent
		if err := json.Unmarshal(response.Body.Bytes(), &agent); err != nil {
			t.Fatal(err)
		}
		agentIDs = append(agentIDs, agent.ID)
	}
	body := fmt.Sprintf(`{
		"input":"document",
		"steps":[
			{"id":"extract","agent_id":%q,"input_from":["workflow"]},
			{"id":"review","agent_id":%q,"depends_on":["extract"],"input_from":["extract"]}
		]}`, agentIDs[0], agentIDs[1])
	create := httptest.NewRecorder()
	server.Handler().ServeHTTP(create, httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(body)))
	if create.Code != http.StatusCreated {
		t.Fatalf("create Workflow: %d %s", create.Code, create.Body.String())
	}
	var workflow domain.Workflow
	if err := json.Unmarshal(create.Body.Bytes(), &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.Status != domain.WorkflowPending || len(workflow.Steps) != 2 {
		t.Fatalf("unexpected Workflow response: %+v", workflow)
	}
	get := httptest.NewRecorder()
	server.Handler().ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/workflows/"+workflow.ID, nil))
	if get.Code != http.StatusOK {
		t.Fatalf("get Workflow: %d %s", get.Code, get.Body.String())
	}
	list := httptest.NewRecorder()
	server.Handler().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/v1/workflows", nil))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), workflow.ID) {
		t.Fatalf("list Workflows: %d %s", list.Code, list.Body.String())
	}
}

func TestWorkflowDefinitionHTTPRejectsInvalidDAGAndUnknownAgent(t *testing.T) {
	server, _ := newTestServer(t)
	for _, test := range []struct {
		body string
		want int
	}{
		{body: `{"steps":[{"id":"a","agent_id":"missing","input":"x","depends_on":["b"]},{"id":"b","agent_id":"missing","input":"x","depends_on":["a"]}]}`, want: http.StatusBadRequest},
		{body: `{"steps":[{"id":"a","agent_id":"missing","input":"x"}]}`, want: http.StatusNotFound},
	} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(test.body)))
		if response.Code != test.want {
			t.Fatalf("expected %d, got %d: %s", test.want, response.Code, response.Body.String())
		}
	}
}

func TestSequentialWorkflowStartHTTP(t *testing.T) {
	repository := store.NewMemory()
	for _, id := range []string{"agt_a", "agt_b"} {
		if _, err := repository.CreateAgent(context.Background(), domain.Agent{ID: id, Name: id}); err != nil {
			t.Fatal(err)
		}
	}
	workflow, err := repository.CreateWorkflow(context.Background(), domain.Workflow{ID: "wf_http", Input: "document", Steps: []domain.WorkflowStep{
		{ID: "a", AgentID: "agt_a", InputFrom: []string{"workflow"}},
		{ID: "b", AgentID: "agt_b", DependsOn: []string{"a"}, InputFrom: []string{"a"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	runEngine := engine.New(repository, events.NewBus(), engine.DemoExecutor{Delay: time.Millisecond}, queue.NewMemory(8), coordination.NewMemory(), 1, engine.RetryPolicy{
		MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	manager := workflowengine.New(repository, runEngine)
	manager.Run(ctx)
	defer func() { cancel(); manager.Stop(); runEngine.Stop() }()
	server := New(repository, runEngine, events.NewBus())
	server.SetWorkflowController(manager)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/workflows/"+workflow.ID+"/start", nil))
	if response.Code != http.StatusAccepted {
		t.Fatalf("start Workflow: %d %s", response.Code, response.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		loaded, err := repository.GetWorkflow(context.Background(), workflow.ID)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Status == domain.WorkflowSucceeded {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Workflow did not complete through HTTP start")
}

func TestAgentToAgentChildRunIsMediatedLimitedAndIdempotent(t *testing.T) {
	repository := store.NewMemory()
	for _, id := range []string{"agt_parent", "agt_child", "agt_other"} {
		_, _ = repository.CreateAgent(context.Background(), domain.Agent{ID: id, Name: id})
	}
	parent := domain.Run{ID: "run_parent", AgentID: "agt_parent", Input: "parent", Status: domain.RunRunning, RequestID: "req_parent", CreatedAt: time.Now().UTC()}
	if _, _, err := repository.CreateRun(context.Background(), parent, ""); err != nil {
		t.Fatal(err)
	}
	bus := events.NewBus()
	runEngine := engine.New(repository, bus, engine.DemoExecutor{Delay: time.Millisecond}, queue.NewMemory(8), coordination.NewMemory(), 1, engine.RetryPolicy{
		MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	defer func() { cancel(); runEngine.Stop() }()
	server := New(repository, runEngine, bus)
	server.SetAgentCallLimits(4, 1)
	parentEvents, unsubscribe := bus.Subscribe(parent.ID)
	defer unsubscribe()
	call := func(caller, key, agentID, input string) *httptest.ResponseRecorder {
		body := fmt.Sprintf(`{"agent_id":%q,"input":%q}`, agentID, input)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+parent.ID+"/children", strings.NewReader(body))
		request.Header.Set("X-AgentMesh-Caller-Agent-ID", caller)
		request.Header.Set("Idempotency-Key", key)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		return response
	}
	forbidden := call("agt_other", "wrong-caller", "agt_child", "task")
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("expected caller rejection, got %d: %s", forbidden.Code, forbidden.Body.String())
	}
	createdResponse := call("agt_parent", "call-1", "agt_child", "task")
	if createdResponse.Code != http.StatusAccepted {
		t.Fatalf("create Agent call: %d %s", createdResponse.Code, createdResponse.Body.String())
	}
	var child domain.Run
	if err := json.Unmarshal(createdResponse.Body.Bytes(), &child); err != nil {
		t.Fatal(err)
	}
	if child.ParentRunID != parent.ID || child.RootRunID != parent.ID || child.RequestID != parent.RequestID {
		t.Fatalf("Agent call lost lineage/correlation: %+v", child)
	}
	replay := call("agt_parent", "call-1", "agt_child", "task")
	if replay.Code != http.StatusOK || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("Agent call replay failed: %d %s", replay.Code, replay.Body.String())
	}
	limited := call("agt_parent", "call-2", "agt_other", "other")
	if limited.Code != http.StatusTooManyRequests {
		t.Fatalf("expected atomic fan-out limit, got %d: %s", limited.Code, limited.Body.String())
	}
	var sawAudit bool
	deadline := time.After(time.Second)
	for !sawAudit {
		select {
		case event := <-parentEvents:
			sawAudit = event.Type == "run.agent_call_queued" && event.ChildRunID == child.ID
		case <-deadline:
			t.Fatal("parent Run did not receive Agent-call audit event")
		}
	}
}

func TestAgentToAgentRejectsDepthAndRepeatedAgent(t *testing.T) {
	repository := store.NewMemory()
	for _, id := range []string{"agt_a", "agt_b", "agt_c"} {
		_, _ = repository.CreateAgent(context.Background(), domain.Agent{ID: id, Name: id})
	}
	root := domain.Run{ID: "run_root", AgentID: "agt_a", Status: domain.RunSucceeded, CreatedAt: time.Now().UTC()}
	if _, _, err := repository.CreateRun(context.Background(), root, ""); err != nil {
		t.Fatal(err)
	}
	parent := domain.Run{ID: "run_parent", AgentID: "agt_b", ParentRunID: root.ID, Status: domain.RunRunning, CreatedAt: time.Now().UTC()}
	if _, _, err := repository.CreateRun(context.Background(), parent, ""); err != nil {
		t.Fatal(err)
	}
	bus := events.NewBus()
	runEngine := engine.New(repository, bus, engine.DemoExecutor{}, queue.NewMemory(4), coordination.NewMemory(), 1, engine.RetryPolicy{MaxAttempts: 1, LeaseTTL: time.Minute})
	server := New(repository, runEngine, bus)
	request := func(target string, maxDepth int) *httptest.ResponseRecorder {
		server.SetAgentCallLimits(maxDepth, 4)
		r := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+parent.ID+"/children", strings.NewReader(fmt.Sprintf(`{"agent_id":%q,"input":"task"}`, target)))
		r.Header.Set("X-AgentMesh-Caller-Agent-ID", parent.AgentID)
		r.Header.Set("Idempotency-Key", "key-"+target+strconv.Itoa(maxDepth))
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, r)
		return w
	}
	if response := request("agt_c", 1); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected depth rejection, got %d: %s", response.Code, response.Body.String())
	}
	if response := request("agt_a", 4); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected repeated Agent rejection, got %d: %s", response.Code, response.Body.String())
	}
}

func TestRequestIDIsPreservedOrGenerated(t *testing.T) {
	server, _ := newTestServer(t)
	for _, test := range []struct {
		name     string
		provided string
		want     string
	}{
		{name: "preserved", provided: "request-client_1", want: "request-client_1"},
		{name: "generated when absent", want: "req_"},
		{name: "generated when unsafe", provided: "unsafe request", want: "req_"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			request.Header.Set("X-Request-ID", test.provided)
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			got := response.Header().Get("X-Request-ID")
			if test.want == "req_" && !strings.HasPrefix(got, test.want) {
				t.Fatalf("expected generated request ID, got %q", got)
			}
			if test.want != "req_" && got != test.want {
				t.Fatalf("expected %q, got %q", test.want, got)
			}
		})
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
	runRequest.Header.Set("X-Request-ID", "request-create-run")
	runResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(runResponse, runRequest)

	if runResponse.Code != http.StatusAccepted {
		t.Fatalf("expected run status 202, got %d: %s", runResponse.Code, runResponse.Body.String())
	}
	var run domain.Run
	if err := json.Unmarshal(runResponse.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	if run.RequestID != "request-create-run" || runResponse.Header().Get("X-Request-ID") != run.RequestID {
		t.Fatalf("request correlation was not persisted: run=%+v header=%q", run, runResponse.Header().Get("X-Request-ID"))
	}
	waitForRunStatus(t, server.store, run.ID, domain.RunSucceeded)
	completed, err := server.store.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.RequestID != run.RequestID || completed.CompletedAt == nil || completed.DurationMS < 0 {
		t.Fatalf("run observability fields were not retained: %+v", completed)
	}
}

func TestRunLogsContainCorrelationFields(t *testing.T) {
	var logs synchronizedBuffer
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(originalLogger)

	server, _ := newTestServer(t)
	if _, err := server.store.CreateAgent(context.Background(), domain.Agent{ID: "agt_logs", Name: "logs"}); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"agent_id":"agt_logs","input":"hello"}`))
	request.Header.Set("X-Request-ID", "request-logs")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	var run domain.Run
	if err := json.Unmarshal(response.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, server.store, run.ID, domain.RunSucceeded)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		output := logs.String()
		if strings.Contains(output, `"msg":"run succeeded"`) {
			for _, field := range []string{
				`"request_id":"request-logs"`, `"instance_id":"local"`, `"worker_id":"memory-1"`,
				`"run_id":"` + run.ID + `"`, `"agent_id":"agt_logs"`, `"attempt":1`, `"duration_ms":`,
			} {
				if !strings.Contains(output, field) {
					t.Fatalf("structured logs do not contain %s: %s", field, output)
				}
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for correlated success log: %s", logs.String())
}

func TestCreateAgentWithExecutionMetadataAndList(t *testing.T) {
	server, _ := newTestServer(t)
	body := `{
		"name":"Legal Agent",
		"system_prompt":"Be precise",
		"runtime":"REMOTE",
		"protocol":"HTTP",
		"endpoint":"http://legal-agent:9000",
		"capabilities":["legal-search","legal-analysis","summarization"],
		"effect_idempotency":"required",
		"max_concurrency":8,
		"priority":50
	}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strings.NewReader(body))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", response.Code, response.Body.String())
	}

	var created domain.Agent
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Runtime != "remote" || created.Protocol != "http" || created.Endpoint != "http://legal-agent:9000" {
		t.Fatalf("unexpected created agent: %+v", created)
	}
	if len(created.Capabilities) != 3 || created.Capabilities[0] != "legal-search" {
		t.Fatalf("unexpected capabilities: %#v", created.Capabilities)
	}
	if created.MaxConcurrency != 8 || created.Priority != 50 {
		t.Fatalf("unexpected routing metadata: %+v", created)
	}
	if created.EffectIdempotency != domain.EffectIdempotencyRequired {
		t.Fatalf("unexpected effect idempotency policy: %+v", created)
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/api/v1/agents/"+created.ID, nil)
	getResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(getResponse, getRequest)
	if getResponse.Code != http.StatusOK || !strings.Contains(getResponse.Body.String(), `"runtime":"remote"`) ||
		!strings.Contains(getResponse.Body.String(), `"effect_idempotency":"required"`) {
		t.Fatalf("unexpected get response: %d %s", getResponse.Code, getResponse.Body.String())
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	listResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK || !strings.Contains(listResponse.Body.String(), `"capabilities":["legal-search","legal-analysis","summarization"]`) {
		t.Fatalf("unexpected list response: %d %s", listResponse.Code, listResponse.Body.String())
	}

	runBody, _ := json.Marshal(map[string]string{"agent_id": created.ID, "input": "hello"})
	runRequest := httptest.NewRequest(http.MethodPost, "/api/v1/runs", bytes.NewReader(runBody))
	runResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(runResponse, runRequest)
	if runResponse.Code != http.StatusAccepted {
		t.Fatalf("expected run status 202, got %d: %s", runResponse.Code, runResponse.Body.String())
	}
	var run domain.Run
	if err := json.Unmarshal(runResponse.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, server.store, run.ID, domain.RunFailed)
	completed, err := server.store.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(completed.Error, agentruntime.ErrUnknownRuntime.Error()) {
		t.Fatalf("configured unknown runtime did not fail explicitly: %+v", completed)
	}
}

func TestListAgentsFiltersByCanonicalCapability(t *testing.T) {
	server, _ := newTestServer(t)
	for _, body := range []string{
		`{"name":"Legal","capabilities":["Legal Analysis","legal_analysis"]}`,
		`{"name":"Code","capabilities":["code-review"]}`,
	} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strings.NewReader(body))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("create failed: %d %s", response.Code, response.Body.String())
		}
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/agents?capability=LEGAL_ANALYSIS", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"name":"Legal"`) ||
		strings.Contains(response.Body.String(), `"name":"Code"`) ||
		!strings.Contains(response.Body.String(), `"capabilities":["legal-analysis"]`) {
		t.Fatalf("unexpected filtered response: %d %s", response.Code, response.Body.String())
	}

	invalid := httptest.NewRequest(http.MethodGet, "/api/v1/agents?capability=legal%2Fanalysis", nil)
	invalidResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(invalidResponse, invalid)
	if invalidResponse.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid capability rejection, got %d: %s", invalidResponse.Code, invalidResponse.Body.String())
	}
}

func TestAgentDiscoveryFiltersHealthAndPaginates(t *testing.T) {
	server, _ := newTestServer(t)
	health := healthMapRegistry{states: make(map[string]agenthealth.Status)}
	server.SetAgentHealth(health)
	for index, name := range []string{"Legal A", "Legal B", "Demo"} {
		body := `{"name":"` + name + `","capabilities":["legal-analysis"]}`
		if index < 2 {
			body = `{"name":"` + name + `","runtime":"remote","protocol":"http","endpoint":"http://agent-` + strconv.Itoa(index) + `","capabilities":["legal-analysis"]}`
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strings.NewReader(body))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("create failed: %d %s", response.Code, response.Body.String())
		}
		var agent domain.Agent
		if err := json.Unmarshal(response.Body.Bytes(), &agent); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			health.states[agent.ID] = agenthealth.StatusUnhealthy
		} else if index == 1 {
			health.states[agent.ID] = agenthealth.StatusHealthy
		}
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/agents?capability=legal-analysis&runtime=remote&protocol=http&status=healthy&limit=1&offset=0", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"name":"Legal B"`) ||
		!strings.Contains(response.Body.String(), `"total":1`) || strings.Contains(response.Body.String(), `"name":"Legal A"`) {
		t.Fatalf("unexpected discovery response: %d %s", response.Code, response.Body.String())
	}

	invalid := httptest.NewRequest(http.MethodGet, "/api/v1/agents?health=healthy&status=unhealthy", nil)
	invalidResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(invalidResponse, invalid)
	if invalidResponse.Code != http.StatusBadRequest {
		t.Fatalf("expected conflicting aliases to fail: %d %s", invalidResponse.Code, invalidResponse.Body.String())
	}
}

func TestCreateRunRoutesByRequiredCapabilitiesAndKeepsManualSelection(t *testing.T) {
	server, _ := newTestServer(t)
	health := healthMapRegistry{states: make(map[string]agenthealth.Status)}
	server.SetAgentHealth(health)
	agentIDs := make([]string, 0, 2)
	for _, name := range []string{"Older Unhealthy", "Healthy"} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strings.NewReader(
			`{"name":"`+name+`","capabilities":["legal-analysis","summarization"]}`,
		))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("create Agent failed: %d %s", response.Code, response.Body.String())
		}
		var agent domain.Agent
		if err := json.Unmarshal(response.Body.Bytes(), &agent); err != nil {
			t.Fatal(err)
		}
		agentIDs = append(agentIDs, agent.ID)
	}
	health.states[agentIDs[0]] = agenthealth.StatusUnhealthy
	health.states[agentIDs[1]] = agenthealth.StatusHealthy

	routedRequest := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(
		`{"required_capabilities":["Legal Analysis","SUMMARIZATION"],"input":"analyze"}`,
	))
	routedRequest.Header.Set("Idempotency-Key", "routed-request")
	routedResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(routedResponse, routedRequest)
	if routedResponse.Code != http.StatusAccepted {
		t.Fatalf("routed Run failed: %d %s", routedResponse.Code, routedResponse.Body.String())
	}
	var routed domain.Run
	if err := json.Unmarshal(routedResponse.Body.Bytes(), &routed); err != nil {
		t.Fatal(err)
	}
	if routed.AgentID != agentIDs[1] || !slices.Equal(routed.RequiredCapabilities, []string{"legal-analysis", "summarization"}) {
		t.Fatalf("unexpected routed Run: %+v", routed)
	}

	// A health change must not break replay of the same routed request.
	health.states[agentIDs[0]] = agenthealth.StatusHealthy
	health.states[agentIDs[1]] = agenthealth.StatusUnhealthy
	replay := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(
		`{"required_capabilities":["legal-analysis","summarization"],"input":"analyze"}`,
	))
	replay.Header.Set("Idempotency-Key", "routed-request")
	replayResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(replayResponse, replay)
	if replayResponse.Code != http.StatusOK || replayResponse.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("routed replay failed: %d %s", replayResponse.Code, replayResponse.Body.String())
	}
	var replayed domain.Run
	if err := json.Unmarshal(replayResponse.Body.Bytes(), &replayed); err != nil || replayed.ID != routed.ID || replayed.AgentID != routed.AgentID {
		t.Fatalf("replay did not retain original decision: run=%+v err=%v", replayed, err)
	}

	manual := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(
		`{"agent_id":"`+agentIDs[1]+`","input":"manual"}`,
	))
	manualResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(manualResponse, manual)
	if manualResponse.Code != http.StatusAccepted {
		t.Fatalf("manual selection regressed: %d %s", manualResponse.Code, manualResponse.Body.String())
	}

	ambiguous := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(
		`{"agent_id":"`+agentIDs[1]+`","required_capabilities":["testing"],"input":"ambiguous"}`,
	))
	ambiguousResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(ambiguousResponse, ambiguous)
	if ambiguousResponse.Code != http.StatusBadRequest {
		t.Fatalf("expected ambiguous selection rejection: %d %s", ambiguousResponse.Code, ambiguousResponse.Body.String())
	}

	missing := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(
		`{"required_capabilities":["nonexistent"],"input":"missing"}`,
	))
	missingResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingResponse, missing)
	if missingResponse.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected no-candidate 422: %d %s", missingResponse.Code, missingResponse.Body.String())
	}
}

func TestLoadRoutingReturns429ButIdempotentReplayBypassesRerouting(t *testing.T) {
	server, _ := newTestServer(t)
	health := healthMapRegistry{states: make(map[string]agenthealth.Status)}
	server.SetAgentHealth(health)
	agent, err := server.store.CreateAgent(context.Background(), domain.Agent{
		ID: "agt_capacity", Name: "Capacity", Capabilities: []string{"testing"}, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	health.states[agent.ID] = agenthealth.StatusHealthy
	existing := domain.Run{
		ID: "run_existing", AgentID: agent.ID, RequiredCapabilities: []string{"testing"},
		Input: "same", Status: domain.RunQueued, MaxAttempts: 1, CreatedAt: time.Now().UTC(),
	}
	if _, _, err := server.store.CreateRun(context.Background(), existing, "capacity-replay"); err != nil {
		t.Fatal(err)
	}

	replay := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(
		`{"required_capabilities":["TESTING"],"input":"same"}`,
	))
	replay.Header.Set("Idempotency-Key", "capacity-replay")
	replayResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(replayResponse, replay)
	if replayResponse.Code != http.StatusOK || replayResponse.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("saturated idempotent replay failed: %d %s", replayResponse.Code, replayResponse.Body.String())
	}

	newRequest := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(
		`{"required_capabilities":["testing"],"input":"new"}`,
	))
	newResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(newResponse, newRequest)
	if newResponse.Code != http.StatusTooManyRequests {
		t.Fatalf("expected saturated routing 429, got %d: %s", newResponse.Code, newResponse.Body.String())
	}
}

func TestCreateAndQueryParentChildRunsWithLineageEvents(t *testing.T) {
	server, _ := newTestServer(t)
	if _, err := server.store.CreateAgent(context.Background(), domain.Agent{ID: "agt_lineage", Name: "Lineage"}); err != nil {
		t.Fatal(err)
	}
	create := func(body, key string) domain.Run {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(body))
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("create Run failed: %d %s", response.Code, response.Body.String())
		}
		var run domain.Run
		if err := json.Unmarshal(response.Body.Bytes(), &run); err != nil {
			t.Fatal(err)
		}
		return run
	}

	root := create(`{"agent_id":"agt_lineage","input":"root"}`, "")
	child := create(`{"agent_id":"agt_lineage","parent_run_id":"`+root.ID+`","input":"child"}`, "child-key")
	if child.ParentRunID != root.ID || child.RootRunID != root.ID {
		t.Fatalf("unexpected direct child lineage: %+v", child)
	}
	grandchild := create(`{"agent_id":"agt_lineage","parent_run_id":"`+child.ID+`","input":"grandchild"}`, "")
	if grandchild.ParentRunID != child.ID || grandchild.RootRunID != root.ID {
		t.Fatalf("unexpected grandchild lineage: %+v", grandchild)
	}

	childrenRequest := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+root.ID+"/children", nil)
	childrenResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(childrenResponse, childrenRequest)
	if childrenResponse.Code != http.StatusOK || !strings.Contains(childrenResponse.Body.String(), child.ID) ||
		strings.Contains(childrenResponse.Body.String(), grandchild.ID) {
		t.Fatalf("unexpected direct children response: %d %s", childrenResponse.Code, childrenResponse.Body.String())
	}

	rootEvents, unsubscribeRoot := server.events.Subscribe(root.ID)
	defer unsubscribeRoot()
	foundParentEvent := false
	for !foundParentEvent {
		select {
		case event := <-rootEvents:
			foundParentEvent = event.Type == "run.child_queued" && event.ChildRunID == child.ID &&
				event.ParentRunID == root.ID && event.RootRunID == root.ID
		case <-time.After(time.Second):
			t.Fatal("parent stream did not record child Run creation")
		}
	}

	childEvents, unsubscribeChild := server.events.Subscribe(child.ID)
	defer unsubscribeChild()
	foundQueuedLineage := false
	for !foundQueuedLineage {
		select {
		case event := <-childEvents:
			foundQueuedLineage = event.Type == "run.queued" && event.ParentRunID == root.ID && event.RootRunID == root.ID
		case <-time.After(time.Second):
			t.Fatal("child stream did not retain lineage")
		}
	}

	otherRoot := create(`{"agent_id":"agt_lineage","input":"other root"}`, "")
	conflict := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(
		`{"agent_id":"agt_lineage","parent_run_id":"`+otherRoot.ID+`","input":"child"}`,
	))
	conflict.Header.Set("Idempotency-Key", "child-key")
	conflictResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(conflictResponse, conflict)
	if conflictResponse.Code != http.StatusConflict {
		t.Fatalf("expected lineage-aware idempotency conflict: %d %s", conflictResponse.Code, conflictResponse.Body.String())
	}

	missingParent := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(
		`{"agent_id":"agt_lineage","parent_run_id":"run_missing","input":"missing"}`,
	))
	missingParentResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingParentResponse, missingParent)
	if missingParentResponse.Code != http.StatusNotFound {
		t.Fatalf("expected missing parent 404: %d %s", missingParentResponse.Code, missingParentResponse.Body.String())
	}
	missingChildren := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run_missing/children", nil)
	missingChildrenResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingChildrenResponse, missingChildren)
	if missingChildrenResponse.Code != http.StatusNotFound {
		t.Fatalf("expected missing children parent 404: %d %s", missingChildrenResponse.Code, missingChildrenResponse.Body.String())
	}
}

func TestAgentUpdateDeleteAndConcurrencyAPI(t *testing.T) {
	server, _ := newTestServer(t)
	health := &recordingAgentHealth{}
	server.SetAgentHealth(health)
	createRequest := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strings.NewReader(`{"name":"original"}`))
	createResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(createResponse, createRequest)
	if createResponse.Code != http.StatusCreated || createResponse.Header().Get("ETag") != `"1"` {
		t.Fatalf("unexpected create response: %d etag=%s body=%s", createResponse.Code, createResponse.Header().Get("ETag"), createResponse.Body.String())
	}
	var created domain.Agent
	if err := json.Unmarshal(createResponse.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	updateBody := `{
		"name":"updated","system_prompt":"new prompt","runtime":"remote","protocol":"http",
		"endpoint":"http://agent:9000","capabilities":["testing","debugging"],
		"max_concurrency":3,"priority":10
	}`
	updateRequest := httptest.NewRequest(http.MethodPut, "/api/v1/agents/"+created.ID, strings.NewReader(updateBody))
	updateRequest.Header.Set("If-Match", `"1"`)
	updateResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(updateResponse, updateRequest)
	if updateResponse.Code != http.StatusOK || updateResponse.Header().Get("ETag") != `"2"` {
		t.Fatalf("unexpected update response: %d etag=%s body=%s", updateResponse.Code, updateResponse.Header().Get("ETag"), updateResponse.Body.String())
	}
	var updated domain.Agent
	if err := json.Unmarshal(updateResponse.Body.Bytes(), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updated.Name != "updated" || !updated.CreatedAt.Equal(created.CreatedAt) ||
		len(updated.Capabilities) != 2 || updated.MaxConcurrency != 3 || updated.Priority != 10 ||
		updated.UpdatedAt.Before(created.UpdatedAt) {
		t.Fatalf("unexpected updated Agent: %+v", updated)
	}

	staleRequest := httptest.NewRequest(http.MethodPut, "/api/v1/agents/"+created.ID, strings.NewReader(updateBody))
	staleRequest.Header.Set("If-Match", `"1"`)
	staleResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(staleResponse, staleRequest)
	if staleResponse.Code != http.StatusConflict {
		t.Fatalf("expected stale update conflict, got %d: %s", staleResponse.Code, staleResponse.Body.String())
	}

	if _, _, err := server.store.CreateRun(context.Background(), domain.Run{
		ID: "run_history", AgentID: created.ID, Status: domain.RunSucceeded, MaxAttempts: 1,
	}, ""); err != nil {
		t.Fatal(err)
	}
	deleteUsed := httptest.NewRequest(http.MethodDelete, "/api/v1/agents/"+created.ID, nil)
	deleteUsed.Header.Set("If-Match", `"2"`)
	deleteUsedResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(deleteUsedResponse, deleteUsed)
	if deleteUsedResponse.Code != http.StatusConflict || !strings.Contains(deleteUsedResponse.Body.String(), "dependent runs") {
		t.Fatalf("dependent Run history was not protected: %d %s", deleteUsedResponse.Code, deleteUsedResponse.Body.String())
	}

	unused, err := server.store.CreateAgent(context.Background(), domain.Agent{ID: "agt_unused", Name: "unused"})
	if err != nil {
		t.Fatal(err)
	}
	deleteUnused := httptest.NewRequest(http.MethodDelete, "/api/v1/agents/"+unused.ID, nil)
	deleteUnused.Header.Set("If-Match", `"1"`)
	deleteUnusedResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(deleteUnusedResponse, deleteUnused)
	if deleteUnusedResponse.Code != http.StatusNoContent {
		t.Fatalf("expected unused Agent deletion, got %d: %s", deleteUnusedResponse.Code, deleteUnusedResponse.Body.String())
	}
	if health.forgottenAgent() != unused.ID || health.refreshCount() < 2 {
		t.Fatalf("health registry was not synchronized: refreshed=%d forgotten=%q", health.refreshCount(), health.forgottenAgent())
	}
}

func TestAgentMutationRejectsMalformedIfMatch(t *testing.T) {
	server, _ := newTestServer(t)
	agent, err := server.store.CreateAgent(context.Background(), domain.Agent{ID: "agt_1", Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/agents/"+agent.ID, nil)
	request.Header.Set("If-Match", "1")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected malformed If-Match rejection, got %d: %s", response.Code, response.Body.String())
	}
}

func TestAgentHealthEndpointReportsDerivedState(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("unexpected health path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer remote.Close()
	server, _ := newTestServer(t)
	health, err := agenthealth.New(server.store, nil, agenthealth.Config{
		Path: "/healthz", Interval: time.Hour, Timeout: 100 * time.Millisecond, Workers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	health.Start(ctx)
	defer func() {
		cancel()
		health.Stop()
	}()
	server.SetAgentHealth(health)

	create := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strings.NewReader(`{
		"name":"remote","runtime":"remote","protocol":"http","endpoint":"`+remote.URL+`"
	}`))
	createResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(createResponse, create)
	if createResponse.Code != http.StatusCreated || strings.Contains(createResponse.Body.String(), `"health"`) {
		t.Fatalf("health must remain separate from Agent configuration: %d %s", createResponse.Code, createResponse.Body.String())
	}
	var agent domain.Agent
	if err := json.Unmarshal(createResponse.Body.Bytes(), &agent); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		request := httptest.NewRequest(http.MethodGet, "/api/v1/agents/"+agent.ID+"/health", nil)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("unexpected health status %d: %s", response.Code, response.Body.String())
		}
		var state agenthealth.State
		if err := json.Unmarshal(response.Body.Bytes(), &state); err != nil {
			t.Fatal(err)
		}
		if state.Status == agenthealth.StatusHealthy && state.LastCheckedAt != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for derived Agent health")
}

func TestCreateAgentRejectsInvalidExecutionMetadata(t *testing.T) {
	tests := []string{
		`{"name":"invalid","runtime":"remote http"}`,
		`{"name":"invalid","protocol":"http"}`,
		`{"name":"invalid","runtime":"remote","endpoint":"http://agent:9000"}`,
		`{"name":"invalid","runtime":"remote","protocol":"http","endpoint":"/v1/runs"}`,
		`{"name":"invalid","capabilities":["testing",""]}`,
		`{"name":"invalid","max_concurrency":-1}`,
		`{"name":"invalid","priority":1001}`,
	}
	for _, body := range tests {
		server, _ := newTestServer(t)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strings.NewReader(body))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d for %s: %s", response.Code, body, response.Body.String())
		}
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

func TestCancelRunEndpoint(t *testing.T) {
	memory := store.NewMemory()
	bus := events.NewBus()
	runEngine := engine.New(
		memory, bus, engine.DemoExecutor{}, queue.NewMemory(4), coordination.NewMemory(), 1,
		engine.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute},
	)
	server := New(memory, runEngine, bus)
	for _, run := range []domain.Run{
		{ID: "run_queued", Status: domain.RunQueued, MaxAttempts: 3},
		{ID: "run_succeeded", Status: domain.RunSucceeded, MaxAttempts: 3},
	} {
		if _, _, err := memory.CreateRun(context.Background(), run, ""); err != nil {
			t.Fatal(err)
		}
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/runs/run_queued/cancel", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	var canceled domain.Run
	if err := json.Unmarshal(response.Body.Bytes(), &canceled); err != nil {
		t.Fatal(err)
	}
	if canceled.Status != domain.RunCanceled || canceled.CompletedAt == nil {
		t.Fatalf("unexpected canceled response: %+v", canceled)
	}
	assertServerEventTypes(t, bus, canceled.ID, "run.canceled")

	for _, test := range []struct {
		name       string
		runID      string
		wantStatus int
	}{
		{name: "already canceled", runID: "run_queued", wantStatus: http.StatusConflict},
		{name: "succeeded", runID: "run_succeeded", wantStatus: http.StatusConflict},
		{name: "missing", runID: "missing", wantStatus: http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+test.runID+"/cancel", nil)
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("expected %d, got %d: %s", test.wantStatus, response.Code, response.Body.String())
			}
		})
	}
}

func assertServerEventTypes(t *testing.T, bus *events.Bus, runID string, expected ...string) {
	t.Helper()
	eventChannel, unsubscribe := bus.Subscribe(runID)
	defer unsubscribe()
	for _, expectedType := range expected {
		select {
		case event := <-eventChannel:
			if event.Type != expectedType {
				t.Fatalf("expected event %s, got %+v", expectedType, event)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for event %s", expectedType)
		}
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

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

type recordingAgentHealth struct {
	mu        sync.Mutex
	refreshed []domain.Agent
	forgotten string
}

type healthMapRegistry struct {
	states map[string]agenthealth.Status
}

func (h healthMapRegistry) State(agentID string) agenthealth.State {
	status := h.states[agentID]
	if status == "" {
		status = agenthealth.StatusUnknown
	}
	return agenthealth.State{AgentID: agentID, Status: status}
}
func (healthMapRegistry) Refresh(domain.Agent) {}
func (healthMapRegistry) Forget(string)        {}

func (r *recordingAgentHealth) State(agentID string) agenthealth.State {
	return agenthealth.State{AgentID: agentID, Status: agenthealth.StatusUnknown}
}

func (r *recordingAgentHealth) Refresh(agent domain.Agent) {
	r.mu.Lock()
	r.refreshed = append(r.refreshed, agent)
	r.mu.Unlock()
}

func (r *recordingAgentHealth) Forget(agentID string) {
	r.mu.Lock()
	r.forgotten = agentID
	r.mu.Unlock()
}

func (r *recordingAgentHealth) refreshCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.refreshed)
}

func (r *recordingAgentHealth) forgottenAgent() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.forgotten
}

func (b *synchronizedBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(payload)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}
