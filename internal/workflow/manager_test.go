package workflow

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/coordination"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

type executorFunc func(context.Context, domain.Agent, string) (string, error)

func (f executorFunc) Execute(ctx context.Context, agent domain.Agent, input string) (string, error) {
	return f(ctx, agent, input)
}

func TestSequentialWorkflowPropagatesOutputsThroughRuns(t *testing.T) {
	repository, runEngine, manager, stop := newWorkflowHarness(t, executorFunc(func(_ context.Context, agent domain.Agent, input string) (string, error) {
		return agent.ID + "(" + input + ")", nil
	}))
	defer stop()
	workflow := createSequentialWorkflow(t, repository)
	if _, err := manager.StartWorkflow(context.Background(), workflow.ID); err != nil {
		t.Fatal(err)
	}
	completed := waitForWorkflow(t, repository, workflow.ID, domain.WorkflowSucceeded)
	if got, want := completed.Steps[2].Output, "agt_c(agt_b(agt_a(document)))"; got != want {
		t.Fatalf("unexpected propagated output: got=%q want=%q", got, want)
	}
	first, err := repository.GetRun(context.Background(), completed.Steps[0].RunID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.GetRun(context.Background(), completed.Steps[1].RunID)
	if err != nil {
		t.Fatal(err)
	}
	if second.ParentRunID != first.ID || second.RootRunID != first.ID {
		t.Fatalf("workflow Runs did not reuse lineage: first=%+v second=%+v", first, second)
	}
	events := waitForWorkflowEvent(t, repository, workflow.ID, "workflow.succeeded")
	if len(events) < 5 || events[0].Type != "workflow.started" || events[len(events)-1].Type != "workflow.succeeded" {
		t.Fatalf("unexpected Workflow events: %+v", events)
	}
	_ = runEngine
}

func TestSequentialWorkflowFailureStopsRemainingSteps(t *testing.T) {
	repository, _, manager, stop := newWorkflowHarness(t, executorFunc(func(_ context.Context, agent domain.Agent, input string) (string, error) {
		if agent.ID == "agt_b" {
			return "", errors.New("review failed")
		}
		return input + ":ok", nil
	}))
	defer stop()
	workflow := createSequentialWorkflow(t, repository)
	if _, err := manager.StartWorkflow(context.Background(), workflow.ID); err != nil {
		t.Fatal(err)
	}
	completed := waitForWorkflow(t, repository, workflow.ID, domain.WorkflowFailed)
	if completed.Steps[1].Status != domain.WorkflowStepFailed || completed.Steps[2].Status != domain.WorkflowStepCanceled || completed.Steps[2].RunID != "" {
		t.Fatalf("failure did not interrupt sequential flow: %+v", completed)
	}
}

func TestSequentialWorkflowCancellationStopsRun(t *testing.T) {
	repository, _, manager, stop := newWorkflowHarness(t, executorFunc(func(ctx context.Context, _ domain.Agent, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}))
	defer stop()
	workflow := createSequentialWorkflow(t, repository)
	if _, err := manager.StartWorkflow(context.Background(), workflow.ID); err != nil {
		t.Fatal(err)
	}
	waitForStepStatus(t, repository, workflow.ID, domain.WorkflowStepRunning)
	canceled, err := manager.CancelWorkflow(context.Background(), workflow.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != domain.WorkflowCanceled {
		t.Fatalf("unexpected cancellation: %+v", canceled)
	}
	waitForWorkflow(t, repository, workflow.ID, domain.WorkflowCanceled)
}

func TestWorkflowFanOutFanInRunsInParallelAndAggregates(t *testing.T) {
	repository := store.NewMemory()
	for _, id := range []string{"agt_a", "agt_b", "agt_c", "agt_e", "agt_d"} {
		if _, err := repository.CreateAgent(context.Background(), domain.Agent{ID: id, Name: id}); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan string, 3)
	release := make(chan struct{})
	var current, maximum atomic.Int32
	var inputsMu sync.Mutex
	inputs := make(map[string]string)
	executor := executorFunc(func(ctx context.Context, agent domain.Agent, input string) (string, error) {
		inputsMu.Lock()
		inputs[agent.ID] = input
		inputsMu.Unlock()
		if agent.ID == "agt_b" || agent.ID == "agt_c" || agent.ID == "agt_e" {
			active := current.Add(1)
			defer current.Add(-1)
			for {
				observed := maximum.Load()
				if active <= observed || maximum.CompareAndSwap(observed, active) {
					break
				}
			}
			started <- agent.ID
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-release:
			}
		}
		return agent.ID + "(" + input + ")", nil
	})
	runEngine := engine.New(repository, events.NewBus(), executor, queue.NewMemory(16), coordination.NewMemory(), 4, engine.RetryPolicy{
		MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute, AttemptTimeout: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	manager := NewWithConcurrency(repository, runEngine, 2)
	manager.Run(ctx)
	defer func() { cancel(); manager.Stop(); runEngine.Stop() }()
	w, err := repository.CreateWorkflow(context.Background(), domain.Workflow{ID: "wf_fanout", Input: "document", Steps: []domain.WorkflowStep{
		{ID: "a", AgentID: "agt_a", InputFrom: []string{"workflow"}},
		{ID: "b", AgentID: "agt_b", DependsOn: []string{"a"}, InputFrom: []string{"a"}},
		{ID: "c", AgentID: "agt_c", DependsOn: []string{"a"}, InputFrom: []string{"a"}},
		{ID: "e", AgentID: "agt_e", DependsOn: []string{"a"}, InputFrom: []string{"a"}},
		{ID: "d", AgentID: "agt_d", DependsOn: []string{"b", "c", "e"}, InputFrom: []string{"b", "c", "e"}, InputAggregation: "json-array"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartWorkflow(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("fan-out branches did not overlap")
		}
	}
	select {
	case third := <-started:
		t.Fatalf("concurrency limit admitted third branch %s before a slot was free", third)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	completed := waitForWorkflow(t, repository, w.ID, domain.WorkflowSucceeded)
	if maximum.Load() != 2 {
		t.Fatalf("expected two concurrent branches, got %d", maximum.Load())
	}
	inputsMu.Lock()
	gotInput := inputs["agt_d"]
	inputsMu.Unlock()
	wantInput := `["agt_b(agt_a(document))","agt_c(agt_a(document))","agt_e(agt_a(document))"]`
	if gotInput != wantInput {
		t.Fatalf("unexpected fan-in aggregation: got=%s want=%s", gotInput, wantInput)
	}
	branchB, _ := repository.GetRun(context.Background(), completed.Steps[1].RunID)
	branchC, _ := repository.GetRun(context.Background(), completed.Steps[2].RunID)
	branchE, _ := repository.GetRun(context.Background(), completed.Steps[3].RunID)
	root := completed.Steps[0].RunID
	if branchB.ParentRunID != root || branchC.ParentRunID != root || branchE.ParentRunID != root {
		t.Fatalf("fan-out branches lost Run lineage: B=%+v C=%+v E=%+v", branchB, branchC, branchE)
	}
}

func TestWorkflowFanOutFailureCancelsSiblingAndFanIn(t *testing.T) {
	repository := store.NewMemory()
	for _, id := range []string{"agt_a", "agt_b", "agt_c", "agt_d"} {
		_, _ = repository.CreateAgent(context.Background(), domain.Agent{ID: id, Name: id})
	}
	branchCStarted := make(chan struct{})
	branchCCanceled := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, agent domain.Agent, input string) (string, error) {
		switch agent.ID {
		case "agt_b":
			select {
			case <-branchCStarted:
			case <-ctx.Done():
				return "", ctx.Err()
			}
			return "", errors.New("branch failed")
		case "agt_c":
			close(branchCStarted)
			<-ctx.Done()
			close(branchCCanceled)
			return "", ctx.Err()
		default:
			return input, nil
		}
	})
	runEngine := engine.New(repository, events.NewBus(), executor, queue.NewMemory(16), coordination.NewMemory(), 4, engine.RetryPolicy{
		MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute, AttemptTimeout: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	manager := NewWithConcurrency(repository, runEngine, 2)
	manager.Run(ctx)
	defer func() { cancel(); manager.Stop(); runEngine.Stop() }()
	w, err := repository.CreateWorkflow(context.Background(), domain.Workflow{ID: "wf_partial_failure", Steps: []domain.WorkflowStep{
		{ID: "a", AgentID: "agt_a", Input: "x"},
		{ID: "b", AgentID: "agt_b", DependsOn: []string{"a"}, InputFrom: []string{"a"}},
		{ID: "c", AgentID: "agt_c", DependsOn: []string{"a"}, InputFrom: []string{"a"}},
		{ID: "d", AgentID: "agt_d", DependsOn: []string{"b", "c"}, InputFrom: []string{"b", "c"}, InputAggregation: "json-array"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartWorkflow(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	completed := waitForWorkflow(t, repository, w.ID, domain.WorkflowFailed)
	if completed.Steps[1].Status != domain.WorkflowStepFailed || completed.Steps[2].Status != domain.WorkflowStepCanceled || completed.Steps[3].Status != domain.WorkflowStepCanceled {
		t.Fatalf("unexpected fail-fast states: %+v", completed)
	}
	select {
	case <-branchCCanceled:
	case <-time.After(time.Second):
		t.Fatal("failed fan-out did not cancel active sibling context")
	}
}

func TestWorkflowConditionsSelectOneBranchWithoutEval(t *testing.T) {
	repository := store.NewMemory()
	for _, id := range []string{"agt_classifier", "agt_yes", "agt_no", "agt_join"} {
		_, _ = repository.CreateAgent(context.Background(), domain.Agent{ID: id, Name: id})
	}
	var callsMu sync.Mutex
	calls := make(map[string]int)
	executor := executorFunc(func(_ context.Context, agent domain.Agent, input string) (string, error) {
		callsMu.Lock()
		calls[agent.ID]++
		callsMu.Unlock()
		if agent.ID == "agt_classifier" {
			return "legal", nil
		}
		return agent.ID + ":" + input, nil
	})
	runEngine := engine.New(repository, events.NewBus(), executor, queue.NewMemory(16), coordination.NewMemory(), 3, engine.RetryPolicy{
		MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute, AttemptTimeout: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	manager := NewWithConcurrency(repository, runEngine, 3)
	manager.Run(ctx)
	defer func() { cancel(); manager.Stop(); runEngine.Stop() }()
	w, err := repository.CreateWorkflow(context.Background(), domain.Workflow{ID: "wf_condition", Steps: []domain.WorkflowStep{
		{ID: "classify", AgentID: "agt_classifier", Input: "request"},
		{ID: "legal", AgentID: "agt_yes", Input: "selected", DependsOn: []string{"classify"}, Condition: &domain.WorkflowCondition{Source: "classify", Operator: "equals", Value: "legal"}},
		{ID: "other", AgentID: "agt_no", Input: "selected", DependsOn: []string{"classify"}, Condition: &domain.WorkflowCondition{Source: "classify", Operator: "not-equals", Value: "legal"}},
		{ID: "join", AgentID: "agt_join", InputFrom: []string{"legal", "other"}, InputAggregation: "json-array", DependsOn: []string{"legal", "other"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartWorkflow(context.Background(), w.ID); err != nil {
		t.Fatal(err)
	}
	completed := waitForWorkflow(t, repository, w.ID, domain.WorkflowSucceeded)
	if completed.Steps[1].Status != domain.WorkflowStepSucceeded || completed.Steps[2].Status != domain.WorkflowStepSkipped || completed.Steps[2].RunID != "" {
		t.Fatalf("unexpected branch states: %+v", completed)
	}
	callsMu.Lock()
	noCalls := calls["agt_no"]
	joinCalls := calls["agt_join"]
	callsMu.Unlock()
	if noCalls != 0 || joinCalls != 1 {
		t.Fatalf("condition executed wrong Agents: calls=%+v", calls)
	}
	if completed.Steps[3].Output != `agt_join:["agt_yes:selected",""]` {
		t.Fatalf("unexpected skipped-branch aggregation: %q", completed.Steps[3].Output)
	}
	events, err := repository.ListWorkflowEvents(context.Background(), w.ID, 100)
	if err != nil || !slices.ContainsFunc(events, func(event domain.WorkflowEvent) bool {
		return event.StepID == "other" && event.Type == "workflow.step_skipped"
	}) {
		t.Fatalf("missing skipped event: events=%+v err=%v", events, err)
	}
}

func TestConditionOperators(t *testing.T) {
	tests := []struct {
		actual string
		op     string
		value  string
		want   bool
	}{
		{actual: "legal-case", op: "equals", value: "legal-case", want: true},
		{actual: "legal-case", op: "not-equals", value: "code", want: true},
		{actual: "legal-case", op: "contains", value: "legal", want: true},
		{actual: "legal-case", op: "not-contains", value: "medical", want: true},
	}
	for _, test := range tests {
		if got := evaluateCondition(test.actual, domain.WorkflowCondition{Operator: test.op, Value: test.value}); got != test.want {
			t.Fatalf("operator %s returned %v", test.op, got)
		}
	}
}

func TestSequentialWorkflowRecoveryResumesAssignedRun(t *testing.T) {
	repository, runEngine, manager, stop := newWorkflowHarness(t, executorFunc(func(_ context.Context, agent domain.Agent, input string) (string, error) {
		return agent.ID + "(" + input + ")", nil
	}))
	defer stop()
	workflow := createSequentialWorkflow(t, repository)
	now := time.Now().UTC()
	workflow.Status, workflow.StartedAt = domain.WorkflowRunning, &now
	run, _, err := repository.CreateRun(context.Background(), domain.Run{
		ID: "run_recovered", AgentID: "agt_a", Input: "document", Status: domain.RunQueued,
		MaxAttempts: 1, CreatedAt: now, RequestID: "workflow:" + workflow.ID,
	}, "workflow:"+workflow.ID+":step:a")
	if err != nil {
		t.Fatal(err)
	}
	workflow.Steps[0].RunID, workflow.Steps[0].Status = run.ID, domain.WorkflowStepQueued
	if _, err := repository.UpdateWorkflow(context.Background(), workflow, workflow.Version); err != nil {
		t.Fatal(err)
	}
	if err := runEngine.Enqueue(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForWorkflow(t, repository, workflow.ID, domain.WorkflowSucceeded)
}

func TestSequentialWorkflowUsesEngineRetry(t *testing.T) {
	var calls atomic.Int32
	repository := store.NewMemory()
	if _, err := repository.CreateAgent(context.Background(), domain.Agent{ID: "agt_a", Name: "agent"}); err != nil {
		t.Fatal(err)
	}
	runEngine := engine.New(repository, events.NewBus(), executorFunc(func(_ context.Context, _ domain.Agent, input string) (string, error) {
		if calls.Add(1) == 1 {
			return "", errors.New("temporary")
		}
		return input + ":retried", nil
	}), queue.NewMemory(8), coordination.NewMemory(), 1, engine.RetryPolicy{
		MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute, AttemptTimeout: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	manager := New(repository, runEngine)
	manager.Run(ctx)
	defer func() { cancel(); manager.Stop(); runEngine.Stop() }()
	workflow, err := repository.CreateWorkflow(context.Background(), domain.Workflow{ID: "wf_retry", Steps: []domain.WorkflowStep{{ID: "only", AgentID: "agt_a", Input: "input"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartWorkflow(context.Background(), workflow.ID); err != nil {
		t.Fatal(err)
	}
	completed := waitForWorkflow(t, repository, workflow.ID, domain.WorkflowSucceeded)
	run, err := repository.GetRun(context.Background(), completed.Steps[0].RunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Attempt != 2 || completed.Steps[0].Output != "input:retried" {
		t.Fatalf("Workflow did not reuse Engine retry lifecycle: run=%+v workflow=%+v", run, completed)
	}
}

func newWorkflowHarness(t *testing.T, executor engine.Executor) (*store.Memory, *engine.Engine, *Manager, func()) {
	t.Helper()
	repository := store.NewMemory()
	for _, id := range []string{"agt_a", "agt_b", "agt_c"} {
		if _, err := repository.CreateAgent(context.Background(), domain.Agent{ID: id, Name: id}); err != nil {
			t.Fatal(err)
		}
	}
	runEngine := engine.New(repository, events.NewBus(), executor, queue.NewMemory(32), coordination.NewMemory(), 3, engine.RetryPolicy{
		MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute, AttemptTimeout: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	manager := New(repository, runEngine)
	manager.Run(ctx)
	return repository, runEngine, manager, func() {
		cancel()
		manager.Stop()
		runEngine.Stop()
	}
}

func createSequentialWorkflow(t *testing.T, repository store.Repository) domain.Workflow {
	t.Helper()
	workflow, err := repository.CreateWorkflow(context.Background(), domain.Workflow{ID: "wf_sequential", Input: "document", Steps: []domain.WorkflowStep{
		{ID: "a", AgentID: "agt_a", InputFrom: []string{"workflow"}},
		{ID: "b", AgentID: "agt_b", DependsOn: []string{"a"}, InputFrom: []string{"a"}},
		{ID: "c", AgentID: "agt_c", DependsOn: []string{"b"}, InputFrom: []string{"b"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return workflow
}

func waitForWorkflow(t *testing.T, repository store.Repository, id string, status domain.WorkflowStatus) domain.Workflow {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		workflow, err := repository.GetWorkflow(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if workflow.Status == status {
			return workflow
		}
		if workflow.IsTerminal() && workflow.Status != status {
			t.Fatalf("Workflow ended in %s instead of %s: %+v", workflow.Status, status, workflow)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("Workflow %s did not reach %s", id, status)
	return domain.Workflow{}
}

func waitForStepStatus(t *testing.T, repository store.Repository, id string, status domain.WorkflowStepStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		workflow, err := repository.GetWorkflow(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		for _, step := range workflow.Steps {
			if step.Status == status {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no Step reached %s", status)
}

func waitForWorkflowEvent(t *testing.T, repository store.Repository, id, eventType string) []domain.WorkflowEvent {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		events, err := repository.ListWorkflowEvents(context.Background(), id, 100)
		if err != nil {
			t.Fatal(err)
		}
		if slices.ContainsFunc(events, func(event domain.WorkflowEvent) bool { return event.Type == eventType }) {
			return events
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("Workflow %s did not emit %s", id, eventType)
	return nil
}

func TestResolveInput(t *testing.T) {
	w := domain.Workflow{Input: "root", Steps: []domain.WorkflowStep{{ID: "a", RunID: "run_a", Output: "out", Status: domain.WorkflowStepSucceeded}}}
	input, parent, err := resolveInput(w, domain.WorkflowStep{ID: "b", DependsOn: []string{"a"}, InputFrom: []string{"a"}})
	if err != nil || input != "out" || parent != "run_a" {
		t.Fatalf("unexpected resolution: input=%q parent=%q err=%v", input, parent, err)
	}
	if _, _, err := resolveInput(w, domain.WorkflowStep{ID: "b", InputFrom: []string{"missing"}}); err == nil || !strings.Contains(err.Error(), "source") {
		t.Fatalf("expected missing source error, got %v", err)
	}
}
