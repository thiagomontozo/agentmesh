package workflow

import (
	"context"
	"errors"
	"strings"
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
	events, err := repository.ListWorkflowEvents(context.Background(), workflow.ID, 100)
	if err != nil {
		t.Fatal(err)
	}
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

func TestSequentialStartRejectsFanOut(t *testing.T) {
	repository, _, manager, stop := newWorkflowHarness(t, executorFunc(func(_ context.Context, _ domain.Agent, input string) (string, error) { return input, nil }))
	defer stop()
	workflow, err := repository.CreateWorkflow(context.Background(), domain.Workflow{ID: "wf_fanout", Steps: []domain.WorkflowStep{
		{ID: "a", AgentID: "agt_a", Input: "x"},
		{ID: "b", AgentID: "agt_b", Input: "x", DependsOn: []string{"a"}},
		{ID: "c", AgentID: "agt_c", Input: "x", DependsOn: []string{"a"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartWorkflow(context.Background(), workflow.ID); !errors.Is(err, ErrNotSequential) {
		t.Fatalf("expected fan-out rejection, got %v", err)
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

func TestResolveSequentialInput(t *testing.T) {
	w := domain.Workflow{Input: "root", Steps: []domain.WorkflowStep{{ID: "a", RunID: "run_a", Output: "out", Status: domain.WorkflowStepSucceeded}}}
	input, parent, err := resolveSequentialInput(w, domain.WorkflowStep{ID: "b", DependsOn: []string{"a"}, InputFrom: []string{"a"}})
	if err != nil || input != "out" || parent != "run_a" {
		t.Fatalf("unexpected resolution: input=%q parent=%q err=%v", input, parent, err)
	}
	if _, _, err := resolveSequentialInput(w, domain.WorkflowStep{ID: "b", InputFrom: []string{"missing"}}); err == nil || !strings.Contains(err.Error(), "source") {
		t.Fatalf("expected missing source error, got %v", err)
	}
}
