package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/coordination"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

type executorFunc func(context.Context, domain.Agent, string) (string, error)

func (f executorFunc) Execute(ctx context.Context, agent domain.Agent, input string) (string, error) {
	return f(ctx, agent, input)
}

type resolvedRuntimeFunc func(context.Context, agentruntime.ExecutionRequest) (agentruntime.ExecutionResult, error)

func (f resolvedRuntimeFunc) Execute(ctx context.Context, request agentruntime.ExecutionRequest) (agentruntime.ExecutionResult, error) {
	return f(ctx, request)
}

func TestEngineCompletesRun(t *testing.T) {
	memory, _, runEngine := newEngineTest(t, executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		return "done", nil
	}), 3)
	enqueueTestRun(t, memory, runEngine)

	run := waitForStatus(t, memory, "run_1", domain.RunSucceeded)
	if run.Output != "done" || run.Attempt != 1 || run.StartedAt == nil || run.CompletedAt == nil {
		t.Fatalf("unexpected completed run: %+v", run)
	}
}

func TestEnginePassesAttemptContextThroughResolvedRuntime(t *testing.T) {
	requests := make(chan agentruntime.ExecutionRequest, 1)
	implementation := resolvedRuntimeFunc(func(_ context.Context, request agentruntime.ExecutionRequest) (agentruntime.ExecutionResult, error) {
		requests <- request
		return agentruntime.ExecutionResult{Output: "resolved"}, nil
	})
	resolver := agentruntime.NewRegistry(implementation)
	memory := store.NewMemory()
	memoryQueue := queue.NewMemory(8)
	runEngine := NewWithResolver(memory, events.NewBus(), resolver, memoryQueue, coordination.NewMemory(), 1, testRetryPolicy(3))
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	t.Cleanup(func() {
		cancel()
		runEngine.Stop()
	})
	enqueueTestRun(t, memory, runEngine)

	run := waitForStatus(t, memory, "run_1", domain.RunSucceeded)
	request := <-requests
	if request.RunID != run.ID || request.AgentID() != run.AgentID || request.Attempt != 1 || request.Input != "hello" {
		t.Fatalf("unexpected runtime request: %+v", request)
	}
}

func TestEngineFailsRunForUnknownRuntime(t *testing.T) {
	memory, memoryQueue, runEngine := newEngineTest(t, executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		return "unexpected", nil
	}), 3)
	ctx := context.Background()
	if _, err := memory.CreateAgent(ctx, domain.Agent{ID: "agt_1", Name: "remote", Runtime: "remote-http"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := memory.CreateRun(ctx, domain.Run{
		ID: "run_1", AgentID: "agt_1", Input: "hello", Status: domain.RunQueued, MaxAttempts: 3,
	}, ""); err != nil {
		t.Fatal(err)
	}
	if err := runEngine.Enqueue(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}

	run := waitForStatus(t, memory, "run_1", domain.RunFailed)
	if !strings.Contains(run.Error, agentruntime.ErrUnknownRuntime.Error()) || !strings.Contains(run.Error, "remote-http") {
		t.Fatalf("expected explicit unknown runtime error, got %+v", run)
	}
	if len(memoryQueue.DeadLetters()) != 1 {
		t.Fatalf("expected unknown runtime in dead-letter queue")
	}
}

func TestEngineRetriesBeforeSuccess(t *testing.T) {
	var attempts atomic.Int32
	wantErr := errors.New("temporary failure")
	executor := executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		if attempts.Add(1) < 3 {
			return "", wantErr
		}
		return "done", nil
	})
	memory, _, runEngine := newEngineTest(t, executor, 3)
	enqueueTestRun(t, memory, runEngine)

	run := waitForStatus(t, memory, "run_1", domain.RunSucceeded)
	if run.Attempt != 3 || run.Output != "done" {
		t.Fatalf("unexpected retried run: %+v", run)
	}
}

func TestEngineDeadLettersAfterRetriesAreExhausted(t *testing.T) {
	wantErr := errors.New("executor failed")
	executor := executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		return "", wantErr
	})
	memory, memoryQueue, runEngine := newEngineTest(t, executor, 2)
	enqueueTestRun(t, memory, runEngine)

	run := waitForStatus(t, memory, "run_1", domain.RunFailed)
	if run.Error != wantErr.Error() || run.Attempt != 2 {
		t.Fatalf("unexpected failed run: %+v", run)
	}
	deadLetters := memoryQueue.DeadLetters()
	if len(deadLetters) != 1 || deadLetters[0].RunID != run.ID {
		t.Fatalf("unexpected dead letters: %+v", deadLetters)
	}
}

func TestEngineLeavesRunningRunRecoverableWhenContextIsCanceled(t *testing.T) {
	started := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, _ domain.Agent, _ string) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	memory := store.NewMemory()
	memoryQueue := queue.NewMemory(8)
	bus := events.NewBus()
	runEngine := New(memory, bus, executor, memoryQueue, coordination.NewMemory(), 1, testRetryPolicy(3))
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	createTestData(t, memory, 3)
	if err := runEngine.Enqueue(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for executor to start")
	}
	cancel()
	runEngine.Stop()

	run, err := memory.GetRun(context.Background(), "run_1")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != domain.RunRunning {
		t.Fatalf("expected recoverable running run, got %+v", run)
	}
	ids, err := memory.RecoverPendingRuns(context.Background())
	if err != nil || len(ids) != 1 || ids[0] != run.ID {
		t.Fatalf("unexpected recovery result: ids=%v err=%v", ids, err)
	}
	recovered, err := memory.GetRun(context.Background(), run.ID)
	if err != nil || recovered.Status != domain.RunQueued || recovered.Attempt != 0 {
		t.Fatalf("unexpected recovered run: run=%+v err=%v", recovered, err)
	}
}

func TestEngineIgnoresDuplicateDeliveryForSucceededRun(t *testing.T) {
	var calls atomic.Int32
	executor := executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		calls.Add(1)
		return "unexpected", nil
	})
	memory, _, runEngine := newEngineTest(t, executor, 3)
	ctx := context.Background()
	if _, err := memory.CreateAgent(ctx, domain.Agent{ID: "agt_1", Name: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := memory.CreateRun(ctx, domain.Run{
		ID: "run_1", AgentID: "agt_1", Status: domain.RunSucceeded, Attempt: 1, MaxAttempts: 3,
	}, ""); err != nil {
		t.Fatal(err)
	}
	if err := runEngine.Enqueue(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("executor was called %d times", calls.Load())
	}
}

func TestEngineLeasePreventsConcurrentDuplicateExecution(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	executor := executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return "done", nil
	})
	memory := store.NewMemory()
	memoryQueue := queue.NewMemory(4)
	runEngine := New(memory, events.NewBus(), executor, memoryQueue, coordination.NewMemory(), 2, testRetryPolicy(3))
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	t.Cleanup(func() {
		cancel()
		runEngine.Stop()
	})
	createTestData(t, memory, 3)
	if err := runEngine.Enqueue(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	if err := runEngine.Enqueue(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for executor")
	}
	close(release)
	waitForStatus(t, memory, "run_1", domain.RunSucceeded)
	time.Sleep(150 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("expected one executor call, got %d", calls.Load())
	}
}

func newEngineTest(t *testing.T, executor Executor, maxAttempts int) (*store.Memory, *queue.Memory, *Engine) {
	t.Helper()
	memory := store.NewMemory()
	memoryQueue := queue.NewMemory(8)
	bus := events.NewBus()
	runEngine := New(memory, bus, executor, memoryQueue, coordination.NewMemory(), 1, testRetryPolicy(maxAttempts))
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	t.Cleanup(func() {
		cancel()
		runEngine.Stop()
	})
	return memory, memoryQueue, runEngine
}

func testRetryPolicy(maxAttempts int) RetryPolicy {
	return RetryPolicy{
		MaxAttempts: maxAttempts, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, LeaseTTL: time.Minute,
	}
}

func enqueueTestRun(t *testing.T, memory *store.Memory, runEngine *Engine) {
	t.Helper()
	createTestData(t, memory, runEngine.MaxAttempts())
	if err := runEngine.Enqueue(context.Background(), "run_1"); err != nil {
		t.Fatal(err)
	}
}

func createTestData(t *testing.T, memory *store.Memory, maxAttempts int) {
	t.Helper()
	ctx := context.Background()
	if _, err := memory.CreateAgent(ctx, domain.Agent{ID: "agt_1", Name: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := memory.CreateRun(ctx, domain.Run{
		ID: "run_1", AgentID: "agt_1", Input: "hello", Status: domain.RunQueued, MaxAttempts: maxAttempts,
	}, ""); err != nil {
		t.Fatal(err)
	}
}

func waitForStatus(t *testing.T, memory *store.Memory, runID string, want domain.RunStatus) domain.Run {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		run, err := memory.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == want {
			return run
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for run status %s", want)
	return domain.Run{}
}
