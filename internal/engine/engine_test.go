package engine

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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

func TestEngineProvidesAttemptDeadlineToFastRuntime(t *testing.T) {
	remaining := make(chan time.Duration, 1)
	executor := executorFunc(func(ctx context.Context, _ domain.Agent, _ string) (string, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return "", errors.New("attempt context has no deadline")
		}
		remaining <- time.Until(deadline)
		return "done", nil
	})
	policy := testRetryPolicy(1)
	policy.AttemptTimeout = 500 * time.Millisecond
	memory, _, _, runEngine := newEngineTestWithPolicy(t, executor, policy)
	enqueueTestRun(t, memory, runEngine)

	waitForStatus(t, memory, "run_1", domain.RunSucceeded)
	observed := <-remaining
	if observed <= 0 || observed > policy.AttemptTimeout {
		t.Fatalf("unexpected attempt deadline: remaining=%s timeout=%s", observed, policy.AttemptTimeout)
	}
}

func TestEngineFailsRunWhenAttemptTimesOut(t *testing.T) {
	executor := executorFunc(func(ctx context.Context, _ domain.Agent, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	policy := testRetryPolicy(1)
	policy.AttemptTimeout = 20 * time.Millisecond
	memory, memoryQueue, bus, runEngine := newEngineTestWithPolicy(t, executor, policy)
	enqueueTestRun(t, memory, runEngine)

	run := waitForStatus(t, memory, "run_1", domain.RunFailed)
	if run.Attempt != 1 || !strings.Contains(run.Error, ErrAttemptTimeout.Error()) || !strings.Contains(run.Error, policy.AttemptTimeout.String()) {
		t.Fatalf("unexpected timed out run: %+v", run)
	}
	if len(memoryQueue.DeadLetters()) != 1 {
		t.Fatalf("expected timed out run in dead-letter queue")
	}
	assertRunEventTypes(t, bus, run.ID, "run.started", "run.attempt_timed_out", "run.failed")
}

func TestEngineRejectsSuccessReturnedAfterAttemptDeadline(t *testing.T) {
	executor := executorFunc(func(ctx context.Context, _ domain.Agent, _ string) (string, error) {
		<-ctx.Done()
		return "late success", nil
	})
	policy := testRetryPolicy(1)
	policy.AttemptTimeout = 20 * time.Millisecond
	memory, _, _, runEngine := newEngineTestWithPolicy(t, executor, policy)
	enqueueTestRun(t, memory, runEngine)

	run := waitForStatus(t, memory, "run_1", domain.RunFailed)
	if run.Output != "" || !strings.Contains(run.Error, ErrAttemptTimeout.Error()) {
		t.Fatalf("late runtime result bypassed attempt timeout: %+v", run)
	}
}

func TestEngineDefaultsAttemptTimeoutForLegacyCallers(t *testing.T) {
	runEngine := New(
		store.NewMemory(), events.NewBus(), DemoExecutor{}, queue.NewMemory(1), coordination.NewMemory(), 1,
		RetryPolicy{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: time.Minute},
	)
	if runEngine.retry.AttemptTimeout != DefaultAttemptTimeout {
		t.Fatalf("expected default attempt timeout %s, got %s", DefaultAttemptTimeout, runEngine.retry.AttemptTimeout)
	}
}

func TestEngineRetriesAfterAttemptTimeout(t *testing.T) {
	var attempts atomic.Int32
	executor := executorFunc(func(ctx context.Context, _ domain.Agent, _ string) (string, error) {
		if attempts.Add(1) == 1 {
			<-ctx.Done()
			return "", ctx.Err()
		}
		return "done", nil
	})
	policy := testRetryPolicy(2)
	policy.AttemptTimeout = 20 * time.Millisecond
	memory, _, bus, runEngine := newEngineTestWithPolicy(t, executor, policy)
	enqueueTestRun(t, memory, runEngine)

	run := waitForStatus(t, memory, "run_1", domain.RunSucceeded)
	if run.Attempt != 2 || run.Output != "done" || attempts.Load() != 2 {
		t.Fatalf("unexpected run after timeout retry: run=%+v calls=%d", run, attempts.Load())
	}
	assertRunEventTypes(t, bus, run.ID,
		"run.started", "run.attempt_timed_out", "run.retrying", "run.started", "run.succeeded",
	)
}

func TestEngineRecoversRuntimePanicAndWorkerContinues(t *testing.T) {
	var logOutput bytes.Buffer
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, nil)))
	defer slog.SetDefault(originalLogger)

	executor := executorFunc(func(_ context.Context, _ domain.Agent, input string) (string, error) {
		if input == "panic" {
			panic("executor exploded")
		}
		return "worker survived", nil
	})
	memory := store.NewMemory()
	memoryQueue := queue.NewMemory(8)
	coordinator := coordination.NewMemory()
	runEngine := New(memory, events.NewBus(), executor, memoryQueue, coordinator, 1, testRetryPolicy(1))
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	defer func() {
		cancel()
		runEngine.Stop()
	}()

	if _, err := memory.CreateAgent(ctx, domain.Agent{ID: "agt_1", Name: "panic-test"}); err != nil {
		t.Fatal(err)
	}
	for _, run := range []domain.Run{
		{ID: "run_panic", AgentID: "agt_1", Input: "panic", Status: domain.RunQueued, MaxAttempts: 1},
		{ID: "run_next", AgentID: "agt_1", Input: "healthy", Status: domain.RunQueued, MaxAttempts: 1},
	} {
		if _, _, err := memory.CreateRun(ctx, run, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := runEngine.Enqueue(ctx, "run_panic"); err != nil {
		t.Fatal(err)
	}
	failed := waitForStatus(t, memory, "run_panic", domain.RunFailed)
	if failed.Attempt != 1 || !strings.Contains(failed.Error, ErrRuntimePanic.Error()) || !strings.Contains(failed.Error, "executor exploded") {
		t.Fatalf("panic was not converted to a controlled Run failure: %+v", failed)
	}
	waitForReleasedLease(t, coordinator, "run:run_panic")
	if len(memoryQueue.DeadLetters()) != 1 {
		t.Fatalf("expected panic Run in dead-letter queue")
	}

	if err := runEngine.Enqueue(ctx, "run_next"); err != nil {
		t.Fatal(err)
	}
	completed := waitForStatus(t, memory, "run_next", domain.RunSucceeded)
	if completed.Output != "worker survived" {
		t.Fatalf("worker did not process the next Run: %+v", completed)
	}

	logs := logOutput.String()
	for _, expected := range []string{
		`"msg":"runtime panic recovered"`, `"run_id":"run_panic"`, `"agent_id":"agt_1"`,
		`"attempt":1`, `"panic":"executor exploded"`, `"stack":`,
	} {
		if !strings.Contains(logs, expected) {
			t.Fatalf("panic log is missing %s: %s", expected, logs)
		}
	}
}

func TestEngineCancelsQueuedRunBeforeExecution(t *testing.T) {
	var calls atomic.Int32
	executor := executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		calls.Add(1)
		return "unexpected", nil
	})
	memory := store.NewMemory()
	memoryQueue := queue.NewMemory(4)
	bus := events.NewBus()
	runEngine := New(memory, bus, executor, memoryQueue, coordination.NewMemory(), 1, testRetryPolicy(3))
	createTestData(t, memory, 3)
	if err := runEngine.Enqueue(context.Background(), "run_1"); err != nil {
		t.Fatal(err)
	}
	canceled, err := runEngine.Cancel(context.Background(), "run_1")
	if err != nil || canceled.Status != domain.RunCanceled || canceled.Attempt != 0 {
		t.Fatalf("unexpected queued cancellation: run=%+v err=%v", canceled, err)
	}

	ctx, stop := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	defer func() {
		stop()
		runEngine.Stop()
	}()
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("canceled queued Run executed %d times", calls.Load())
	}
	loaded, err := memory.GetRun(context.Background(), "run_1")
	if err != nil || loaded.Status != domain.RunCanceled {
		t.Fatalf("queued cancellation was not preserved: run=%+v err=%v", loaded, err)
	}
	assertRunEventTypes(t, bus, loaded.ID, "run.canceled")
}

func TestEngineCancelsRunningContextAndStopsRetries(t *testing.T) {
	started := make(chan struct{})
	contextCanceled := make(chan struct{})
	var calls atomic.Int32
	executor := executorFunc(func(ctx context.Context, _ domain.Agent, _ string) (string, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
		close(contextCanceled)
		return "", ctx.Err()
	})
	policy := testRetryPolicy(3)
	policy.InitialBackoff = 5 * time.Millisecond
	memory, _, bus, runEngine := newEngineTestWithPolicy(t, executor, policy)
	enqueueTestRun(t, memory, runEngine)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for running execution")
	}

	canceled, err := runEngine.Cancel(context.Background(), "run_1")
	if err != nil || canceled.Status != domain.RunCanceled || canceled.Attempt != 1 {
		t.Fatalf("unexpected running cancellation: run=%+v err=%v", canceled, err)
	}
	select {
	case <-contextCanceled:
	case <-time.After(time.Second):
		t.Fatal("runtime context was not canceled")
	}
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("canceled Run retried %d times", calls.Load())
	}
	loaded, err := memory.GetRun(context.Background(), "run_1")
	if err != nil || loaded.Status != domain.RunCanceled || loaded.Error != "" {
		t.Fatalf("running cancellation was not preserved: run=%+v err=%v", loaded, err)
	}
	assertRunEventTypes(t, bus, loaded.ID, "run.started", "run.canceled")
}

func TestRemoteReplicaCancellationCannotBeOverwrittenByStaleWorker(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	executionReturned := make(chan struct{})
	executor := executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		close(started)
		<-release
		close(executionReturned)
		return "stale success", nil
	})
	memory := store.NewMemory()
	sharedCoordinator := coordination.NewMemory()
	workerQueue := queue.NewMemory(4)
	workerEngine := New(memory, events.NewBus(), executor, workerQueue, sharedCoordinator, 1, testRetryPolicy(1))
	apiReplicaEngine := New(
		memory, events.NewBus(), DemoExecutor{}, queue.NewMemory(1), sharedCoordinator, 1, testRetryPolicy(1),
	)
	ctx, stop := context.WithCancel(context.Background())
	workerEngine.Start(ctx)
	defer func() {
		stop()
		workerEngine.Stop()
	}()
	createTestData(t, memory, 1)
	if err := workerEngine.Enqueue(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replica A")
	}

	canceled, err := apiReplicaEngine.Cancel(context.Background(), "run_1")
	if err != nil || canceled.Status != domain.RunCanceled {
		t.Fatalf("replica B cancellation failed: run=%+v err=%v", canceled, err)
	}
	close(release)
	select {
	case <-executionReturned:
	case <-time.After(time.Second):
		t.Fatal("replica A runtime did not return")
	}
	waitForReleasedLease(t, sharedCoordinator, "run:run_1")
	loaded, err := memory.GetRun(context.Background(), "run_1")
	if err != nil || loaded.Status != domain.RunCanceled || loaded.Output != "" {
		t.Fatalf("stale replica overwrote canceled Run: run=%+v err=%v", loaded, err)
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
	stopStarted := time.Now()
	cancel()
	runEngine.Stop()
	if elapsed := time.Since(stopStarted); elapsed > time.Second {
		t.Fatalf("Engine.Stop waited %s for a context-aware runtime", elapsed)
	}

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
	memory, memoryQueue, _, runEngine := newEngineTestWithPolicy(t, executor, testRetryPolicy(maxAttempts))
	return memory, memoryQueue, runEngine
}

func newEngineTestWithPolicy(t *testing.T, executor Executor, policy RetryPolicy) (*store.Memory, *queue.Memory, *events.Bus, *Engine) {
	t.Helper()
	memory := store.NewMemory()
	memoryQueue := queue.NewMemory(8)
	bus := events.NewBus()
	runEngine := New(memory, bus, executor, memoryQueue, coordination.NewMemory(), 1, policy)
	ctx, cancel := context.WithCancel(context.Background())
	runEngine.Start(ctx)
	t.Cleanup(func() {
		cancel()
		runEngine.Stop()
	})
	return memory, memoryQueue, bus, runEngine
}

func testRetryPolicy(maxAttempts int) RetryPolicy {
	return RetryPolicy{
		MaxAttempts: maxAttempts, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond,
		LeaseTTL: time.Minute, AttemptTimeout: DefaultAttemptTimeout,
	}
}

func assertRunEventTypes(t *testing.T, bus *events.Bus, runID string, expected ...string) {
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

func waitForReleasedLease(t *testing.T, coordinator coordination.Coordinator, key string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		lease, acquired, err := coordinator.Acquire(context.Background(), key, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if acquired {
			if err := lease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("lease %s was not released after panic", key)
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
