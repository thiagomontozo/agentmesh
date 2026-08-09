package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

type executorFunc func(context.Context, domain.Agent, string) (string, error)

func (f executorFunc) Execute(ctx context.Context, agent domain.Agent, input string) (string, error) {
	return f(ctx, agent, input)
}

func TestEngineCompletesRun(t *testing.T) {
	memory := store.NewMemory()
	memory.CreateAgent(domain.Agent{ID: "agt_1", Name: "test"})
	memory.CreateRun(domain.Run{ID: "run_1", AgentID: "agt_1", Input: "hello", Status: domain.RunQueued})
	bus := events.NewBus()
	executor := executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		return "done", nil
	})
	engine := New(memory, bus, executor, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	engine.Start(ctx)
	t.Cleanup(func() {
		cancel()
		engine.Stop()
	})

	if err := engine.Enqueue("run_1"); err != nil {
		t.Fatal(err)
	}
	run := waitForStatus(t, memory, "run_1", domain.RunSucceeded)
	if run.Output != "done" || run.StartedAt == nil || run.CompletedAt == nil {
		t.Fatalf("unexpected completed run: %+v", run)
	}
}

func TestEngineFailsRunWhenExecutorFails(t *testing.T) {
	memory := store.NewMemory()
	memory.CreateAgent(domain.Agent{ID: "agt_1", Name: "test"})
	memory.CreateRun(domain.Run{ID: "run_1", AgentID: "agt_1", Status: domain.RunQueued})
	bus := events.NewBus()
	wantErr := errors.New("executor failed")
	executor := executorFunc(func(context.Context, domain.Agent, string) (string, error) {
		return "", wantErr
	})
	engine := New(memory, bus, executor, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	engine.Start(ctx)
	t.Cleanup(func() {
		cancel()
		engine.Stop()
	})

	if err := engine.Enqueue("run_1"); err != nil {
		t.Fatal(err)
	}
	run := waitForStatus(t, memory, "run_1", domain.RunFailed)
	if run.Error != wantErr.Error() {
		t.Fatalf("expected %q, got %q", wantErr, run.Error)
	}
}

func TestEngineFailsRunningRunWhenContextIsCanceled(t *testing.T) {
	memory := store.NewMemory()
	memory.CreateAgent(domain.Agent{ID: "agt_1", Name: "test"})
	memory.CreateRun(domain.Run{ID: "run_1", AgentID: "agt_1", Status: domain.RunQueued})
	bus := events.NewBus()
	started := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, _ domain.Agent, _ string) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	engine := New(memory, bus, executor, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	engine.Start(ctx)

	if err := engine.Enqueue("run_1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for executor to start")
	}
	cancel()
	engine.Stop()

	run, err := memory.GetRun("run_1")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != domain.RunFailed || run.Error != context.Canceled.Error() {
		t.Fatalf("unexpected canceled run: %+v", run)
	}
}

func waitForStatus(t *testing.T, memory *store.Memory, runID string, want domain.RunStatus) domain.Run {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		run, err := memory.GetRun(runID)
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
