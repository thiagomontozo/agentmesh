package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

type Executor interface {
	Execute(ctx context.Context, agent domain.Agent, input string) (string, error)
}

type DemoExecutor struct {
	Delay time.Duration
}

func (d DemoExecutor) Execute(ctx context.Context, agent domain.Agent, input string) (string, error) {
	timer := time.NewTimer(d.Delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-timer.C:
	}

	return fmt.Sprintf("Agent %q processed: %s", agent.Name, input), nil
}

type Engine struct {
	store    *store.Memory
	events   *events.Bus
	executor Executor
	queue    chan string
	workers  int
	wg       sync.WaitGroup
}

func New(s *store.Memory, bus *events.Bus, executor Executor, workers, queueSize int) *Engine {
	return &Engine{
		store:    s,
		events:   bus,
		executor: executor,
		queue:    make(chan string, queueSize),
		workers:  workers,
	}
}

func (e *Engine) Start(ctx context.Context) {
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker(ctx, i+1)
	}
}

func (e *Engine) Stop() {
	e.wg.Wait()
}

func (e *Engine) Enqueue(runID string) error {
	select {
	case e.queue <- runID:
		return nil
	default:
		return fmt.Errorf("run queue is full")
	}
}

func (e *Engine) worker(ctx context.Context, workerID int) {
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case runID := <-e.queue:
			e.execute(ctx, workerID, runID)
		}
	}
}

func (e *Engine) execute(ctx context.Context, workerID int, runID string) {
	run, err := e.store.GetRun(runID)
	if err != nil {
		slog.Error("worker could not load run", "worker", workerID, "run_id", runID, "error", err)
		return
	}

	agent, err := e.store.GetAgent(run.AgentID)
	if err != nil {
		e.failRun(run, fmt.Errorf("agent not found: %w", err))
		return
	}

	started := time.Now().UTC()
	run.Status = domain.RunRunning
	run.StartedAt = &started
	_ = e.store.UpdateRun(run)
	e.publish(run.ID, "run.started", fmt.Sprintf("worker %d started the run", workerID))

	output, err := e.executor.Execute(ctx, agent, run.Input)
	if err != nil {
		e.failRun(run, err)
		return
	}

	completed := time.Now().UTC()
	run.Status = domain.RunSucceeded
	run.Output = output
	run.CompletedAt = &completed
	_ = e.store.UpdateRun(run)
	e.publish(run.ID, "run.succeeded", "run completed successfully")
}

func (e *Engine) failRun(run domain.Run, err error) {
	completed := time.Now().UTC()
	run.Status = domain.RunFailed
	run.Error = err.Error()
	run.CompletedAt = &completed
	_ = e.store.UpdateRun(run)
	e.publish(run.ID, "run.failed", err.Error())
}

func (e *Engine) publish(runID, eventType, message string) {
	e.events.Publish(domain.RunEvent{
		RunID:     runID,
		Type:      eventType,
		Message:   message,
		Timestamp: time.Now().UTC(),
	})
}
