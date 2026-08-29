package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/coordination"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

type Executor interface {
	Execute(ctx context.Context, agent domain.Agent, input string) (string, error)
}

type DemoExecutor struct {
	Delay time.Duration
}

type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	LeaseTTL       time.Duration
	AttemptTimeout time.Duration
}

const DefaultAttemptTimeout = 30 * time.Second
const DefaultLeaseTTL = 5 * time.Minute

var ErrAttemptTimeout = errors.New("run attempt timed out")
var ErrRuntimePanic = errors.New("runtime panic")
var ErrLeaseRenewal = errors.New("run lease renewal failed")

type leaseKeeper struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.RWMutex
	err    error
}

func startLeaseKeeper(parent context.Context, lease coordination.Lease, ttl time.Duration, onFailure context.CancelFunc) *leaseKeeper {
	ctx, cancel := context.WithCancel(parent)
	keeper := &leaseKeeper{cancel: cancel, done: make(chan struct{})}
	interval := ttl / 3
	if interval <= 0 {
		interval = ttl
	}
	go func() {
		defer close(keeper.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, cancelRenew := context.WithTimeout(ctx, interval)
				err := lease.Renew(renewCtx, ttl)
				cancelRenew()
				if err == nil {
					continue
				}
				if ctx.Err() != nil {
					return
				}
				keeper.mu.Lock()
				keeper.err = fmt.Errorf("%w: %w", ErrLeaseRenewal, err)
				keeper.mu.Unlock()
				onFailure()
				return
			}
		}
	}()
	return keeper
}

func (k *leaseKeeper) Stop() {
	k.cancel()
	<-k.done
}

func (k *leaseKeeper) Err() error {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.err
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
	store    store.Repository
	events   events.Broker
	resolver agentruntime.Resolver
	queue    queue.Queue
	coord    coordination.Coordinator
	workers  int
	retry    RetryPolicy
	wg       sync.WaitGroup
	errors   chan error
	activeMu sync.Mutex
	active   map[string]context.CancelFunc
}

func New(s store.Repository, bus events.Broker, executor Executor, q queue.Queue, coord coordination.Coordinator, workers int, retry RetryPolicy) *Engine {
	resolver := agentruntime.NewRegistry(agentruntime.AdaptLegacy(executor))
	return NewWithResolver(s, bus, resolver, q, coord, workers, retry)
}

func NewWithResolver(s store.Repository, bus events.Broker, resolver agentruntime.Resolver, q queue.Queue, coord coordination.Coordinator, workers int, retry RetryPolicy) *Engine {
	if retry.AttemptTimeout <= 0 {
		retry.AttemptTimeout = DefaultAttemptTimeout
	}
	if retry.LeaseTTL <= 0 {
		retry.LeaseTTL = DefaultLeaseTTL
	}
	return &Engine{
		store:    s,
		events:   bus,
		resolver: resolver,
		queue:    q,
		coord:    coord,
		workers:  workers,
		retry:    retry,
		errors:   make(chan error, 1),
		active:   make(map[string]context.CancelFunc),
	}
}

func (e *Engine) Start(ctx context.Context) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		if err := e.queue.Consume(ctx, e.workers, e.execute); err != nil && ctx.Err() == nil {
			select {
			case e.errors <- err:
			default:
			}
		}
	}()
}

func (e *Engine) Stop() {
	e.wg.Wait()
	if err := e.queue.Close(); err != nil {
		slog.Error("run queue close failed", "error", err)
	}
}

func (e *Engine) Errors() <-chan error {
	return e.errors
}

func (e *Engine) MaxAttempts() int {
	return e.retry.MaxAttempts
}

func (e *Engine) Enqueue(ctx context.Context, runID string) error {
	return e.queue.Enqueue(ctx, runID)
}

func (e *Engine) Cancel(ctx context.Context, runID string) (domain.Run, error) {
	run, err := e.store.CancelRun(ctx, runID, time.Now())
	if err != nil {
		return run, err
	}
	e.activeMu.Lock()
	cancelExecution := e.active[runID]
	e.activeMu.Unlock()
	if cancelExecution != nil {
		cancelExecution()
	}
	e.publish(run.ID, "run.canceled", "run canceled", run.Attempt)
	return run, nil
}

func (e *Engine) Recover(ctx context.Context) error {
	pendingRuns, err := e.store.ListPendingRuns(ctx)
	if err != nil {
		return fmt.Errorf("list pending runs: %w", err)
	}
	for _, pending := range pendingRuns {
		if pending.Status == domain.RunQueued {
			if err := e.Enqueue(ctx, pending.ID); err != nil {
				return fmt.Errorf("enqueue pending run %s: %w", pending.ID, err)
			}
			continue
		}
		if pending.Status != domain.RunRunning {
			continue
		}
		lease, acquired, err := e.coord.Acquire(ctx, "run:"+pending.ID, e.retry.LeaseTTL)
		if err != nil {
			return fmt.Errorf("acquire recovery lease for run %s: %w", pending.ID, err)
		}
		if !acquired {
			continue
		}
		recovered, recoverErr := e.store.RecoverRun(ctx, pending.ID, lease.FencingToken())
		releaseErr := releaseLease(lease)
		if recoverErr != nil {
			return fmt.Errorf("recover run %s: %w", pending.ID, recoverErr)
		}
		if releaseErr != nil {
			return fmt.Errorf("release recovery lease for run %s: %w", pending.ID, releaseErr)
		}
		if recovered {
			if err := e.Enqueue(ctx, pending.ID); err != nil {
				return fmt.Errorf("enqueue recovered run %s: %w", pending.ID, err)
			}
		}
	}
	return nil
}

func (e *Engine) Ready(ctx context.Context) error {
	if err := e.store.Ping(ctx); err != nil {
		return fmt.Errorf("repository: %w", err)
	}
	if err := e.queue.Ping(ctx); err != nil {
		return fmt.Errorf("queue: %w", err)
	}
	if err := e.coord.Ping(ctx); err != nil {
		return fmt.Errorf("coordination: %w", err)
	}
	return nil
}

func (e *Engine) execute(ctx context.Context, runID string) error {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("load run %s: %w", runID, err)
	}
	if run.Status == domain.RunSucceeded {
		return nil
	}
	if run.Status == domain.RunFailed {
		return e.queue.DeadLetter(ctx, run.ID, errors.New(run.Error))
	}
	if run.Status == domain.RunCanceled {
		return nil
	}
	lease, acquired, err := e.coord.Acquire(ctx, "run:"+run.ID, e.retry.LeaseTTL)
	if err != nil {
		return fmt.Errorf("acquire run lease: %w", err)
	}
	if !acquired {
		return fmt.Errorf("run %s is already being processed", run.ID)
	}
	defer func() {
		if err := releaseLease(lease); err != nil {
			slog.Error("run lease release failed", "run_id", run.ID, "error", err)
		}
	}()
	executionFence, err := e.store.ClaimRunExecution(ctx, run.ID, lease.FencingToken())
	if errors.Is(err, store.ErrRunCanceled) || errors.Is(err, store.ErrRunNotExecutable) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("claim run %s execution: %w", run.ID, err)
	}
	runCtx, cancelExecution := context.WithCancel(ctx)
	e.activeMu.Lock()
	e.active[run.ID] = cancelExecution
	e.activeMu.Unlock()
	defer func() {
		cancelExecution()
		e.activeMu.Lock()
		delete(e.active, run.ID)
		e.activeMu.Unlock()
	}()
	keeper := startLeaseKeeper(ctx, lease, e.retry.LeaseTTL, cancelExecution)
	defer keeper.Stop()

	run, err = e.store.GetRun(ctx, run.ID)
	if err != nil {
		return fmt.Errorf("reload run %s: %w", run.ID, err)
	}
	if run.Status == domain.RunCanceled {
		return nil
	}

	agent, err := e.store.GetAgent(ctx, run.AgentID)
	if err != nil {
		return e.failRun(ctx, run, executionFence, fmt.Errorf("agent not found: %w", err))
	}
	implementation, err := e.resolver.Resolve(agent)
	if err != nil {
		return e.failRun(ctx, run, executionFence, fmt.Errorf("resolve runtime for agent %s: %w", agent.ID, err))
	}

	if run.MaxAttempts < 1 {
		run.MaxAttempts = e.retry.MaxAttempts
	}
	for {
		if renewalErr := e.leaseRenewalError(run.ID, run.Attempt, keeper); renewalErr != nil {
			return renewalErr
		}
		if runCtx.Err() != nil {
			return nil
		}
		if run.Status == domain.RunQueued {
			err = run.Start(time.Now())
		} else {
			err = run.Retry()
		}
		if err != nil {
			return err
		}
		if err := e.store.UpdateRunFenced(ctx, run, executionFence); err != nil {
			if errors.Is(err, store.ErrRunCanceled) {
				return nil
			}
			return fmt.Errorf("persist run attempt: %w", err)
		}
		e.publish(run.ID, "run.started", fmt.Sprintf("attempt %d of %d started", run.Attempt, run.MaxAttempts), run.Attempt)

		attemptCtx, cancelAttempt := context.WithTimeout(runCtx, e.retry.AttemptTimeout)
		result, executeErr := executeRuntimeSafely(attemptCtx, implementation, agentruntime.ExecutionRequest{
			RunID: run.ID, Agent: agent, Attempt: run.Attempt, Input: run.Input,
		})
		attemptContextErr := attemptCtx.Err()
		cancelAttempt()
		if renewalErr := e.leaseRenewalError(run.ID, run.Attempt, keeper); renewalErr != nil {
			return renewalErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if runCtx.Err() != nil {
			return nil
		}
		if errors.Is(attemptContextErr, context.DeadlineExceeded) && !errors.Is(executeErr, ErrRuntimePanic) {
			executeErr = fmt.Errorf("%w: attempt %d exceeded %s", ErrAttemptTimeout, run.Attempt, e.retry.AttemptTimeout)
			e.publish(run.ID, "run.attempt_timed_out", executeErr.Error(), run.Attempt)
		}
		if executeErr == nil {
			if err := run.Succeed(result.Output, time.Now()); err != nil {
				return err
			}
			if err := e.store.UpdateRunFenced(ctx, run, executionFence); err != nil {
				if errors.Is(err, store.ErrRunCanceled) {
					return nil
				}
				return fmt.Errorf("persist completed run: %w", err)
			}
			e.publish(run.ID, "run.succeeded", "run completed successfully", run.Attempt)
			return nil
		}
		if run.Attempt >= run.MaxAttempts {
			return e.failRun(ctx, run, executionFence, executeErr)
		}

		backoff := e.backoff(run.Attempt)
		e.publish(run.ID, "run.retrying", fmt.Sprintf("attempt %d failed; retrying in %s: %v", run.Attempt, backoff, executeErr), run.Attempt)
		retryTimer := time.NewTimer(backoff)
		select {
		case <-runCtx.Done():
			retryTimer.Stop()
			if renewalErr := e.leaseRenewalError(run.ID, run.Attempt, keeper); renewalErr != nil {
				return renewalErr
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		case <-retryTimer.C:
		}
	}
}

func releaseLease(lease coordination.Lease) error {
	releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return lease.Release(releaseCtx)
}

func (e *Engine) leaseRenewalError(runID string, attempt int, keeper *leaseKeeper) error {
	err := keeper.Err()
	if err == nil {
		return nil
	}
	e.publish(runID, "run.lease_lost", err.Error(), attempt)
	return err
}

func executeRuntimeSafely(ctx context.Context, implementation agentruntime.Runtime, request agentruntime.ExecutionRequest) (result agentruntime.ExecutionResult, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = agentruntime.ExecutionResult{}
			err = fmt.Errorf("%w: %v", ErrRuntimePanic, recovered)
			slog.ErrorContext(ctx, "runtime panic recovered",
				"run_id", request.RunID,
				"agent_id", request.AgentID(),
				"attempt", request.Attempt,
				"panic", fmt.Sprint(recovered),
				"stack", string(debug.Stack()),
			)
		}
	}()
	return implementation.Execute(ctx, request)
}

func (e *Engine) failRun(ctx context.Context, run domain.Run, executionFence int64, err error) error {
	if transitionErr := run.Fail(err, time.Now()); transitionErr != nil {
		return transitionErr
	}
	if updateErr := e.store.UpdateRunFenced(ctx, run, executionFence); updateErr != nil {
		if errors.Is(updateErr, store.ErrRunCanceled) {
			return nil
		}
		return fmt.Errorf("persist failed run: %w", updateErr)
	}
	e.publish(run.ID, "run.failed", err.Error(), run.Attempt)
	if deadLetterErr := e.queue.DeadLetter(ctx, run.ID, err); deadLetterErr != nil {
		return fmt.Errorf("dead-letter run: %w", deadLetterErr)
	}
	return nil
}

func (e *Engine) backoff(attempt int) time.Duration {
	backoff := e.retry.InitialBackoff
	for i := 1; i < attempt; i++ {
		if backoff >= e.retry.MaxBackoff/2 {
			return e.retry.MaxBackoff
		}
		backoff *= 2
	}
	if backoff > e.retry.MaxBackoff {
		return e.retry.MaxBackoff
	}
	return backoff
}

func (e *Engine) publish(runID, eventType, message string, attempt int) {
	e.events.Publish(domain.RunEvent{
		RunID:     runID,
		Type:      eventType,
		Message:   message,
		Attempt:   attempt,
		Timestamp: time.Now().UTC(),
	})
}
