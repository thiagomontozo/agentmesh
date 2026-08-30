package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

const (
	defaultPollInterval = 25 * time.Millisecond
	eventRetention      = 7 * 24 * time.Hour
	eventHistoryLimit   = 1000
)

var ErrNotSequential = errors.New("workflow is not sequential")

type Manager struct {
	store  store.Repository
	engine *engine.Engine
	poll   time.Duration

	mu     sync.Mutex
	ctx    context.Context
	active map[string]struct{}
	wg     sync.WaitGroup
}

func New(repository store.Repository, runEngine *engine.Engine) *Manager {
	return &Manager{store: repository, engine: runEngine, poll: defaultPollInterval, active: make(map[string]struct{})}
}

func (m *Manager) Run(ctx context.Context) {
	m.mu.Lock()
	m.ctx = ctx
	m.mu.Unlock()
}

func (m *Manager) Stop() { m.wg.Wait() }

func (m *Manager) Recover(ctx context.Context) error {
	workflows, err := m.store.ListRunningWorkflows(ctx)
	if err != nil {
		return fmt.Errorf("list running workflows: %w", err)
	}
	for _, candidate := range workflows {
		if err := candidate.ValidateSequential(); err != nil {
			m.failInvalidRecovered(ctx, candidate, err)
			continue
		}
		m.schedule(candidate.ID)
	}
	return nil
}

func (m *Manager) StartWorkflow(ctx context.Context, id string) (domain.Workflow, error) {
	candidate, err := m.store.GetWorkflow(ctx, id)
	if err != nil {
		return domain.Workflow{}, err
	}
	if err := candidate.ValidateSequential(); err != nil {
		return domain.Workflow{}, fmt.Errorf("%w: %v", ErrNotSequential, err)
	}
	if candidate.Status != domain.WorkflowPending {
		return domain.Workflow{}, fmt.Errorf("workflow cannot start from status %s", candidate.Status)
	}
	now := time.Now().UTC()
	candidate.Status = domain.WorkflowRunning
	candidate.StartedAt = &now
	updated, err := m.store.UpdateWorkflow(ctx, candidate, candidate.Version)
	if err != nil {
		return domain.Workflow{}, err
	}
	m.emit(ctx, updated.ID, "", "", "workflow.started", "workflow execution started")
	m.schedule(updated.ID)
	return updated, nil
}

func (m *Manager) CancelWorkflow(ctx context.Context, id string) (domain.Workflow, error) {
	candidate, err := m.store.GetWorkflow(ctx, id)
	if err != nil {
		return domain.Workflow{}, err
	}
	if candidate.Status != domain.WorkflowPending && candidate.Status != domain.WorkflowRunning {
		return domain.Workflow{}, fmt.Errorf("%w from status %s", domain.ErrWorkflowNotCancelable, candidate.Status)
	}
	now := time.Now().UTC()
	candidate.Status = domain.WorkflowCanceled
	candidate.CompletedAt = &now
	activeRunIDs := make([]string, 0)
	for index := range candidate.Steps {
		step := &candidate.Steps[index]
		if step.Status == domain.WorkflowStepQueued || step.Status == domain.WorkflowStepRunning {
			activeRunIDs = append(activeRunIDs, step.RunID)
		}
		if step.Status == domain.WorkflowStepPending || step.Status == domain.WorkflowStepQueued || step.Status == domain.WorkflowStepRunning {
			step.Status = domain.WorkflowStepCanceled
			step.CompletedAt = &now
		}
	}
	updated, err := m.store.UpdateWorkflow(ctx, candidate, candidate.Version)
	if err != nil {
		return domain.Workflow{}, err
	}
	for _, runID := range activeRunIDs {
		if _, cancelErr := m.engine.Cancel(ctx, runID); cancelErr != nil && !errors.Is(cancelErr, domain.ErrRunNotCancelable) {
			slog.WarnContext(ctx, "workflow Run cancellation failed", "workflow_id", id, "run_id", runID, "error", cancelErr)
		}
	}
	m.emit(ctx, updated.ID, "", "", "workflow.canceled", "workflow canceled")
	return updated, nil
}

func (m *Manager) schedule(id string) {
	m.mu.Lock()
	if _, exists := m.active[id]; exists {
		m.mu.Unlock()
		return
	}
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	m.active[id] = struct{}{}
	m.wg.Add(1)
	m.mu.Unlock()
	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.active, id)
			m.mu.Unlock()
			m.wg.Done()
		}()
		if err := m.reconcile(ctx, id); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "workflow reconciliation stopped", "workflow_id", id, "error", err)
		}
	}()
}

func (m *Manager) reconcile(ctx context.Context, id string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidate, err := m.store.GetWorkflow(ctx, id)
		if err != nil {
			return err
		}
		if candidate.IsTerminal() {
			return nil
		}
		changed, terminal, err := m.refreshSteps(ctx, &candidate)
		if err != nil {
			return err
		}
		if terminal {
			if _, err := m.store.UpdateWorkflow(ctx, candidate, candidate.Version); errors.Is(err, store.ErrConflict) {
				continue
			} else if err != nil {
				return err
			}
			m.emit(ctx, candidate.ID, "", "", workflowTerminalEvent(candidate.Status), candidate.Error)
			return nil
		}
		if changed {
			if _, err := m.store.UpdateWorkflow(ctx, candidate, candidate.Version); errors.Is(err, store.ErrConflict) {
				continue
			} else if err != nil {
				return err
			}
			continue
		}
		if hasActiveStep(candidate) {
			if err := wait(ctx, m.poll); err != nil {
				return err
			}
			continue
		}
		next := readyStep(candidate)
		if next < 0 {
			return m.failWorkflow(ctx, candidate, fmt.Errorf("no sequential step is ready"))
		}
		input, parentRunID, err := resolveSequentialInput(candidate, candidate.Steps[next])
		if err != nil {
			return m.failWorkflow(ctx, candidate, err)
		}
		run := domain.Run{
			ID: newID("run"), AgentID: candidate.Steps[next].AgentID, ParentRunID: parentRunID,
			Input: input, Status: domain.RunQueued, MaxAttempts: m.engine.MaxAttempts(),
			RequestID: "workflow:" + candidate.ID, CreatedAt: time.Now().UTC(),
		}
		created, _, err := m.store.CreateRun(ctx, run, "workflow:"+candidate.ID+":step:"+candidate.Steps[next].ID)
		if err != nil {
			return m.failWorkflow(ctx, candidate, fmt.Errorf("create Run for step %s: %w", candidate.Steps[next].ID, err))
		}
		candidate.Steps[next].RunID = created.ID
		candidate.Steps[next].Status = domain.WorkflowStepQueued
		updated, err := m.store.UpdateWorkflow(ctx, candidate, candidate.Version)
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		if err != nil {
			return err
		}
		step := updated.Steps[next]
		m.emit(ctx, updated.ID, step.ID, step.RunID, "workflow.step_queued", "workflow step queued")
		for {
			if err := m.engine.Enqueue(ctx, created.ID); err == nil {
				break
			}
			if err := wait(ctx, m.poll); err != nil {
				return err
			}
		}
	}
}

func (m *Manager) refreshSteps(ctx context.Context, workflow *domain.Workflow) (changed bool, terminal bool, err error) {
	allSucceeded := true
	for index := range workflow.Steps {
		step := &workflow.Steps[index]
		if step.RunID == "" {
			allSucceeded = false
			continue
		}
		run, loadErr := m.store.GetRun(ctx, step.RunID)
		if loadErr != nil {
			return false, false, loadErr
		}
		previous := step.Status
		switch run.Status {
		case domain.RunQueued:
			step.Status = domain.WorkflowStepQueued
		case domain.RunRunning:
			step.Status, step.StartedAt = domain.WorkflowStepRunning, run.StartedAt
		case domain.RunSucceeded:
			step.Status, step.Output, step.Error = domain.WorkflowStepSucceeded, run.Output, ""
			step.StartedAt, step.CompletedAt = run.StartedAt, run.CompletedAt
		case domain.RunFailed:
			step.Status, step.Error, step.Output = domain.WorkflowStepFailed, run.Error, ""
			step.StartedAt, step.CompletedAt = run.StartedAt, run.CompletedAt
		case domain.RunCanceled:
			step.Status, step.Output = domain.WorkflowStepCanceled, ""
			step.StartedAt, step.CompletedAt = run.StartedAt, run.CompletedAt
		}
		changed = changed || previous != step.Status
		if step.Status != domain.WorkflowStepSucceeded {
			allSucceeded = false
		}
		if step.Status == domain.WorkflowStepFailed || step.Status == domain.WorkflowStepCanceled {
			now := time.Now().UTC()
			if step.Status == domain.WorkflowStepFailed {
				workflow.Status, workflow.Error = domain.WorkflowFailed, fmt.Sprintf("step %s failed: %s", step.ID, step.Error)
			} else {
				workflow.Status = domain.WorkflowCanceled
			}
			workflow.CompletedAt = &now
			cancelPendingSteps(workflow, now)
			return true, true, nil
		}
	}
	if allSucceeded {
		now := time.Now().UTC()
		workflow.Status, workflow.Error, workflow.CompletedAt = domain.WorkflowSucceeded, "", &now
		return true, true, nil
	}
	return changed, false, nil
}

func (m *Manager) failWorkflow(ctx context.Context, workflow domain.Workflow, cause error) error {
	now := time.Now().UTC()
	workflow.Status, workflow.Error, workflow.CompletedAt = domain.WorkflowFailed, cause.Error(), &now
	cancelPendingSteps(&workflow, now)
	if _, err := m.store.UpdateWorkflow(ctx, workflow, workflow.Version); err != nil && !errors.Is(err, store.ErrConflict) {
		return err
	}
	m.emit(ctx, workflow.ID, "", "", "workflow.failed", workflow.Error)
	return cause
}

func (m *Manager) failInvalidRecovered(ctx context.Context, workflow domain.Workflow, cause error) {
	_ = m.failWorkflow(ctx, workflow, fmt.Errorf("%w: %v", ErrNotSequential, cause))
}

func (m *Manager) emit(ctx context.Context, workflowID, stepID, runID, eventType, message string) {
	event := domain.WorkflowEvent{ID: newID("evt"), WorkflowID: workflowID, StepID: stepID, RunID: runID, Type: eventType, Message: message, Timestamp: time.Now().UTC()}
	if err := m.store.AppendWorkflowEvent(ctx, event, eventRetention, eventHistoryLimit); err != nil {
		slog.WarnContext(ctx, "workflow event persistence failed", "workflow_id", workflowID, "type", eventType, "error", err)
	}
}

func hasActiveStep(workflow domain.Workflow) bool {
	for _, step := range workflow.Steps {
		if step.Status == domain.WorkflowStepQueued || step.Status == domain.WorkflowStepRunning {
			return true
		}
	}
	return false
}

func readyStep(workflow domain.Workflow) int {
	statuses := make(map[string]domain.WorkflowStepStatus, len(workflow.Steps))
	for _, step := range workflow.Steps {
		statuses[step.ID] = step.Status
	}
	for index, step := range workflow.Steps {
		if step.Status != domain.WorkflowStepPending {
			continue
		}
		ready := true
		for _, dependency := range step.DependsOn {
			if statuses[dependency] != domain.WorkflowStepSucceeded {
				ready = false
				break
			}
		}
		if ready {
			return index
		}
	}
	return -1
}

func resolveSequentialInput(workflow domain.Workflow, step domain.WorkflowStep) (string, string, error) {
	parentRunID := ""
	if len(step.DependsOn) == 1 {
		dependency := findStep(workflow, step.DependsOn[0])
		if dependency == nil || dependency.Status != domain.WorkflowStepSucceeded {
			return "", "", fmt.Errorf("step %s dependency is not complete", step.ID)
		}
		parentRunID = dependency.RunID
	}
	if len(step.InputFrom) == 0 {
		return step.Input, parentRunID, nil
	}
	if step.InputFrom[0] == domain.WorkflowInputSource {
		return workflow.Input, parentRunID, nil
	}
	source := findStep(workflow, step.InputFrom[0])
	if source == nil || source.Status != domain.WorkflowStepSucceeded {
		return "", "", fmt.Errorf("step %s input source is not complete", step.ID)
	}
	return source.Output, parentRunID, nil
}

func findStep(workflow domain.Workflow, id string) *domain.WorkflowStep {
	for index := range workflow.Steps {
		if workflow.Steps[index].ID == id {
			return &workflow.Steps[index]
		}
	}
	return nil
}

func cancelPendingSteps(workflow *domain.Workflow, at time.Time) {
	for index := range workflow.Steps {
		if workflow.Steps[index].Status == domain.WorkflowStepPending {
			workflow.Steps[index].Status = domain.WorkflowStepCanceled
			workflow.Steps[index].CompletedAt = &at
		}
	}
}

func workflowTerminalEvent(status domain.WorkflowStatus) string {
	switch status {
	case domain.WorkflowSucceeded:
		return "workflow.succeeded"
	case domain.WorkflowCanceled:
		return "workflow.canceled"
	default:
		return "workflow.failed"
	}
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func newID(prefix string) string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(buffer)
}
