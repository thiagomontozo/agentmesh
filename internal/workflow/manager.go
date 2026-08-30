package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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

type Manager struct {
	store       store.Repository
	engine      *engine.Engine
	poll        time.Duration
	concurrency int

	mu     sync.Mutex
	ctx    context.Context
	active map[string]struct{}
	wg     sync.WaitGroup
}

func New(repository store.Repository, runEngine *engine.Engine) *Manager {
	return NewWithConcurrency(repository, runEngine, 4)
}

func NewWithConcurrency(repository store.Repository, runEngine *engine.Engine, concurrency int) *Manager {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Manager{store: repository, engine: runEngine, poll: defaultPollInterval, concurrency: concurrency, active: make(map[string]struct{})}
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
		m.schedule(candidate.ID)
	}
	return nil
}

func (m *Manager) StartWorkflow(ctx context.Context, id string) (domain.Workflow, error) {
	candidate, err := m.store.GetWorkflow(ctx, id)
	if err != nil {
		return domain.Workflow{}, err
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
		previousStatuses := stepStatuses(candidate)
		changed, terminal, err := m.refreshSteps(ctx, &candidate)
		if err != nil {
			return err
		}
		if terminal {
			updated, err := m.store.UpdateWorkflow(ctx, candidate, candidate.Version)
			if errors.Is(err, store.ErrConflict) {
				continue
			} else if err != nil {
				return err
			}
			m.cancelCanceledStepRuns(ctx, updated)
			m.emitStepTransitions(ctx, updated, previousStatuses)
			m.emit(ctx, candidate.ID, "", "", workflowTerminalEvent(candidate.Status), candidate.Error)
			return nil
		}
		if changed {
			updated, err := m.store.UpdateWorkflow(ctx, candidate, candidate.Version)
			if errors.Is(err, store.ErrConflict) {
				continue
			} else if err != nil {
				return err
			}
			m.emitStepTransitions(ctx, updated, previousStatuses)
			continue
		}
		if applyConditions(&candidate) {
			updated, err := m.store.UpdateWorkflow(ctx, candidate, candidate.Version)
			if errors.Is(err, store.ErrConflict) {
				continue
			} else if err != nil {
				return err
			}
			m.emitStepTransitions(ctx, updated, previousStatuses)
			continue
		}
		if activeStepCount(candidate) >= m.concurrency {
			if err := wait(ctx, m.poll); err != nil {
				return err
			}
			continue
		}
		next := readyStep(candidate)
		if next < 0 {
			if activeStepCount(candidate) > 0 {
				if err := wait(ctx, m.poll); err != nil {
					return err
				}
				continue
			}
			return m.failWorkflow(ctx, candidate, fmt.Errorf("no workflow step is ready"))
		}
		input, parentRunID, err := resolveInput(candidate, candidate.Steps[next])
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
		if step.Status == domain.WorkflowStepSkipped {
			continue
		}
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
		if step.Status != domain.WorkflowStepSucceeded && step.Status != domain.WorkflowStepSkipped {
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
			cancelNonterminalSteps(workflow, now)
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
	cancelNonterminalSteps(&workflow, now)
	if _, err := m.store.UpdateWorkflow(ctx, workflow, workflow.Version); err != nil && !errors.Is(err, store.ErrConflict) {
		return err
	}
	m.emit(ctx, workflow.ID, "", "", "workflow.failed", workflow.Error)
	return cause
}

func (m *Manager) emit(ctx context.Context, workflowID, stepID, runID, eventType, message string) {
	event := domain.WorkflowEvent{ID: newID("evt"), WorkflowID: workflowID, StepID: stepID, RunID: runID, Type: eventType, Message: message, Timestamp: time.Now().UTC()}
	if err := m.store.AppendWorkflowEvent(ctx, event, eventRetention, eventHistoryLimit); err != nil {
		slog.WarnContext(ctx, "workflow event persistence failed", "workflow_id", workflowID, "type", eventType, "error", err)
	}
}

func (m *Manager) emitStepTransitions(ctx context.Context, workflow domain.Workflow, previous map[string]domain.WorkflowStepStatus) {
	for _, step := range workflow.Steps {
		if previous[step.ID] == step.Status {
			continue
		}
		eventType := "workflow.step_" + string(step.Status)
		message := fmt.Sprintf("workflow step %s", step.Status)
		m.emit(ctx, workflow.ID, step.ID, step.RunID, eventType, message)
	}
}

func stepStatuses(workflow domain.Workflow) map[string]domain.WorkflowStepStatus {
	result := make(map[string]domain.WorkflowStepStatus, len(workflow.Steps))
	for _, step := range workflow.Steps {
		result[step.ID] = step.Status
	}
	return result
}

func activeStepCount(workflow domain.Workflow) int {
	count := 0
	for _, step := range workflow.Steps {
		if step.Status == domain.WorkflowStepQueued || step.Status == domain.WorkflowStepRunning {
			count++
		}
	}
	return count
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
			if statuses[dependency] != domain.WorkflowStepSucceeded && statuses[dependency] != domain.WorkflowStepSkipped {
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

func resolveInput(workflow domain.Workflow, step domain.WorkflowStep) (string, string, error) {
	parentRunID := ""
	if len(step.DependsOn) > 0 {
		for _, dependencyID := range step.DependsOn {
			dependency := findStep(workflow, dependencyID)
			if dependency == nil || (dependency.Status != domain.WorkflowStepSucceeded && dependency.Status != domain.WorkflowStepSkipped) {
				return "", "", fmt.Errorf("step %s dependency is not complete", step.ID)
			}
			if parentRunID == "" && dependency.RunID != "" {
				parentRunID = dependency.RunID
			}
		}
	}
	if len(step.InputFrom) == 0 {
		return step.Input, parentRunID, nil
	}
	values := make([]string, 0, len(step.InputFrom))
	for _, sourceID := range step.InputFrom {
		if sourceID == domain.WorkflowInputSource {
			values = append(values, workflow.Input)
			continue
		}
		source := findStep(workflow, sourceID)
		if source == nil || (source.Status != domain.WorkflowStepSucceeded && source.Status != domain.WorkflowStepSkipped) {
			return "", "", fmt.Errorf("step %s input source %s is not complete", step.ID, sourceID)
		}
		values = append(values, source.Output)
	}
	if len(values) == 1 {
		return values[0], parentRunID, nil
	}
	if step.InputAggregation != domain.WorkflowInputJSONArray {
		return "", "", fmt.Errorf("step %s requires json-array aggregation", step.ID)
	}
	aggregated, err := json.Marshal(values)
	if err != nil {
		return "", "", fmt.Errorf("aggregate step %s input: %w", step.ID, err)
	}
	return string(aggregated), parentRunID, nil
}

func applyConditions(workflow *domain.Workflow) bool {
	statuses := stepStatuses(*workflow)
	changed := false
	now := time.Now().UTC()
	for index := range workflow.Steps {
		step := &workflow.Steps[index]
		if step.Status != domain.WorkflowStepPending || step.Condition == nil {
			continue
		}
		dependenciesComplete := true
		for _, dependency := range step.DependsOn {
			status := statuses[dependency]
			if status != domain.WorkflowStepSucceeded && status != domain.WorkflowStepSkipped {
				dependenciesComplete = false
				break
			}
		}
		if !dependenciesComplete {
			continue
		}
		actual := workflow.Input
		if step.Condition.Source != domain.WorkflowInputSource {
			source := findStep(*workflow, step.Condition.Source)
			if source == nil {
				continue
			}
			actual = source.Output
		}
		if evaluateCondition(actual, *step.Condition) {
			continue
		}
		step.Status = domain.WorkflowStepSkipped
		step.CompletedAt = &now
		changed = true
		statuses[step.ID] = step.Status
	}
	return changed
}

func evaluateCondition(actual string, condition domain.WorkflowCondition) bool {
	switch condition.Operator {
	case domain.WorkflowConditionEquals:
		return actual == condition.Value
	case domain.WorkflowConditionNotEquals:
		return actual != condition.Value
	case domain.WorkflowConditionContains:
		return strings.Contains(actual, condition.Value)
	case domain.WorkflowConditionNotContains:
		return !strings.Contains(actual, condition.Value)
	default:
		return false
	}
}

func findStep(workflow domain.Workflow, id string) *domain.WorkflowStep {
	for index := range workflow.Steps {
		if workflow.Steps[index].ID == id {
			return &workflow.Steps[index]
		}
	}
	return nil
}

func cancelNonterminalSteps(workflow *domain.Workflow, at time.Time) {
	for index := range workflow.Steps {
		if workflow.Steps[index].Status == domain.WorkflowStepPending || workflow.Steps[index].Status == domain.WorkflowStepQueued || workflow.Steps[index].Status == domain.WorkflowStepRunning {
			workflow.Steps[index].Status = domain.WorkflowStepCanceled
			workflow.Steps[index].CompletedAt = &at
		}
	}
}

func (m *Manager) cancelCanceledStepRuns(ctx context.Context, workflow domain.Workflow) {
	for _, step := range workflow.Steps {
		if step.Status != domain.WorkflowStepCanceled || step.RunID == "" {
			continue
		}
		if _, err := m.engine.Cancel(ctx, step.RunID); err != nil && !errors.Is(err, domain.ErrRunNotCancelable) {
			slog.WarnContext(ctx, "fan-out Run cancellation failed", "workflow_id", workflow.ID, "step_id", step.ID, "run_id", step.RunID, "error", err)
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
