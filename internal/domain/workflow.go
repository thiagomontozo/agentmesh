package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var ErrWorkflowNotCancelable = errors.New("workflow cannot be canceled")

type WorkflowStatus string

const (
	WorkflowPending   WorkflowStatus = "pending"
	WorkflowRunning   WorkflowStatus = "running"
	WorkflowSucceeded WorkflowStatus = "succeeded"
	WorkflowFailed    WorkflowStatus = "failed"
	WorkflowCanceled  WorkflowStatus = "canceled"
)

type WorkflowStepStatus string

const (
	WorkflowStepPending   WorkflowStepStatus = "pending"
	WorkflowStepQueued    WorkflowStepStatus = "queued"
	WorkflowStepRunning   WorkflowStepStatus = "running"
	WorkflowStepSucceeded WorkflowStepStatus = "succeeded"
	WorkflowStepSkipped   WorkflowStepStatus = "skipped"
	WorkflowStepFailed    WorkflowStepStatus = "failed"
	WorkflowStepCanceled  WorkflowStepStatus = "canceled"
)

const (
	WorkflowInputSource          = "workflow"
	WorkflowInputSingle          = "single"
	WorkflowInputJSONArray       = "json-array"
	MaxWorkflowSteps             = 100
	WorkflowConditionEquals      = "equals"
	WorkflowConditionNotEquals   = "not-equals"
	WorkflowConditionContains    = "contains"
	WorkflowConditionNotContains = "not-contains"
)

var workflowStepIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// Workflow is a persisted DAG definition. Execution state fields are included
// now so later lifecycle changes do not require a second representation.
type Workflow struct {
	ID          string         `json:"id"`
	Input       string         `json:"input,omitempty"`
	Status      WorkflowStatus `json:"status"`
	Steps       []WorkflowStep `json:"steps"`
	Error       string         `json:"error,omitempty"`
	Version     int64          `json:"version"`
	CreatedAt   time.Time      `json:"created_at"`
	StartedAt   *time.Time     `json:"started_at,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
}

type WorkflowStep struct {
	ID               string             `json:"id"`
	WorkflowID       string             `json:"workflow_id"`
	AgentID          string             `json:"agent_id"`
	Input            string             `json:"input,omitempty"`
	InputFrom        []string           `json:"input_from,omitempty"`
	InputAggregation string             `json:"input_aggregation,omitempty"`
	DependsOn        []string           `json:"depends_on,omitempty"`
	Condition        *WorkflowCondition `json:"condition,omitempty"`
	Status           WorkflowStepStatus `json:"status"`
	RunID            string             `json:"run_id,omitempty"`
	Output           string             `json:"output,omitempty"`
	Error            string             `json:"error,omitempty"`
	CreatedAt        time.Time          `json:"created_at"`
	StartedAt        *time.Time         `json:"started_at,omitempty"`
	CompletedAt      *time.Time         `json:"completed_at,omitempty"`
}

type WorkflowCondition struct {
	Source   string `json:"source"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type WorkflowEvent struct {
	ID         string    `json:"event_id"`
	WorkflowID string    `json:"workflow_id"`
	StepID     string    `json:"step_id,omitempty"`
	RunID      string    `json:"run_id,omitempty"`
	Type       string    `json:"type"`
	Message    string    `json:"message"`
	Timestamp  time.Time `json:"timestamp"`
}

func (w Workflow) IsTerminal() bool {
	return w.Status == WorkflowSucceeded || w.Status == WorkflowFailed || w.Status == WorkflowCanceled
}

func (w Workflow) ValidateSequential() error {
	if len(w.Steps) == 0 {
		return fmt.Errorf("workflow has no steps")
	}
	dependents := make(map[string]int, len(w.Steps))
	roots := 0
	for _, step := range w.Steps {
		if len(step.DependsOn) > 1 {
			return fmt.Errorf("step %q has multiple dependencies; fan-in is not supported by sequential execution", step.ID)
		}
		if len(step.InputFrom) > 1 {
			return fmt.Errorf("step %q has multiple input sources; fan-in is not supported by sequential execution", step.ID)
		}
		if len(step.DependsOn) == 0 {
			roots++
			continue
		}
		dependency := step.DependsOn[0]
		dependents[dependency]++
		if dependents[dependency] > 1 {
			return fmt.Errorf("step %q has multiple dependents; fan-out is not supported by sequential execution", dependency)
		}
	}
	if roots != 1 {
		return fmt.Errorf("sequential workflow requires exactly one root step")
	}
	return nil
}

func (w *Workflow) InitializeForCreate(at time.Time) error {
	w.ID = strings.TrimSpace(w.ID)
	if w.ID == "" {
		return fmt.Errorf("workflow id is required")
	}
	if len(w.Steps) == 0 {
		return fmt.Errorf("workflow must contain at least one step")
	}
	if len(w.Steps) > MaxWorkflowSteps {
		return fmt.Errorf("workflow cannot contain more than %d steps", MaxWorkflowSteps)
	}
	at = at.UTC()
	w.Status = WorkflowPending
	w.Error = ""
	w.Version = 1
	w.CreatedAt = at
	w.StartedAt = nil
	w.CompletedAt = nil

	ids := make(map[string]int, len(w.Steps))
	for index := range w.Steps {
		step := &w.Steps[index]
		step.ID = strings.ToLower(strings.TrimSpace(step.ID))
		step.AgentID = strings.TrimSpace(step.AgentID)
		if !workflowStepIDPattern.MatchString(step.ID) {
			return fmt.Errorf("step %d has invalid id %q", index, step.ID)
		}
		if _, exists := ids[step.ID]; exists {
			return fmt.Errorf("duplicate workflow step id %q", step.ID)
		}
		if step.AgentID == "" {
			return fmt.Errorf("step %q requires agent_id", step.ID)
		}
		ids[step.ID] = index
		step.WorkflowID = w.ID
		step.Status = WorkflowStepPending
		step.RunID, step.Output, step.Error = "", "", ""
		step.CreatedAt = at
		step.StartedAt, step.CompletedAt = nil, nil
		step.DependsOn = normalizeWorkflowRefs(step.DependsOn)
		step.InputFrom = normalizeWorkflowRefs(step.InputFrom)
		step.InputAggregation = strings.ToLower(strings.TrimSpace(step.InputAggregation))
		if step.Condition != nil {
			step.Condition.Source = strings.ToLower(strings.TrimSpace(step.Condition.Source))
			step.Condition.Operator = strings.ToLower(strings.TrimSpace(step.Condition.Operator))
		}
	}

	for index := range w.Steps {
		if err := w.validateStep(index, ids); err != nil {
			return err
		}
	}
	if err := validateWorkflowDAG(w.Steps, ids); err != nil {
		return err
	}
	return nil
}

func (w Workflow) validateStep(index int, ids map[string]int) error {
	step := w.Steps[index]
	dependencies := make(map[string]struct{}, len(step.DependsOn))
	for _, dependency := range step.DependsOn {
		if dependency == step.ID {
			return fmt.Errorf("step %q cannot depend on itself", step.ID)
		}
		if _, exists := ids[dependency]; !exists {
			return fmt.Errorf("step %q depends on unknown step %q", step.ID, dependency)
		}
		dependencies[dependency] = struct{}{}
	}
	if step.Condition != nil {
		if step.Condition.Source == "" {
			return fmt.Errorf("step %q condition requires source", step.ID)
		}
		if step.Condition.Source != WorkflowInputSource {
			if _, exists := ids[step.Condition.Source]; !exists {
				return fmt.Errorf("step %q condition has unknown source %q", step.ID, step.Condition.Source)
			}
			if _, isDependency := dependencies[step.Condition.Source]; !isDependency {
				return fmt.Errorf("step %q condition source %q must be a dependency", step.ID, step.Condition.Source)
			}
		}
		switch step.Condition.Operator {
		case WorkflowConditionEquals, WorkflowConditionNotEquals, WorkflowConditionContains, WorkflowConditionNotContains:
		default:
			return fmt.Errorf("step %q has unsupported condition operator %q", step.ID, step.Condition.Operator)
		}
	}
	if len(step.InputFrom) == 0 {
		if strings.TrimSpace(step.Input) == "" {
			return fmt.Errorf("step %q requires input or input_from", step.ID)
		}
		if step.InputAggregation != "" {
			return fmt.Errorf("step %q cannot aggregate literal input", step.ID)
		}
		return nil
	}
	if strings.TrimSpace(step.Input) != "" {
		return fmt.Errorf("step %q cannot declare both input and input_from", step.ID)
	}
	for _, source := range step.InputFrom {
		if source == WorkflowInputSource {
			if strings.TrimSpace(w.Input) == "" {
				return fmt.Errorf("step %q references empty workflow input", step.ID)
			}
			continue
		}
		if _, exists := ids[source]; !exists {
			return fmt.Errorf("step %q has unknown input source %q", step.ID, source)
		}
		if _, isDependency := dependencies[source]; !isDependency {
			return fmt.Errorf("step %q input source %q must be a dependency", step.ID, source)
		}
	}
	if len(step.InputFrom) == 1 {
		if step.InputAggregation == "" {
			w.Steps[index].InputAggregation = WorkflowInputSingle
		} else if step.InputAggregation != WorkflowInputSingle {
			return fmt.Errorf("step %q requires single input aggregation", step.ID)
		}
		return nil
	}
	if step.InputAggregation != WorkflowInputJSONArray {
		return fmt.Errorf("step %q with multiple input sources requires json-array aggregation", step.ID)
	}
	return nil
}

func normalizeWorkflowRefs(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func validateWorkflowDAG(steps []WorkflowStep, ids map[string]int) error {
	indegree := make([]int, len(steps))
	dependents := make([][]int, len(steps))
	for index, step := range steps {
		indegree[index] = len(step.DependsOn)
		for _, dependency := range step.DependsOn {
			parent := ids[dependency]
			dependents[parent] = append(dependents[parent], index)
		}
	}
	queue := make([]int, 0, len(steps))
	for index, degree := range indegree {
		if degree == 0 {
			queue = append(queue, index)
		}
	}
	visited := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		visited++
		for _, dependent := range dependents[current] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}
	if visited != len(steps) {
		return fmt.Errorf("workflow dependencies contain a cycle")
	}
	return nil
}
