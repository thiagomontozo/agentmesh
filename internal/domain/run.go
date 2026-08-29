package domain

import (
	"errors"
	"fmt"
	"time"
)

type RunStatus string

const (
	RunQueued    RunStatus = "queued"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCanceled  RunStatus = "canceled"
)

var ErrRunNotCancelable = errors.New("run cannot be canceled")

type Run struct {
	ID                   string     `json:"id"`
	AgentID              string     `json:"agent_id"`
	RequiredCapabilities []string   `json:"required_capabilities,omitempty"`
	Input                string     `json:"input"`
	Output               string     `json:"output,omitempty"`
	Status               RunStatus  `json:"status"`
	Error                string     `json:"error,omitempty"`
	Attempt              int        `json:"attempt"`
	MaxAttempts          int        `json:"max_attempts"`
	RequestID            string     `json:"request_id,omitempty"`
	DurationMS           int64      `json:"duration_ms"`
	CreatedAt            time.Time  `json:"created_at"`
	StartedAt            *time.Time `json:"started_at,omitempty"`
	CompletedAt          *time.Time `json:"completed_at,omitempty"`
}

func (r *Run) Start(at time.Time) error {
	if r.Status != RunQueued {
		return fmt.Errorf("cannot transition run from %s to %s", r.Status, RunRunning)
	}
	if r.MaxAttempts > 0 && r.Attempt >= r.MaxAttempts {
		return fmt.Errorf("run has exhausted %d attempts", r.MaxAttempts)
	}
	at = at.UTC()
	r.Status = RunRunning
	r.StartedAt = &at
	r.Attempt++
	return nil
}

func (r *Run) Retry() error {
	if r.Status != RunRunning {
		return fmt.Errorf("cannot retry run in status %s", r.Status)
	}
	if r.MaxAttempts > 0 && r.Attempt >= r.MaxAttempts {
		return fmt.Errorf("run has exhausted %d attempts", r.MaxAttempts)
	}
	r.Attempt++
	return nil
}

func (r *Run) Succeed(output string, at time.Time) error {
	if r.Status != RunRunning {
		return fmt.Errorf("cannot transition run from %s to %s", r.Status, RunSucceeded)
	}
	at = at.UTC()
	r.Status = RunSucceeded
	r.Output = output
	r.Error = ""
	r.CompletedAt = &at
	r.DurationMS = r.durationMilliseconds(at)
	return nil
}

func (r *Run) Fail(err error, at time.Time) error {
	if r.Status != RunQueued && r.Status != RunRunning {
		return fmt.Errorf("cannot transition run from %s to %s", r.Status, RunFailed)
	}
	if err == nil {
		return fmt.Errorf("cannot fail run without an error")
	}
	at = at.UTC()
	r.Status = RunFailed
	r.Output = ""
	r.Error = err.Error()
	r.CompletedAt = &at
	r.DurationMS = r.durationMilliseconds(at)
	return nil
}

func (r *Run) Cancel(at time.Time) error {
	if r.Status != RunQueued && r.Status != RunRunning {
		return fmt.Errorf("%w from status %s", ErrRunNotCancelable, r.Status)
	}
	at = at.UTC()
	r.Status = RunCanceled
	r.Output = ""
	r.Error = ""
	r.CompletedAt = &at
	r.DurationMS = r.durationMilliseconds(at)
	return nil
}

func (r Run) durationMilliseconds(completedAt time.Time) int64 {
	startedAt := r.CreatedAt
	if r.StartedAt != nil {
		startedAt = *r.StartedAt
	}
	if startedAt.IsZero() || completedAt.Before(startedAt) {
		return 0
	}
	return completedAt.Sub(startedAt).Milliseconds()
}

type RunEvent struct {
	ID        string    `json:"event_id"`
	RunID     string    `json:"run_id"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	Attempt   int       `json:"attempt,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}
