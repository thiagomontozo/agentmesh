package domain

import "time"

type RunStatus string

const (
	RunQueued    RunStatus = "queued"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
)

type Run struct {
	ID          string     `json:"id"`
	AgentID     string     `json:"agent_id"`
	Input       string     `json:"input"`
	Output      string     `json:"output,omitempty"`
	Status      RunStatus  `json:"status"`
	Error       string     `json:"error,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type RunEvent struct {
	RunID     string    `json:"run_id"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}
