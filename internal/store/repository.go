package store

import (
	"context"
	"errors"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

var ErrRunCanceled = errors.New("run was canceled")
var ErrStaleExecution = errors.New("stale run execution fence")
var ErrRunNotExecutable = errors.New("run is not executable")
var ErrConflict = errors.New("concurrent update conflict")
var ErrAgentInUse = errors.New("agent has dependent runs")
var ErrChildRunLimit = errors.New("parent Run child limit reached")

type PendingRun struct {
	ID     string
	Status domain.RunStatus
}

type AgentFilter struct {
	Capability string
	Runtime    string
	Protocol   string
}

type AgentRepository interface {
	CreateAgent(ctx context.Context, agent domain.Agent) (domain.Agent, error)
	GetAgent(ctx context.Context, id string) (domain.Agent, error)
	ListAgents(ctx context.Context) ([]domain.Agent, error)
	ListAgentsByCapability(ctx context.Context, capability string) ([]domain.Agent, error)
	FindAgents(ctx context.Context, filter AgentFilter) ([]domain.Agent, error)
	UpdateAgent(ctx context.Context, agent domain.Agent, expectedVersion int64) (domain.Agent, error)
	DeleteAgent(ctx context.Context, id string, expectedVersion int64) error
}

type RunRepository interface {
	CreateRun(ctx context.Context, run domain.Run, idempotencyKey string) (created domain.Run, isNew bool, err error)
	CreateChildRun(ctx context.Context, run domain.Run, idempotencyKey string, maxChildren int) (created domain.Run, isNew bool, err error)
	GetRun(ctx context.Context, id string) (domain.Run, error)
	GetRunByIdempotencyKey(ctx context.Context, key string) (domain.Run, error)
	UpdateRun(ctx context.Context, run domain.Run) error
	ClaimRunExecution(ctx context.Context, id string, minimumFence int64) (int64, error)
	UpdateRunFenced(ctx context.Context, run domain.Run, fence int64) error
	CancelRun(ctx context.Context, id string, at time.Time) (domain.Run, error)
	ListRuns(ctx context.Context) ([]domain.Run, error)
	ListChildRuns(ctx context.Context, parentRunID string) ([]domain.Run, error)
	CountActiveRunsByAgent(ctx context.Context, agentIDs []string) (map[string]int, error)
	ListPendingRuns(ctx context.Context) ([]PendingRun, error)
	RecoverRun(ctx context.Context, id string, minimumFence int64) (bool, error)
}

type EventRepository interface {
	AppendRunEvent(ctx context.Context, event domain.RunEvent, retention time.Duration, maxPerRun int) error
	ListRunEvents(ctx context.Context, runID string, limit int) ([]domain.RunEvent, error)
}

type WorkflowRepository interface {
	CreateWorkflow(ctx context.Context, workflow domain.Workflow) (domain.Workflow, error)
	GetWorkflow(ctx context.Context, id string) (domain.Workflow, error)
	ListWorkflows(ctx context.Context) ([]domain.Workflow, error)
	UpdateWorkflow(ctx context.Context, workflow domain.Workflow, expectedVersion int64) (domain.Workflow, error)
	ListRunningWorkflows(ctx context.Context) ([]domain.Workflow, error)
	AppendWorkflowEvent(ctx context.Context, event domain.WorkflowEvent, retention time.Duration, maxPerWorkflow int) error
	ListWorkflowEvents(ctx context.Context, workflowID string, limit int) ([]domain.WorkflowEvent, error)
}

type AuditRepository interface {
	AppendAuditEvent(ctx context.Context, event domain.AuditEvent, retention time.Duration, maxEvents int) error
	ListAuditEvents(ctx context.Context, limit int) ([]domain.AuditEvent, error)
}

type Repository interface {
	AgentRepository
	RunRepository
	EventRepository
	WorkflowRepository
	AuditRepository
	Ping(ctx context.Context) error
}
