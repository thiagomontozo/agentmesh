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

type AgentRepository interface {
	CreateAgent(ctx context.Context, agent domain.Agent) (domain.Agent, error)
	GetAgent(ctx context.Context, id string) (domain.Agent, error)
	ListAgents(ctx context.Context) ([]domain.Agent, error)
}

type RunRepository interface {
	CreateRun(ctx context.Context, run domain.Run, idempotencyKey string) (created domain.Run, isNew bool, err error)
	GetRun(ctx context.Context, id string) (domain.Run, error)
	UpdateRun(ctx context.Context, run domain.Run) error
	ClaimRunExecution(ctx context.Context, id string, minimumFence int64) (int64, error)
	UpdateRunFenced(ctx context.Context, run domain.Run, fence int64) error
	CancelRun(ctx context.Context, id string, at time.Time) (domain.Run, error)
	ListRuns(ctx context.Context) ([]domain.Run, error)
	RecoverPendingRuns(ctx context.Context) ([]string, error)
}

type Repository interface {
	AgentRepository
	RunRepository
	Ping(ctx context.Context) error
}
