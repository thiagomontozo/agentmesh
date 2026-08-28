package store

import (
	"context"
	"errors"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

var ErrRunCanceled = errors.New("run was canceled")

type AgentRepository interface {
	CreateAgent(ctx context.Context, agent domain.Agent) (domain.Agent, error)
	GetAgent(ctx context.Context, id string) (domain.Agent, error)
	ListAgents(ctx context.Context) ([]domain.Agent, error)
}

type RunRepository interface {
	CreateRun(ctx context.Context, run domain.Run, idempotencyKey string) (created domain.Run, isNew bool, err error)
	GetRun(ctx context.Context, id string) (domain.Run, error)
	UpdateRun(ctx context.Context, run domain.Run) error
	CancelRun(ctx context.Context, id string, at time.Time) (domain.Run, error)
	ListRuns(ctx context.Context) ([]domain.Run, error)
	RecoverPendingRuns(ctx context.Context) ([]string, error)
}

type Repository interface {
	AgentRepository
	RunRepository
	Ping(ctx context.Context) error
}
