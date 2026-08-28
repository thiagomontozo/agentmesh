package runtime

import (
	"context"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

// ExecutionRequest contains the stable, runtime-facing context for one run attempt.
// Agent.ID is the single source of truth for the agent identifier.
type ExecutionRequest struct {
	RunID   string
	Agent   domain.Agent
	Attempt int
	Input   string
}

func (r ExecutionRequest) AgentID() string {
	return r.Agent.ID
}

type ExecutionResult struct {
	Output string
}

type Runtime interface {
	Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error)
}

// LegacyExecutor matches the existing engine executor contract without importing
// the engine package. This keeps the runtime package independent of orchestration.
type LegacyExecutor interface {
	Execute(ctx context.Context, agent domain.Agent, input string) (string, error)
}

func AdaptLegacy(executor LegacyExecutor) Runtime {
	return legacyAdapter{executor: executor}
}

type legacyAdapter struct {
	executor LegacyExecutor
}

func (a legacyAdapter) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	output, err := a.executor.Execute(ctx, request.Agent, request.Input)
	if err != nil {
		return ExecutionResult{}, err
	}
	return ExecutionResult{Output: output}, nil
}

var _ Runtime = legacyAdapter{}
