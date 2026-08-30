package runtime

import (
	"context"
	"fmt"
	"strings"

	"github.com/thiagomontozo/agentmesh/internal/llm"
)

const LLMRuntimeName = "llm"

type LLMRuntime struct {
	providers llm.Resolver
}

func NewLLMRuntime(providers llm.Resolver) *LLMRuntime {
	return &LLMRuntime{providers: providers}
}

func (r *LLMRuntime) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	if r == nil || r.providers == nil {
		return ExecutionResult{}, fmt.Errorf("LLM provider resolver is unavailable")
	}
	if strings.TrimSpace(request.Agent.Model) == "" {
		return ExecutionResult{}, fmt.Errorf("LLM Agent %s requires model", request.Agent.ID)
	}
	provider, err := r.providers.Resolve(request.Agent.Protocol)
	if err != nil {
		return ExecutionResult{}, err
	}
	result, err := provider.Complete(ctx, llm.CompletionRequest{
		RunID: request.RunID, Agent: request.Agent, Attempt: request.Attempt, Input: request.Input,
	})
	if err != nil {
		return ExecutionResult{}, err
	}
	return ExecutionResult{Output: result.Output}, nil
}

var _ Runtime = (*LLMRuntime)(nil)
