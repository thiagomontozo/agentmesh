package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/llm"
)

type completionProvider func(context.Context, llm.CompletionRequest) (llm.CompletionResult, error)

func (f completionProvider) Complete(ctx context.Context, request llm.CompletionRequest) (llm.CompletionResult, error) {
	return f(ctx, request)
}

func TestLLMRuntimeResolvesProviderFromAgentProtocol(t *testing.T) {
	providers := llm.NewRegistry()
	if err := providers.Register(llm.OpenAIProtocol, completionProvider(func(_ context.Context, request llm.CompletionRequest) (llm.CompletionResult, error) {
		if request.RunID != "run_1" || request.Agent.Model != "model-a" || request.Input != "hello" {
			t.Fatalf("unexpected completion request: %+v", request)
		}
		return llm.CompletionResult{Output: "world"}, nil
	})); err != nil {
		t.Fatal(err)
	}
	result, err := NewLLMRuntime(providers).Execute(context.Background(), ExecutionRequest{
		RunID: "run_1", Attempt: 1, Input: "hello",
		Agent: domain.Agent{ID: "agt_1", Runtime: LLMRuntimeName, Protocol: llm.OpenAIProtocol, Model: "model-a"},
	})
	if err != nil || result.Output != "world" {
		t.Fatalf("unexpected result: %+v err=%v", result, err)
	}
}

func TestLLMRuntimeRejectsMissingModelAndUnknownProvider(t *testing.T) {
	runtime := NewLLMRuntime(llm.NewRegistry())
	_, err := runtime.Execute(context.Background(), ExecutionRequest{Agent: domain.Agent{ID: "agt_1", Protocol: "openai"}})
	if err == nil {
		t.Fatal("expected missing model error")
	}
	_, err = runtime.Execute(context.Background(), ExecutionRequest{Agent: domain.Agent{ID: "agt_1", Protocol: "missing", Model: "model-a"}})
	if !errors.Is(err, llm.ErrUnknownProvider) {
		t.Fatalf("expected unknown provider, got %v", err)
	}
}
