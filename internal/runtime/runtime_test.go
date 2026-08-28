package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
)

type legacyExecutorFunc func(context.Context, domain.Agent, string) (string, error)

func (f legacyExecutorFunc) Execute(ctx context.Context, agent domain.Agent, input string) (string, error) {
	return f(ctx, agent, input)
}

func TestExecutionRequestCarriesRunContext(t *testing.T) {
	request := agentruntime.ExecutionRequest{
		RunID: "run_1", Agent: domain.Agent{ID: "agt_1", Name: "test"}, Attempt: 2, Input: "hello",
	}
	if request.RunID != "run_1" || request.AgentID() != "agt_1" || request.Attempt != 2 || request.Input != "hello" {
		t.Fatalf("unexpected execution request: %+v", request)
	}
}

func TestLegacyAdapterExecutesDemoExecutor(t *testing.T) {
	runtime := agentruntime.AdaptLegacy(engine.DemoExecutor{})
	result, err := runtime.Execute(context.Background(), agentruntime.ExecutionRequest{
		RunID: "run_1", Agent: domain.Agent{ID: "agt_1", Name: "demo"}, Attempt: 1, Input: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != `Agent "demo" processed: hello` {
		t.Fatalf("unexpected output: %q", result.Output)
	}
}

func TestLegacyAdapterPropagatesErrors(t *testing.T) {
	wantErr := errors.New("executor failed")
	runtime := agentruntime.AdaptLegacy(legacyExecutorFunc(func(context.Context, domain.Agent, string) (string, error) {
		return "", wantErr
	}))
	result, err := runtime.Execute(context.Background(), agentruntime.ExecutionRequest{Agent: domain.Agent{ID: "agt_1"}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
	if result.Output != "" {
		t.Fatalf("expected empty result on failure, got %+v", result)
	}
}

func TestLegacyAdapterPropagatesContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runtime := agentruntime.AdaptLegacy(engine.DemoExecutor{Delay: time.Hour})
	_, err := runtime.Execute(ctx, agentruntime.ExecutionRequest{
		RunID: "run_1", Agent: domain.Agent{ID: "agt_1", Name: "demo"}, Attempt: 1, Input: "hello",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}
