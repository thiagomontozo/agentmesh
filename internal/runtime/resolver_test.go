package runtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
)

type runtimeFunc func(context.Context, agentruntime.ExecutionRequest) (agentruntime.ExecutionResult, error)

func (f runtimeFunc) Execute(ctx context.Context, request agentruntime.ExecutionRequest) (agentruntime.ExecutionResult, error) {
	return f(ctx, request)
}

func TestRegistryResolvesLegacyAgentToDemo(t *testing.T) {
	demo := runtimeFunc(func(context.Context, agentruntime.ExecutionRequest) (agentruntime.ExecutionResult, error) {
		return agentruntime.ExecutionResult{Output: "demo"}, nil
	})
	registry := agentruntime.NewRegistry(demo)

	resolved, err := registry.Resolve(domain.Agent{ID: "agt_legacy"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved == nil {
		t.Fatal("expected demo runtime")
	}
}

func TestRegistryResolvesExplicitDemoRuntime(t *testing.T) {
	demo := runtimeFunc(func(context.Context, agentruntime.ExecutionRequest) (agentruntime.ExecutionResult, error) {
		return agentruntime.ExecutionResult{Output: "demo"}, nil
	})
	registry := agentruntime.NewRegistry(demo)

	resolved, err := registry.Resolve(domain.Agent{ID: "agt_demo", Runtime: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := resolved.Execute(context.Background(), agentruntime.ExecutionRequest{})
	if err != nil || result.Output != "demo" {
		t.Fatalf("unexpected demo result: result=%+v err=%v", result, err)
	}
}

func TestRegistryRejectsUnknownRuntime(t *testing.T) {
	registry := agentruntime.NewRegistry(runtimeFunc(func(context.Context, agentruntime.ExecutionRequest) (agentruntime.ExecutionResult, error) {
		return agentruntime.ExecutionResult{}, nil
	}))

	_, err := registry.Resolve(domain.Agent{ID: "agt_remote", Runtime: "remote-http"})
	if !errors.Is(err, agentruntime.ErrUnknownRuntime) {
		t.Fatalf("expected ErrUnknownRuntime, got %v", err)
	}
}

func TestRegistrySupportsConcurrentRegistrationAndResolution(t *testing.T) {
	registry := agentruntime.NewRegistry(nil)
	implementation := runtimeFunc(func(context.Context, agentruntime.ExecutionRequest) (agentruntime.ExecutionResult, error) {
		return agentruntime.ExecutionResult{}, nil
	})

	var wg sync.WaitGroup
	for index := 0; index < 32; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := registry.Register("custom", implementation); err != nil {
				t.Errorf("register runtime: %v", err)
				return
			}
			if _, err := registry.Resolve(domain.Agent{Runtime: "custom"}); err != nil {
				t.Errorf("resolve runtime: %v", err)
			}
		}()
	}
	wg.Wait()
}
