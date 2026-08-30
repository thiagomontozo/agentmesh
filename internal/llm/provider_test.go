package llm

import (
	"context"
	"errors"
	"testing"
)

type stubProvider struct{}

func (stubProvider) Complete(context.Context, CompletionRequest) (CompletionResult, error) {
	return CompletionResult{Output: "ok"}, nil
}

func TestRegistryResolvesProviders(t *testing.T) {
	registry := NewRegistry()
	provider := stubProvider{}
	if err := registry.Register(" OPENAI ", provider); err != nil {
		t.Fatal(err)
	}
	resolved, err := registry.Resolve("openai")
	if err != nil {
		t.Fatal(err)
	}
	result, err := resolved.Complete(context.Background(), CompletionRequest{})
	if err != nil || result.Output != "ok" {
		t.Fatalf("unexpected provider result: result=%+v err=%v", result, err)
	}
}

func TestRegistryRejectsUnknownProvider(t *testing.T) {
	_, err := NewRegistry().Resolve("missing")
	if !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("expected ErrUnknownProvider, got %v", err)
	}
}
