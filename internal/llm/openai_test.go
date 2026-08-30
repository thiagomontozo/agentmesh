package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

type authFunc func(context.Context, domain.Agent, *http.Request) error

func (f authFunc) Authenticate(ctx context.Context, agent domain.Agent, request *http.Request) error {
	return f(ctx, agent, request)
}

func TestOpenAICompatibleCompletesWithSystemPromptAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer test" {
			t.Fatalf("unexpected request: path=%s authorization=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		var request chatRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.Model != "model-a" || len(request.Messages) != 2 || request.Messages[0].Role != "system" || request.Messages[1].Content != "hello" {
			t.Fatalf("unexpected chat request: %+v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"world"}}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`))
	}))
	defer server.Close()
	provider := NewOpenAICompatible(server.Client(), 1024, 1024, authFunc(func(_ context.Context, _ domain.Agent, request *http.Request) error {
		request.Header.Set("Authorization", "Bearer test")
		return nil
	}))
	result, err := provider.Complete(context.Background(), CompletionRequest{
		RunID: "run_1", Attempt: 1, Input: "hello",
		Agent: domain.Agent{ID: "agt_1", Endpoint: server.URL, Model: "model-a", SystemPrompt: "be concise"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "world" || result.Usage.TotalTokens != 6 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestOpenAICompatibleClassifiesProviderErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"slow down","code":"rate_limit"}}`))
	}))
	defer server.Close()
	_, err := NewOpenAICompatible(server.Client(), 1024, 1024, nil).Complete(context.Background(), CompletionRequest{
		RunID: "run_1", Attempt: 1, Input: "hello", Agent: domain.Agent{ID: "agt_1", Endpoint: server.URL + "/v1", Model: "model-a"},
	})
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || !providerErr.Retryable() || providerErr.Code != "rate_limit" {
		t.Fatalf("unexpected error: %#v", err)
	}
}

func TestOpenAICompatibleTreatsPlainServerFailureAsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream unavailable"))
	}))
	defer server.Close()
	_, err := NewOpenAICompatible(server.Client(), 1024, 1024, nil).Complete(context.Background(), CompletionRequest{
		RunID: "run_1", Attempt: 1, Input: "hello", Agent: domain.Agent{ID: "agt_1", Endpoint: server.URL, Model: "model-a"},
	})
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || !providerErr.Retryable() || providerErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("unexpected error: %#v", err)
	}
}

func TestOpenAICompatibleRejectsInvalidResponses(t *testing.T) {
	tests := []struct {
		name    string
		handler http.Handler
		limit   int64
	}{
		{name: "invalid json", limit: 1024, handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{"))
		})},
		{name: "empty completion", limit: 1024, handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"choices":[]}`))
		})},
		{name: "oversized", limit: 32, handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"` + strings.Repeat("x", 64) + `"}}]}`))
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			_, err := NewOpenAICompatible(server.Client(), 1024, test.limit, nil).Complete(context.Background(), CompletionRequest{
				RunID: "run_1", Attempt: 1, Input: "hello", Agent: domain.Agent{ID: "agt_1", Endpoint: server.URL, Model: "model-a"},
			})
			if err == nil {
				t.Fatal("expected response validation error")
			}
		})
	}
}

func TestOpenAICompatibleRejectsOversizedRequestBeforeNetworkCall(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer server.Close()
	_, err := NewOpenAICompatible(server.Client(), 32, 1024, nil).Complete(context.Background(), CompletionRequest{
		RunID: "run_1", Attempt: 1, Input: strings.Repeat("x", 64), Agent: domain.Agent{ID: "agt_1", Endpoint: server.URL, Model: "model-a"},
	})
	if err == nil || called {
		t.Fatalf("oversized request was not rejected before network call: called=%v err=%v", called, err)
	}
}

func TestOpenAICompatibleHonorsContextTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"late"}}]}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := NewOpenAICompatible(server.Client(), 1024, 1024, nil).Complete(ctx, CompletionRequest{
		RunID: "run_1", Attempt: 1, Input: "hello", Agent: domain.Agent{ID: "agt_1", Endpoint: server.URL, Model: "model-a"},
	})
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || !providerErr.Retryable() || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected timeout error: %v", err)
	}
}
