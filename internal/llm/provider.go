package llm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

const OpenAIProtocol = "openai"

var ErrUnknownProvider = errors.New("unknown LLM provider")

type CompletionRequest struct {
	RunID   string
	Agent   domain.Agent
	Attempt int
	Input   string
}

type Usage struct {
	InputTokens  int `json:"input_tokens,omitempty"`
	OutputTokens int `json:"output_tokens,omitempty"`
	TotalTokens  int `json:"total_tokens,omitempty"`
}

type CompletionResult struct {
	Output string
	Usage  Usage
}

type Provider interface {
	Complete(context.Context, CompletionRequest) (CompletionResult, error)
}

type Resolver interface {
	Resolve(protocol string) (Provider, error)
}

type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

func (r *Registry) Register(protocol string, provider Provider) error {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if protocol == "" {
		return fmt.Errorf("LLM provider protocol is required")
	}
	if provider == nil {
		return fmt.Errorf("LLM provider for %q is required", protocol)
	}
	r.mu.Lock()
	r.providers[protocol] = provider
	r.mu.Unlock()
	return nil
}

func (r *Registry) Resolve(protocol string) (Provider, error) {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	r.mu.RLock()
	provider, ok := r.providers[protocol]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownProvider, protocol)
	}
	return provider, nil
}

var _ Resolver = (*Registry)(nil)
