package runtime

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

const DemoRuntime = "demo"

var ErrUnknownRuntime = errors.New("unknown agent runtime")

// Resolver selects the execution implementation for an already-selected Agent.
// It does not select Agents or inspect their capabilities.
type Resolver interface {
	Resolve(agent domain.Agent) (Runtime, error)
}

// Registry is a concurrency-safe runtime resolver. Registering an existing name
// replaces its implementation, which keeps runtime wiring explicit and testable.
type Registry struct {
	mu       sync.RWMutex
	runtimes map[string]Runtime
}

func NewRegistry(demo Runtime) *Registry {
	registry := &Registry{runtimes: make(map[string]Runtime)}
	if demo != nil {
		registry.runtimes[DemoRuntime] = demo
	}
	return registry
}

func (r *Registry) Register(name string, implementation Runtime) error {
	name = normalizeRuntimeName(name)
	if name == "" {
		return fmt.Errorf("runtime name is required")
	}
	if implementation == nil {
		return fmt.Errorf("runtime %q implementation is required", name)
	}

	r.mu.Lock()
	r.runtimes[name] = implementation
	r.mu.Unlock()
	return nil
}

func (r *Registry) Resolve(agent domain.Agent) (Runtime, error) {
	name := normalizeRuntimeName(agent.Runtime)
	if name == "" {
		name = DemoRuntime
	}

	r.mu.RLock()
	implementation, ok := r.runtimes[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownRuntime, name)
	}
	return implementation, nil
}

func normalizeRuntimeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

var _ Resolver = (*Registry)(nil)
