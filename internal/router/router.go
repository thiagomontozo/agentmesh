package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/thiagomontozo/agentmesh/internal/agenthealth"
	"github.com/thiagomontozo/agentmesh/internal/discovery"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/observability"
)

var ErrNoCandidate = errors.New("no Agent matches the required capabilities")
var ErrNoCapacity = errors.New("all matching Agents are at routing capacity")
var ErrInvalidRequirements = errors.New("invalid routing requirements")

type ActiveRunCounter interface {
	CountActiveRunsByAgent(ctx context.Context, agentIDs []string) (map[string]int, error)
}

type Decision struct {
	Agent                domain.Agent
	RequiredCapabilities []string
	Health               agenthealth.Status
	Strategy             string
	CandidateCount       int
	ActiveRuns           int
	EffectiveCapacity    int
	Priority             int
}

type Router struct {
	discovery *discovery.Service
	load      ActiveRunCounter
}

func New(service *discovery.Service) *Router {
	return NewWithLoad(service, zeroLoad{})
}

func NewWithLoad(service *discovery.Service, load ActiveRunCounter) *Router {
	if load == nil {
		load = zeroLoad{}
	}
	return &Router{discovery: service, load: load}
}

// Select chooses deterministically among Agents that declare every required
// capability. Healthy Agents are preferred; unknown is the explicit fallback.
// Unhealthy Agents are never eligible.
func (r *Router) Select(ctx context.Context, required []string) (Decision, error) {
	capabilities, err := NormalizeRequirements(required)
	if err != nil {
		return Decision{}, err
	}
	if r == nil || r.discovery == nil {
		return Decision{}, fmt.Errorf("Agent router discovery is required")
	}

	sawSaturated := false
	for _, tier := range []struct {
		health   agenthealth.Status
		strategy string
	}{
		{health: agenthealth.StatusHealthy, strategy: "healthy-load-priority-created-at-id"},
		{health: agenthealth.StatusUnknown, strategy: "unknown-fallback-load-priority-created-at-id"},
	} {
		result, searchErr := r.discovery.Search(ctx, discovery.Query{
			Capability: capabilities[0], Health: tier.health,
		})
		if searchErr != nil {
			return Decision{}, searchErr
		}
		candidates := matchingAll(result.Items, capabilities)
		if len(candidates) == 0 {
			continue
		}
		loads, loadErr := r.load.CountActiveRunsByAgent(ctx, agentIDs(candidates))
		if loadErr != nil {
			return Decision{}, fmt.Errorf("load matching Agents: %w", loadErr)
		}
		eligible := rankAvailable(candidates, loads)
		if len(eligible) == 0 {
			sawSaturated = true
			continue
		}
		selected := eligible[0]
		decision := Decision{
			Agent: selected.agent, RequiredCapabilities: capabilities, Health: tier.health,
			Strategy: tier.strategy, CandidateCount: len(eligible), ActiveRuns: selected.active,
			EffectiveCapacity: selected.capacity, Priority: selected.agent.Priority,
		}
		attributes := append(observability.ContextAttrs(ctx),
			"agent_id", decision.Agent.ID,
			"required_capabilities", decision.RequiredCapabilities,
			"agent_health", decision.Health,
			"routing_strategy", decision.Strategy,
			"candidate_count", decision.CandidateCount,
			"active_runs", decision.ActiveRuns,
			"effective_capacity", decision.EffectiveCapacity,
			"agent_priority", decision.Priority,
		)
		slog.InfoContext(ctx, "agent routing decision", attributes...)
		return decision, nil
	}
	if sawSaturated {
		return Decision{}, ErrNoCapacity
	}
	return Decision{}, ErrNoCandidate
}

type rankedAgent struct {
	agent    domain.Agent
	active   int
	capacity int
}

func rankAvailable(agents []domain.Agent, loads map[string]int) []rankedAgent {
	result := make([]rankedAgent, 0, len(agents))
	for _, agent := range agents {
		capacity := agent.MaxConcurrency
		if capacity == 0 {
			capacity = 1
		}
		active := loads[agent.ID]
		if active >= capacity {
			continue
		}
		result = append(result, rankedAgent{agent: agent, active: active, capacity: capacity})
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		leftLoad := int64(left.active) * int64(right.capacity)
		rightLoad := int64(right.active) * int64(left.capacity)
		if leftLoad != rightLoad {
			return leftLoad < rightLoad
		}
		if left.agent.Priority != right.agent.Priority {
			return left.agent.Priority > right.agent.Priority
		}
		leftRemaining := left.capacity - left.active
		rightRemaining := right.capacity - right.active
		if leftRemaining != rightRemaining {
			return leftRemaining > rightRemaining
		}
		if !left.agent.CreatedAt.Equal(right.agent.CreatedAt) {
			return left.agent.CreatedAt.Before(right.agent.CreatedAt)
		}
		return left.agent.ID < right.agent.ID
	})
	return result
}

func agentIDs(agents []domain.Agent) []string {
	result := make([]string, len(agents))
	for index, agent := range agents {
		result[index] = agent.ID
	}
	return result
}

type zeroLoad struct{}

func (zeroLoad) CountActiveRunsByAgent(_ context.Context, agentIDs []string) (map[string]int, error) {
	return make(map[string]int, len(agentIDs)), nil
}

func NormalizeRequirements(required []string) ([]string, error) {
	if len(required) == 0 {
		return nil, fmt.Errorf("%w: at least one required capability is required", ErrInvalidRequirements)
	}
	result := make([]string, 0, len(required))
	seen := make(map[string]struct{}, len(required))
	for _, capability := range required {
		normalized, err := domain.NormalizeCapability(capability)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidRequirements, err)
		}
		if _, duplicate := seen[normalized]; duplicate {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result, nil
}

func matchingAll(agents []domain.Agent, required []string) []domain.Agent {
	result := make([]domain.Agent, 0, len(agents))
	for _, agent := range agents {
		declared := make(map[string]struct{}, len(agent.Capabilities))
		for _, capability := range agent.Capabilities {
			declared[capability] = struct{}{}
		}
		matches := true
		for _, capability := range required {
			if _, ok := declared[capability]; !ok {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, agent)
		}
	}
	return result
}
