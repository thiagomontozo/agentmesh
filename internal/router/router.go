package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/thiagomontozo/agentmesh/internal/agenthealth"
	"github.com/thiagomontozo/agentmesh/internal/discovery"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/observability"
)

var ErrNoCandidate = errors.New("no Agent matches the required capabilities")
var ErrInvalidRequirements = errors.New("invalid routing requirements")

type Decision struct {
	Agent                domain.Agent
	RequiredCapabilities []string
	Health               agenthealth.Status
	Strategy             string
	CandidateCount       int
}

type Router struct {
	discovery *discovery.Service
}

func New(service *discovery.Service) *Router {
	return &Router{discovery: service}
}

// Select chooses deterministically among Agents that declare every required
// capability. Healthy Agents are preferred; unknown is the explicit fallback.
// Unhealthy Agents are never eligible.
func (r *Router) Select(ctx context.Context, required []string) (Decision, error) {
	capabilities, err := normalizeRequirements(required)
	if err != nil {
		return Decision{}, err
	}
	if r == nil || r.discovery == nil {
		return Decision{}, fmt.Errorf("Agent router discovery is required")
	}

	for _, tier := range []struct {
		health   agenthealth.Status
		strategy string
	}{
		{health: agenthealth.StatusHealthy, strategy: "healthy-created-at-id"},
		{health: agenthealth.StatusUnknown, strategy: "unknown-fallback-created-at-id"},
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
		decision := Decision{
			Agent: candidates[0], RequiredCapabilities: capabilities, Health: tier.health,
			Strategy: tier.strategy, CandidateCount: len(candidates),
		}
		attributes := append(observability.ContextAttrs(ctx),
			"agent_id", decision.Agent.ID,
			"required_capabilities", decision.RequiredCapabilities,
			"agent_health", decision.Health,
			"routing_strategy", decision.Strategy,
			"candidate_count", decision.CandidateCount,
		)
		slog.InfoContext(ctx, "agent routing decision", attributes...)
		return decision, nil
	}
	return Decision{}, ErrNoCandidate
}

func normalizeRequirements(required []string) ([]string, error) {
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
