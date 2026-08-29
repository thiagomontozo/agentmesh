package discovery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/thiagomontozo/agentmesh/internal/agenthealth"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

const MaxPageSize = 200

var ErrInvalidQuery = errors.New("invalid Agent discovery query")

type Query struct {
	Capability string
	Runtime    string
	Protocol   string
	Health     agenthealth.Status
	Limit      int
	Offset     int
}

type Result struct {
	Items  []domain.Agent `json:"items"`
	Total  int            `json:"total"`
	Limit  int            `json:"limit,omitempty"`
	Offset int            `json:"offset,omitempty"`
}

type Service struct {
	repository store.AgentRepository
	health     agenthealth.Registry
}

func New(repository store.AgentRepository, health agenthealth.Registry) *Service {
	if health == nil {
		health = agenthealth.Noop{}
	}
	return &Service{repository: repository, health: health}
}

func (s *Service) Search(ctx context.Context, query Query) (Result, error) {
	if s == nil || s.repository == nil {
		return Result{}, fmt.Errorf("agent discovery repository is required")
	}
	var err error
	if query.Capability != "" {
		query.Capability, err = domain.NormalizeCapability(query.Capability)
		if err != nil {
			return Result{}, fmt.Errorf("%w: %v", ErrInvalidQuery, err)
		}
	}
	if query.Runtime, err = domain.NormalizeAgentIdentifier("runtime", query.Runtime); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalidQuery, err)
	}
	if query.Protocol, err = domain.NormalizeAgentIdentifier("protocol", query.Protocol); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalidQuery, err)
	}
	if query.Limit < 0 || query.Limit > MaxPageSize || query.Offset < 0 {
		return Result{}, fmt.Errorf("%w: limit must be between 0 and %d and offset cannot be negative", ErrInvalidQuery, MaxPageSize)
	}
	query.Health = agenthealth.Status(strings.ToLower(strings.TrimSpace(string(query.Health))))
	if query.Health != "" && query.Health != agenthealth.StatusUnknown && query.Health != agenthealth.StatusHealthy && query.Health != agenthealth.StatusUnhealthy {
		return Result{}, fmt.Errorf("%w: health must be unknown, healthy or unhealthy", ErrInvalidQuery)
	}

	agents, err := s.repository.FindAgents(ctx, store.AgentFilter{
		Capability: query.Capability, Runtime: query.Runtime, Protocol: query.Protocol,
	})
	if err != nil {
		return Result{}, err
	}
	filtered := agents[:0]
	for _, agent := range agents {
		if query.Health == "" || s.health.State(agent.ID).Status == query.Health {
			filtered = append(filtered, agent)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].CreatedAt.Equal(filtered[j].CreatedAt) {
			return filtered[i].ID < filtered[j].ID
		}
		return filtered[i].CreatedAt.Before(filtered[j].CreatedAt)
	})
	total := len(filtered)
	start := query.Offset
	if start > total {
		start = total
	}
	end := total
	if query.Limit > 0 && start+query.Limit < end {
		end = start + query.Limit
	}
	items := append([]domain.Agent(nil), filtered[start:end]...)
	return Result{Items: items, Total: total, Limit: query.Limit, Offset: query.Offset}, nil
}
