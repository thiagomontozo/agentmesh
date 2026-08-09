package store

import (
	"errors"
	"sort"
	"sync"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

var ErrNotFound = errors.New("not found")

type Memory struct {
	mu     sync.RWMutex
	agents map[string]domain.Agent
	runs   map[string]domain.Run
}

var _ Repository = (*Memory)(nil)

func NewMemory() *Memory {
	return &Memory{
		agents: make(map[string]domain.Agent),
		runs:   make(map[string]domain.Run),
	}
}

func (m *Memory) CreateAgent(agent domain.Agent) domain.Agent {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.agents[agent.ID] = agent
	return agent
}

func (m *Memory) GetAgent(id string) (domain.Agent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	agent, ok := m.agents[id]
	if !ok {
		return domain.Agent{}, ErrNotFound
	}
	return agent, nil
}

func (m *Memory) ListAgents() []domain.Agent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]domain.Agent, 0, len(m.agents))
	for _, agent := range m.agents {
		result = append(result, agent)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (m *Memory) CreateRun(run domain.Run) domain.Run {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.ID] = run
	return run
}

func (m *Memory) GetRun(id string) (domain.Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.runs[id]
	if !ok {
		return domain.Run{}, ErrNotFound
	}
	return run, nil
}

func (m *Memory) UpdateRun(run domain.Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.runs[run.ID]; !ok {
		return ErrNotFound
	}
	m.runs[run.ID] = run
	return nil
}

func (m *Memory) ListRuns() []domain.Run {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]domain.Run, 0, len(m.runs))
	for _, run := range m.runs {
		result = append(result, run)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}
