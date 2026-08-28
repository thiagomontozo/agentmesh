package store

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

var ErrNotFound = errors.New("not found")

type Memory struct {
	mu              sync.RWMutex
	agents          map[string]domain.Agent
	runs            map[string]domain.Run
	idempotencyKeys map[string]string
}

var _ Repository = (*Memory)(nil)

func NewMemory() *Memory {
	return &Memory{
		agents:          make(map[string]domain.Agent),
		runs:            make(map[string]domain.Run),
		idempotencyKeys: make(map[string]string),
	}
}

func (m *Memory) CreateAgent(_ context.Context, agent domain.Agent) (domain.Agent, error) {
	if err := agent.NormalizeAndValidate(); err != nil {
		return domain.Agent{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.agents[agent.ID] = cloneAgent(agent)
	return cloneAgent(agent), nil
}

func (m *Memory) GetAgent(_ context.Context, id string) (domain.Agent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	agent, ok := m.agents[id]
	if !ok {
		return domain.Agent{}, ErrNotFound
	}
	return cloneAgent(agent), nil
}

func (m *Memory) ListAgents(_ context.Context) ([]domain.Agent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]domain.Agent, 0, len(m.agents))
	for _, agent := range m.agents {
		result = append(result, cloneAgent(agent))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, nil
}

func (m *Memory) CreateRun(_ context.Context, run domain.Run, idempotencyKey string) (domain.Run, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idempotencyKey != "" {
		if runID, ok := m.idempotencyKeys[idempotencyKey]; ok {
			return m.runs[runID], false, nil
		}
		m.idempotencyKeys[idempotencyKey] = run.ID
	}
	m.runs[run.ID] = run
	return run, true, nil
}

func (m *Memory) GetRun(_ context.Context, id string) (domain.Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.runs[id]
	if !ok {
		return domain.Run{}, ErrNotFound
	}
	return run, nil
}

func (m *Memory) UpdateRun(_ context.Context, run domain.Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.runs[run.ID]; !ok {
		return ErrNotFound
	}
	m.runs[run.ID] = run
	return nil
}

func (m *Memory) ListRuns(_ context.Context) ([]domain.Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]domain.Run, 0, len(m.runs))
	for _, run := range m.runs {
		result = append(result, run)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, nil
}

func (m *Memory) RecoverPendingRuns(_ context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, 0)
	for id, run := range m.runs {
		if run.Status == domain.RunRunning {
			run.Status = domain.RunQueued
			run.StartedAt = nil
			if run.Attempt > 0 {
				run.Attempt--
			}
			m.runs[id] = run
		}
		if run.Status == domain.RunQueued {
			result = append(result, id)
		}
	}
	sort.Strings(result)
	return result, nil
}

func (m *Memory) Ping(context.Context) error {
	return nil
}

func cloneAgent(agent domain.Agent) domain.Agent {
	agent.Capabilities = append([]string(nil), agent.Capabilities...)
	return agent
}
