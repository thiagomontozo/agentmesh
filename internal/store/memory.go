package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

var ErrNotFound = errors.New("not found")

type Memory struct {
	mu              sync.RWMutex
	agents          map[string]domain.Agent
	runs            map[string]domain.Run
	runFences       map[string]int64
	runEvents       map[string][]domain.RunEvent
	idempotencyKeys map[string]string
}

var _ Repository = (*Memory)(nil)

func NewMemory() *Memory {
	return &Memory{
		agents:          make(map[string]domain.Agent),
		runs:            make(map[string]domain.Run),
		runFences:       make(map[string]int64),
		runEvents:       make(map[string][]domain.RunEvent),
		idempotencyKeys: make(map[string]string),
	}
}

func (m *Memory) AppendRunEvent(_ context.Context, event domain.RunEvent, retention time.Duration, maxPerRun int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.runs[event.RunID]; !ok {
		return ErrNotFound
	}
	events := append(m.runEvents[event.RunID], event)
	if retention > 0 {
		cutoff := time.Now().UTC().Add(-retention)
		first := 0
		for first < len(events) && events[first].Timestamp.Before(cutoff) {
			first++
		}
		events = events[first:]
	}
	if maxPerRun > 0 && len(events) > maxPerRun {
		events = events[len(events)-maxPerRun:]
	}
	m.runEvents[event.RunID] = append([]domain.RunEvent(nil), events...)
	return nil
}

func (m *Memory) ListRunEvents(_ context.Context, runID string, limit int) ([]domain.RunEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.runs[runID]; !ok {
		return nil, ErrNotFound
	}
	events := m.runEvents[runID]
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}
	return append([]domain.RunEvent(nil), events...), nil
}

func (m *Memory) CreateAgent(_ context.Context, agent domain.Agent) (domain.Agent, error) {
	if err := agent.InitializeForCreate(time.Now()); err != nil {
		return domain.Agent{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.agents[agent.ID]; exists {
		return domain.Agent{}, ErrConflict
	}
	m.agents[agent.ID] = cloneAgent(agent)
	return cloneAgent(agent), nil
}

func (m *Memory) UpdateAgent(_ context.Context, agent domain.Agent, expectedVersion int64) (domain.Agent, error) {
	if err := agent.NormalizeAndValidate(); err != nil {
		return domain.Agent{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.agents[agent.ID]
	if !ok {
		return domain.Agent{}, ErrNotFound
	}
	if expectedVersion < 1 || existing.Version != expectedVersion {
		return domain.Agent{}, ErrConflict
	}
	agent.CreatedAt = existing.CreatedAt
	agent.UpdatedAt = time.Now().UTC()
	agent.Version = existing.Version + 1
	m.agents[agent.ID] = cloneAgent(agent)
	return cloneAgent(agent), nil
}

func (m *Memory) DeleteAgent(_ context.Context, id string, expectedVersion int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	agent, ok := m.agents[id]
	if !ok {
		return ErrNotFound
	}
	if expectedVersion < 1 || agent.Version != expectedVersion {
		return ErrConflict
	}
	for _, run := range m.runs {
		if run.AgentID == id {
			return ErrAgentInUse
		}
	}
	delete(m.agents, id)
	return nil
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

func (m *Memory) ListAgents(ctx context.Context) ([]domain.Agent, error) {
	return m.FindAgents(ctx, AgentFilter{})
}

func (m *Memory) ListAgentsByCapability(ctx context.Context, capability string) ([]domain.Agent, error) {
	return m.FindAgents(ctx, AgentFilter{Capability: capability})
}

func (m *Memory) FindAgents(_ context.Context, filter AgentFilter) ([]domain.Agent, error) {
	var err error
	if filter.Capability != "" {
		filter.Capability, err = domain.NormalizeCapability(filter.Capability)
		if err != nil {
			return nil, err
		}
	}
	if filter.Runtime, err = domain.NormalizeAgentIdentifier("runtime", filter.Runtime); err != nil {
		return nil, err
	}
	if filter.Protocol, err = domain.NormalizeAgentIdentifier("protocol", filter.Protocol); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]domain.Agent, 0, len(m.agents))
	for _, agent := range m.agents {
		if (filter.Runtime != "" && agent.Runtime != filter.Runtime) ||
			(filter.Protocol != "" && agent.Protocol != filter.Protocol) {
			continue
		}
		if filter.Capability != "" && !agentHasCapability(agent, filter.Capability) {
			continue
		}
		result = append(result, cloneAgent(agent))
	}
	sortAgents(result)
	return result, nil
}

func agentHasCapability(agent domain.Agent, capability string) bool {
	for _, candidate := range agent.Capabilities {
		if candidate == capability {
			return true
		}
	}
	return false
}

func sortAgents(agents []domain.Agent) {
	sort.Slice(agents, func(i, j int) bool {
		if agents[i].CreatedAt.Equal(agents[j].CreatedAt) {
			return agents[i].ID < agents[j].ID
		}
		return agents[i].CreatedAt.Before(agents[j].CreatedAt)
	})
}

func (m *Memory) CreateRun(_ context.Context, run domain.Run, idempotencyKey string) (domain.Run, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idempotencyKey != "" {
		if runID, ok := m.idempotencyKeys[idempotencyKey]; ok {
			return cloneRun(m.runs[runID]), false, nil
		}
		m.idempotencyKeys[idempotencyKey] = run.ID
	}
	m.runs[run.ID] = cloneRun(run)
	return cloneRun(run), true, nil
}

func (m *Memory) GetRun(_ context.Context, id string) (domain.Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.runs[id]
	if !ok {
		return domain.Run{}, ErrNotFound
	}
	return cloneRun(run), nil
}

func (m *Memory) GetRunByIdempotencyKey(_ context.Context, key string) (domain.Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	runID, ok := m.idempotencyKeys[key]
	if !ok {
		return domain.Run{}, ErrNotFound
	}
	return cloneRun(m.runs[runID]), nil
}

func (m *Memory) UpdateRun(_ context.Context, run domain.Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.runs[run.ID]
	if !ok {
		return ErrNotFound
	}
	if existing.Status == domain.RunCanceled {
		return ErrRunCanceled
	}
	if m.runFences[run.ID] != 0 {
		return ErrStaleExecution
	}
	m.runs[run.ID] = cloneRun(run)
	return nil
}

func (m *Memory) ClaimRunExecution(_ context.Context, id string, minimumFence int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, ok := m.runs[id]
	if !ok {
		return 0, ErrNotFound
	}
	if run.Status == domain.RunCanceled {
		return 0, ErrRunCanceled
	}
	if run.Status != domain.RunQueued && run.Status != domain.RunRunning {
		return 0, fmt.Errorf("%w from status %s", ErrRunNotExecutable, run.Status)
	}
	next := m.runFences[id] + 1
	if minimumFence > next {
		next = minimumFence
	}
	m.runFences[id] = next
	return next, nil
}

func (m *Memory) UpdateRunFenced(_ context.Context, run domain.Run, fence int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.runs[run.ID]
	if !ok {
		return ErrNotFound
	}
	if existing.Status == domain.RunCanceled {
		return ErrRunCanceled
	}
	if fence <= 0 || m.runFences[run.ID] != fence {
		return ErrStaleExecution
	}
	m.runs[run.ID] = cloneRun(run)
	return nil
}

func (m *Memory) CancelRun(_ context.Context, id string, at time.Time) (domain.Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, ok := m.runs[id]
	if !ok {
		return domain.Run{}, ErrNotFound
	}
	if err := run.Cancel(at); err != nil {
		return run, err
	}
	m.runs[id] = cloneRun(run)
	return cloneRun(run), nil
}

func (m *Memory) ListRuns(_ context.Context) ([]domain.Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]domain.Run, 0, len(m.runs))
	for _, run := range m.runs {
		result = append(result, cloneRun(run))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, nil
}

func (m *Memory) CountActiveRunsByAgent(_ context.Context, agentIDs []string) (map[string]int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]int, len(agentIDs))
	for _, id := range agentIDs {
		result[id] = 0
	}
	for _, run := range m.runs {
		if run.Status != domain.RunQueued && run.Status != domain.RunRunning {
			continue
		}
		if _, requested := result[run.AgentID]; requested {
			result[run.AgentID]++
		}
	}
	return result, nil
}

func (m *Memory) ListPendingRuns(_ context.Context) ([]PendingRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]PendingRun, 0)
	for id, run := range m.runs {
		if run.Status == domain.RunQueued || run.Status == domain.RunRunning {
			result = append(result, PendingRun{ID: id, Status: run.Status})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (m *Memory) RecoverRun(_ context.Context, id string, minimumFence int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, ok := m.runs[id]
	if !ok {
		return false, ErrNotFound
	}
	if run.Status != domain.RunRunning {
		return false, nil
	}
	nextFence := m.runFences[id] + 1
	if minimumFence > nextFence {
		nextFence = minimumFence
	}
	m.runFences[id] = nextFence
	run.Status = domain.RunQueued
	run.StartedAt = nil
	run.DurationMS = 0
	if run.Attempt > 0 {
		run.Attempt--
	}
	m.runs[id] = run
	return true, nil
}

func (m *Memory) Ping(context.Context) error {
	return nil
}

func cloneAgent(agent domain.Agent) domain.Agent {
	agent.Capabilities = append([]string(nil), agent.Capabilities...)
	return agent
}

func cloneRun(run domain.Run) domain.Run {
	run.RequiredCapabilities = append([]string(nil), run.RequiredCapabilities...)
	return run
}
