package store

import "github.com/thiagomontozo/agentmesh/internal/domain"

type AgentRepository interface {
	CreateAgent(agent domain.Agent) domain.Agent
	GetAgent(id string) (domain.Agent, error)
	ListAgents() []domain.Agent
}

type RunRepository interface {
	CreateRun(run domain.Run) domain.Run
	GetRun(id string) (domain.Run, error)
	UpdateRun(run domain.Run) error
	ListRuns() []domain.Run
}

type Repository interface {
	AgentRepository
	RunRepository
}
