package agenthealth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

type Status string

const (
	StatusUnknown   Status = "unknown"
	StatusHealthy   Status = "healthy"
	StatusUnhealthy Status = "unhealthy"
)

type State struct {
	AgentID       string     `json:"agent_id"`
	Status        Status     `json:"status"`
	LastCheckedAt *time.Time `json:"last_checked_at,omitempty"`
	Reason        string     `json:"reason,omitempty"`
}

type Registry interface {
	State(agentID string) State
	Refresh(agent domain.Agent)
	Forget(agentID string)
}

type Noop struct{}

func (Noop) State(agentID string) State { return State{AgentID: agentID, Status: StatusUnknown} }
func (Noop) Refresh(domain.Agent)       {}
func (Noop) Forget(string)              {}

type Config struct {
	Path     string
	Interval time.Duration
	Timeout  time.Duration
	Workers  int
}

type Service struct {
	repository store.AgentRepository
	client     *http.Client
	config     Config
	queue      chan domain.Agent

	mu      sync.RWMutex
	states  map[string]State
	pending map[string]struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func New(repository store.AgentRepository, client *http.Client, config Config) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("agent repository is required")
	}
	if config.Path == "" {
		config.Path = "/healthz"
	}
	if !strings.HasPrefix(config.Path, "/") || strings.HasPrefix(config.Path, "//") {
		return nil, fmt.Errorf("agent health path must start with /")
	}
	parsedPath, err := url.Parse(config.Path)
	if err != nil || parsedPath.IsAbs() || parsedPath.Host != "" || parsedPath.RawQuery != "" || parsedPath.Fragment != "" {
		return nil, fmt.Errorf("agent health path must be a relative URL path without query or fragment")
	}
	if config.Interval <= 0 || config.Timeout <= 0 {
		return nil, fmt.Errorf("agent health interval and timeout must be positive")
	}
	if config.Workers < 1 {
		return nil, fmt.Errorf("agent health workers must be positive")
	}
	if client == nil {
		client = &http.Client{}
	}
	clientCopy := *client
	clientCopy.Timeout = config.Timeout
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Service{
		repository: repository,
		client:     &clientCopy,
		config:     config,
		queue:      make(chan domain.Agent, config.Workers*64),
		states:     make(map[string]State),
		pending:    make(map[string]struct{}),
	}, nil
}

func (s *Service) Start(parent context.Context) {
	s.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		s.cancel = cancel
		for workerID := 1; workerID <= s.config.Workers; workerID++ {
			s.wg.Add(1)
			go s.worker(ctx, workerID)
		}
		s.wg.Add(1)
		go s.schedule(ctx)
	})
}

func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
	})
}

func (s *Service) State(agentID string) State {
	s.mu.RLock()
	state, ok := s.states[agentID]
	s.mu.RUnlock()
	if !ok {
		return State{AgentID: agentID, Status: StatusUnknown}
	}
	return state
}

func (s *Service) Refresh(agent domain.Agent) {
	if !probeable(agent) {
		s.Forget(agent.ID)
		return
	}
	s.mu.Lock()
	if _, exists := s.pending[agent.ID]; exists {
		s.mu.Unlock()
		return
	}
	s.pending[agent.ID] = struct{}{}
	s.mu.Unlock()
	select {
	case s.queue <- agent:
	default:
		s.mu.Lock()
		delete(s.pending, agent.ID)
		s.mu.Unlock()
		slog.Warn("agent health queue is full", "agent_id", agent.ID)
	}
}

func (s *Service) Forget(agentID string) {
	s.mu.Lock()
	delete(s.states, agentID)
	delete(s.pending, agentID)
	s.mu.Unlock()
}

func (s *Service) schedule(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.Interval)
	defer ticker.Stop()
	for {
		s.refreshAll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) refreshAll(ctx context.Context) {
	agents, err := s.repository.ListAgents(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("could not list agents for health checks", "error", err)
		}
		return
	}
	for _, agent := range agents {
		s.Refresh(agent)
	}
}

func (s *Service) worker(ctx context.Context, workerID int) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case agent := <-s.queue:
			state := s.check(ctx, agent)
			current, getErr := s.repository.GetAgent(ctx, agent.ID)
			s.mu.Lock()
			delete(s.pending, agent.ID)
			if getErr == nil && sameTarget(current, agent) {
				s.states[agent.ID] = state
			}
			s.mu.Unlock()
			slog.Debug("agent health checked", "agent_id", agent.ID, "status", state.Status, "reason", state.Reason, "health_worker_id", workerID)
		}
	}
}

func (s *Service) check(parent context.Context, agent domain.Agent) State {
	checkedAt := time.Now().UTC()
	state := State{AgentID: agent.ID, Status: StatusUnhealthy, LastCheckedAt: &checkedAt}
	healthURL, err := buildHealthURL(agent.Endpoint, s.config.Path)
	if err != nil {
		state.Reason = "invalid_endpoint"
		return state
	}
	ctx, cancel := context.WithTimeout(parent, s.config.Timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		state.Reason = "invalid_endpoint"
		return state
	}
	request.Header.Set("Accept", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			state.Reason = "timeout"
		} else if errors.Is(ctx.Err(), context.Canceled) {
			state.Reason = "canceled"
		} else {
			state.Reason = "unreachable"
		}
		return state
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		state.Reason = fmt.Sprintf("http_status_%d", response.StatusCode)
		return state
	}
	state.Status = StatusHealthy
	state.Reason = ""
	return state
}

func buildHealthURL(endpoint, path string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("invalid endpoint")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("unsupported endpoint scheme")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("unsafe endpoint")
	}
	return url.JoinPath(parsed.String(), path)
}

func probeable(agent domain.Agent) bool {
	return strings.EqualFold(strings.TrimSpace(agent.Runtime), "remote") &&
		strings.EqualFold(strings.TrimSpace(agent.Protocol), "http") && strings.TrimSpace(agent.Endpoint) != ""
}

func sameTarget(current, checked domain.Agent) bool {
	return current.Runtime == checked.Runtime && current.Protocol == checked.Protocol && current.Endpoint == checked.Endpoint
}

var _ Registry = (*Service)(nil)
var _ Registry = Noop{}
