package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/agenthealth"
	"github.com/thiagomontozo/agentmesh/internal/discovery"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/observability"
	agentrouter "github.com/thiagomontozo/agentmesh/internal/router"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

type Server struct {
	store       store.Repository
	engine      *engine.Engine
	events      events.Broker
	mux         *http.ServeMux
	instanceID  string
	agentHealth agenthealth.Registry
	discovery   *discovery.Service
	agentRouter *agentrouter.Router
}

func New(s store.Repository, e *engine.Engine, bus events.Broker) *Server {
	return NewWithInstanceID(s, e, bus, "local")
}

func NewWithInstanceID(s store.Repository, e *engine.Engine, bus events.Broker, instanceID string) *Server {
	server := &Server{
		store: s, engine: e, events: bus, mux: http.NewServeMux(), instanceID: instanceID,
		agentHealth: agenthealth.Noop{},
	}
	server.discovery = discovery.New(s, server.agentHealth)
	server.agentRouter = agentrouter.NewWithLoad(server.discovery, s)
	server.routes()
	return server
}

func (s *Server) SetAgentHealth(registry agenthealth.Registry) {
	if registry != nil {
		s.agentHealth = registry
		s.discovery = discovery.New(s.store, registry)
		s.agentRouter = agentrouter.NewWithLoad(s.discovery, s.store)
	}
}

func (s *Server) Handler() http.Handler {
	return requestContextMiddleware(s.instanceID, loggingMiddleware(recoverMiddleware(s.mux)))
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /readyz", s.ready)
	s.mux.HandleFunc("GET /api/v1/agents", s.listAgents)
	s.mux.HandleFunc("POST /api/v1/agents", s.createAgent)
	s.mux.HandleFunc("GET /api/v1/agents/{id}", s.getAgent)
	s.mux.HandleFunc("PUT /api/v1/agents/{id}", s.updateAgent)
	s.mux.HandleFunc("DELETE /api/v1/agents/{id}", s.deleteAgent)
	s.mux.HandleFunc("GET /api/v1/agents/{id}/health", s.getAgentHealth)
	s.mux.HandleFunc("GET /api/v1/runs", s.listRuns)
	s.mux.HandleFunc("POST /api/v1/runs", s.createRun)
	s.mux.HandleFunc("GET /api/v1/runs/{id}", s.getRun)
	s.mux.HandleFunc("GET /api/v1/runs/{id}/children", s.listChildRuns)
	s.mux.HandleFunc("POST /api/v1/runs/{id}/cancel", s.cancelRun)
	s.mux.HandleFunc("GET /api/v1/runs/{id}/events", s.runEvents)
	s.mux.HandleFunc("GET /api/v1/workflows", s.listWorkflows)
	s.mux.HandleFunc("POST /api/v1/workflows", s.createWorkflow)
	s.mux.HandleFunc("GET /api/v1/workflows/{id}", s.getWorkflow)
}

type createWorkflowRequest struct {
	Input string                      `json:"input"`
	Steps []createWorkflowStepRequest `json:"steps"`
}

type createWorkflowStepRequest struct {
	ID               string   `json:"id"`
	AgentID          string   `json:"agent_id"`
	Input            string   `json:"input"`
	InputFrom        []string `json:"input_from"`
	InputAggregation string   `json:"input_aggregation"`
	DependsOn        []string `json:"depends_on"`
}

func (s *Server) createWorkflow(w http.ResponseWriter, r *http.Request) {
	var request createWorkflowRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	workflow := domain.Workflow{ID: newID("wf"), Input: request.Input, Steps: make([]domain.WorkflowStep, len(request.Steps))}
	for index, requested := range request.Steps {
		workflow.Steps[index] = domain.WorkflowStep{
			ID: requested.ID, AgentID: requested.AgentID, Input: requested.Input,
			InputFrom: requested.InputFrom, InputAggregation: requested.InputAggregation,
			DependsOn: requested.DependsOn,
		}
	}
	if err := workflow.InitializeForCreate(time.Now()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	for _, step := range workflow.Steps {
		if _, err := s.store.GetAgent(r.Context(), step.AgentID); errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("agent %s not found", step.AgentID))
			return
		} else if err != nil {
			writeError(w, http.StatusInternalServerError, "could not validate workflow agents")
			return
		}
	}
	created, err := s.store.CreateWorkflow(r.Context(), workflow)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create workflow")
		return
	}
	slog.InfoContext(r.Context(), "workflow created", append(observability.ContextAttrs(r.Context()), "workflow_id", created.ID, "steps", len(created.Steps))...)
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	workflow, err := s.store.GetWorkflow(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load workflow")
		return
	}
	writeJSON(w, http.StatusOK, workflow)
}

func (s *Server) listWorkflows(w http.ResponseWriter, r *http.Request) {
	workflows, err := s.store.ListWorkflows(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list workflows")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": workflows})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "agentmesh",
		"time":    time.Now().UTC(),
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.engine.Ready(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "dependencies are not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type createAgentRequest struct {
	Name           string   `json:"name"`
	SystemPrompt   string   `json:"system_prompt"`
	Runtime        string   `json:"runtime"`
	Protocol       string   `json:"protocol"`
	Endpoint       string   `json:"endpoint"`
	Capabilities   []string `json:"capabilities"`
	MaxConcurrency int      `json:"max_concurrency"`
	Priority       int      `json:"priority"`
}

func (s *Server) createAgent(w http.ResponseWriter, r *http.Request) {
	var request createAgentRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	agent := domain.Agent{
		ID:             newID("agt"),
		Name:           request.Name,
		SystemPrompt:   request.SystemPrompt,
		Runtime:        request.Runtime,
		Protocol:       request.Protocol,
		Endpoint:       request.Endpoint,
		Capabilities:   request.Capabilities,
		MaxConcurrency: request.MaxConcurrency,
		Priority:       request.Priority,
		CreatedAt:      time.Now().UTC(),
	}
	if err := agent.NormalizeAndValidate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	created, err := s.store.CreateAgent(r.Context(), agent)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create agent")
		return
	}
	s.agentHealth.Refresh(created)
	attributes := append(observability.ContextAttrs(r.Context()), "agent_id", created.ID, "runtime", created.Runtime)
	slog.InfoContext(r.Context(), "agent created", attributes...)
	setAgentETag(w, created.Version)
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) getAgentHealth(w http.ResponseWriter, r *http.Request) {
	agent, err := s.store.GetAgent(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load agent")
		return
	}
	state := s.agentHealth.State(agent.ID)
	s.agentHealth.Refresh(agent)
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) listAgents(w http.ResponseWriter, r *http.Request) {
	query, err := agentDiscoveryQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := s.discovery.Search(r.Context(), query)
	if errors.Is(err, discovery.ErrInvalidQuery) {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not discover agents")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func agentDiscoveryQuery(r *http.Request) (discovery.Query, error) {
	values := r.URL.Query()
	health := strings.ToLower(strings.TrimSpace(values.Get("health")))
	status := strings.ToLower(strings.TrimSpace(values.Get("status")))
	if health != "" && status != "" && health != status {
		return discovery.Query{}, fmt.Errorf("health and status filters conflict")
	}
	if health == "" {
		health = status
	}
	limit, err := queryNonNegativeInt(values.Get("limit"), "limit")
	if err != nil {
		return discovery.Query{}, err
	}
	offset, err := queryNonNegativeInt(values.Get("offset"), "offset")
	if err != nil {
		return discovery.Query{}, err
	}
	return discovery.Query{
		Capability: values.Get("capability"), Runtime: values.Get("runtime"), Protocol: values.Get("protocol"),
		Health: agenthealth.Status(health), Limit: limit, Offset: offset,
	}, nil
}

func queryNonNegativeInt(value, name string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return parsed, nil
}

func (s *Server) getAgent(w http.ResponseWriter, r *http.Request) {
	agent, err := s.store.GetAgent(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load agent")
		return
	}
	setAgentETag(w, agent.Version)
	writeJSON(w, http.StatusOK, agent)
}

func (s *Server) updateAgent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, err := s.store.GetAgent(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load agent")
		return
	}
	expectedVersion, err := agentExpectedVersion(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var request createAgentRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	agent := domain.Agent{
		ID: id, Name: request.Name, SystemPrompt: request.SystemPrompt,
		Runtime: request.Runtime, Protocol: request.Protocol, Endpoint: request.Endpoint,
		Capabilities:   request.Capabilities,
		MaxConcurrency: request.MaxConcurrency,
		Priority:       request.Priority,
	}
	if err := agent.NormalizeAndValidate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	updated, err := s.store.UpdateAgent(r.Context(), agent, expectedVersion)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	}
	if errors.Is(err, store.ErrConflict) {
		writeError(w, http.StatusConflict, "agent was modified concurrently")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not update agent")
		return
	}
	s.agentHealth.Forget(updated.ID)
	s.agentHealth.Refresh(updated)
	setAgentETag(w, updated.Version)
	attributes := append(observability.ContextAttrs(r.Context()), "agent_id", updated.ID, "version", updated.Version)
	slog.InfoContext(r.Context(), "agent updated", attributes...)
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) deleteAgent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, err := s.store.GetAgent(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load agent")
		return
	}
	expectedVersion, err := agentExpectedVersion(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	err = s.store.DeleteAgent(r.Context(), id, expectedVersion)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	}
	if errors.Is(err, store.ErrConflict) {
		writeError(w, http.StatusConflict, "agent was modified concurrently")
		return
	}
	if errors.Is(err, store.ErrAgentInUse) {
		writeError(w, http.StatusConflict, "agent has dependent runs and cannot be deleted")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not delete agent")
		return
	}
	s.agentHealth.Forget(id)
	attributes := append(observability.ContextAttrs(r.Context()), "agent_id", id, "version", expectedVersion)
	slog.InfoContext(r.Context(), "agent deleted", attributes...)
	w.WriteHeader(http.StatusNoContent)
}

func agentExpectedVersion(r *http.Request) (int64, error) {
	value := strings.TrimSpace(r.Header.Get("If-Match"))
	if value == "" {
		return 0, fmt.Errorf("If-Match is required for Agent mutations")
	}
	if strings.HasPrefix(value, "W/") || len(value) < 3 || value[0] != '"' || value[len(value)-1] != '"' {
		return 0, fmt.Errorf("If-Match must use a strong numeric ETag")
	}
	value = value[1 : len(value)-1]
	version, err := strconv.ParseInt(value, 10, 64)
	if err != nil || version < 1 {
		return 0, fmt.Errorf("If-Match must contain a positive numeric Agent version")
	}
	return version, nil
}

func setAgentETag(w http.ResponseWriter, version int64) {
	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, version))
}

type createRunRequest struct {
	AgentID              string   `json:"agent_id"`
	ParentRunID          string   `json:"parent_run_id"`
	RequiredCapabilities []string `json:"required_capabilities"`
	Input                string   `json:"input"`
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	var request createRunRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	request.AgentID = strings.TrimSpace(request.AgentID)
	request.ParentRunID = strings.TrimSpace(request.ParentRunID)
	request.Input = strings.TrimSpace(request.Input)
	if request.Input == "" {
		writeError(w, http.StatusBadRequest, "input is required")
		return
	}
	if request.AgentID != "" && len(request.RequiredCapabilities) > 0 {
		writeError(w, http.StatusBadRequest, "agent_id and required_capabilities are mutually exclusive")
		return
	}
	if request.AgentID == "" && len(request.RequiredCapabilities) == 0 {
		writeError(w, http.StatusBadRequest, "agent_id or required_capabilities is required")
		return
	}
	routed := request.AgentID == ""
	if routed {
		normalized, err := agentrouter.NormalizeRequirements(request.RequiredCapabilities)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		request.RequiredCapabilities = normalized
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) > 128 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key must be at most 128 characters")
		return
	}
	if idempotencyKey != "" {
		existing, err := s.store.GetRunByIdempotencyKey(r.Context(), idempotencyKey)
		if err == nil {
			s.writeRunReplay(w, r, existing, request, routed)
			return
		}
		if !errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusInternalServerError, "could not check idempotency key")
			return
		}
	}
	var parentRun domain.Run
	if request.ParentRunID != "" {
		var err error
		parentRun, err = s.store.GetRun(r.Context(), request.ParentRunID)
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "parent Run not found")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not load parent Run")
			return
		}
	}
	if routed {
		decision, err := s.agentRouter.Select(r.Context(), request.RequiredCapabilities)
		if errors.Is(err, agentrouter.ErrInvalidRequirements) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, agentrouter.ErrNoCandidate) {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		if errors.Is(err, agentrouter.ErrNoCapacity) {
			writeError(w, http.StatusTooManyRequests, err.Error())
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not route run")
			return
		}
		request.AgentID = decision.Agent.ID
		request.RequiredCapabilities = decision.RequiredCapabilities
	} else if _, err := s.store.GetAgent(r.Context(), request.AgentID); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load agent")
		return
	}
	run := domain.Run{
		ID:                   newID("run"),
		AgentID:              request.AgentID,
		ParentRunID:          request.ParentRunID,
		RequiredCapabilities: request.RequiredCapabilities,
		Input:                request.Input,
		Status:               domain.RunQueued,
		MaxAttempts:          s.engine.MaxAttempts(),
		RequestID:            observability.RequestID(r.Context()),
		CreatedAt:            time.Now().UTC(),
	}
	if request.ParentRunID != "" {
		if err := run.AttachTo(parentRun); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	createdRun, isNew, err := s.store.CreateRun(r.Context(), run, idempotencyKey)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create run")
		return
	}
	if !isNew {
		s.writeRunReplay(w, r, createdRun, request, routed)
		return
	}
	run = createdRun
	s.events.Publish(domain.RunEvent{
		RunID: run.ID, ParentRunID: run.ParentRunID, RootRunID: run.RootRunID,
		Type: "run.queued", Message: "run queued", Attempt: run.Attempt, Timestamp: time.Now().UTC(),
	})
	if run.ParentRunID != "" {
		s.events.Publish(domain.RunEvent{
			RunID: run.ParentRunID, ChildRunID: run.ID, ParentRunID: run.ParentRunID, RootRunID: run.RootRunID,
			Type: "run.child_queued", Message: "child Run queued", Timestamp: time.Now().UTC(),
		})
	}
	attributes := append(observability.ContextAttrs(r.Context()),
		"run_id", run.ID, "agent_id", run.AgentID, "parent_run_id", run.ParentRunID,
		"root_run_id", run.RootRunID, "attempt", run.Attempt,
	)
	slog.InfoContext(r.Context(), "run queued", attributes...)

	if err := s.engine.Enqueue(r.Context(), run.ID); err != nil {
		if transitionErr := run.Fail(err, time.Now()); transitionErr != nil {
			writeError(w, http.StatusInternalServerError, "could not fail run")
			return
		}
		if updateErr := s.store.UpdateRun(r.Context(), run); updateErr != nil {
			writeError(w, http.StatusInternalServerError, "could not update run")
			return
		}
		s.events.Publish(domain.RunEvent{
			RunID: run.ID, ParentRunID: run.ParentRunID, RootRunID: run.RootRunID,
			Type: "run.failed", Message: err.Error(), Attempt: run.Attempt, Timestamp: time.Now().UTC(),
		})
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, run)
}

func (s *Server) writeRunReplay(w http.ResponseWriter, r *http.Request, existing domain.Run, request createRunRequest, routed bool) {
	requestChanged := existing.Input != request.Input ||
		existing.ParentRunID != request.ParentRunID ||
		!slices.Equal(existing.RequiredCapabilities, request.RequiredCapabilities)
	if !routed {
		requestChanged = requestChanged || existing.AgentID != request.AgentID
	}
	if requestChanged {
		writeError(w, http.StatusConflict, "Idempotency-Key was already used with a different request")
		return
	}
	w.Header().Set("Idempotency-Replayed", "true")
	attributes := append(observability.ContextAttrs(r.Context()), "run_id", existing.ID, "agent_id", existing.AgentID)
	slog.InfoContext(r.Context(), "run submission replayed", attributes...)
	writeJSON(w, http.StatusOK, existing)
}

func (s *Server) listChildRuns(w http.ResponseWriter, r *http.Request) {
	children, err := s.store.ListChildRuns(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "Run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list child Runs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": children})
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.store.ListRuns(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list runs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": runs})
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.GetRun(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load run")
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.engine.Cancel(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if errors.Is(err, domain.ErrRunNotCancelable) {
		writeError(w, http.StatusConflict, fmt.Sprintf("run in status %s cannot be canceled", run.Status))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not cancel run")
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) runEvents(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if _, err := s.store.GetRun(r.Context(), runID); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	eventCh, unsubscribe := s.events.Subscribe(runID)
	defer unsubscribe()

	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-eventCh:
			if !ok {
				return
			}
			payload, err := json.Marshal(event)
			if err != nil {
				return
			}
			_, _ = fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", event.ID, event.Type, payload)
			flusher.Flush()
			if event.Type == "run.succeeded" || event.Type == "run.failed" || event.Type == "run.canceled" {
				return
			}
		}
	}
}

func decodeJSON(r *http.Request, destination any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("invalid JSON: body must contain a single object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"message": message,
		},
	})
}

func newID(prefix string) string {
	random := make([]byte, 12)
	if _, err := rand.Read(random); err == nil {
		return prefix + "_" + hex.EncodeToString(random)
	}
	return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &responseRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		attributes := append(observability.ContextAttrs(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"status", status,
			"response_bytes", recorder.bytes,
			"duration_ms", time.Since(started).Milliseconds(),
		)
		slog.InfoContext(r.Context(), "http request", attributes...)
	})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				attributes := append(observability.ContextAttrs(r.Context()), "panic", recovered)
				slog.ErrorContext(r.Context(), "http panic recovered", attributes...)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(payload []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	written, err := r.ResponseWriter.Write(payload)
	r.bytes += written
	return written, err
}

func (r *responseRecorder) Flush() {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *responseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func requestContextMiddleware(instanceID string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := normalizedRequestID(r.Header.Get("X-Request-ID"))
		w.Header().Set("X-Request-ID", requestID)
		ctx := observability.WithRequestID(r.Context(), requestID)
		ctx = observability.WithInstanceID(ctx, instanceID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func normalizedRequestID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return newID("req")
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			strings.ContainsRune("-_.:", character) {
			continue
		}
		return newID("req")
	}
	return value
}
