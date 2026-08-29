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
	"strings"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

type Server struct {
	store  store.Repository
	engine *engine.Engine
	events events.Broker
	mux    *http.ServeMux
}

func New(s store.Repository, e *engine.Engine, bus events.Broker) *Server {
	server := &Server{store: s, engine: e, events: bus, mux: http.NewServeMux()}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler {
	return loggingMiddleware(recoverMiddleware(s.mux))
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /readyz", s.ready)
	s.mux.HandleFunc("GET /api/v1/agents", s.listAgents)
	s.mux.HandleFunc("POST /api/v1/agents", s.createAgent)
	s.mux.HandleFunc("GET /api/v1/agents/{id}", s.getAgent)
	s.mux.HandleFunc("GET /api/v1/runs", s.listRuns)
	s.mux.HandleFunc("POST /api/v1/runs", s.createRun)
	s.mux.HandleFunc("GET /api/v1/runs/{id}", s.getRun)
	s.mux.HandleFunc("POST /api/v1/runs/{id}/cancel", s.cancelRun)
	s.mux.HandleFunc("GET /api/v1/runs/{id}/events", s.runEvents)
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
	Name         string   `json:"name"`
	SystemPrompt string   `json:"system_prompt"`
	Runtime      string   `json:"runtime"`
	Protocol     string   `json:"protocol"`
	Endpoint     string   `json:"endpoint"`
	Capabilities []string `json:"capabilities"`
}

func (s *Server) createAgent(w http.ResponseWriter, r *http.Request) {
	var request createAgentRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	agent := domain.Agent{
		ID:           newID("agt"),
		Name:         request.Name,
		SystemPrompt: request.SystemPrompt,
		Runtime:      request.Runtime,
		Protocol:     request.Protocol,
		Endpoint:     request.Endpoint,
		Capabilities: request.Capabilities,
		CreatedAt:    time.Now().UTC(),
	}
	if err := agent.NormalizeAndValidate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := s.store.CreateAgent(r.Context(), agent); err != nil {
		writeError(w, http.StatusInternalServerError, "could not create agent")
		return
	}
	writeJSON(w, http.StatusCreated, agent)
}

func (s *Server) listAgents(w http.ResponseWriter, r *http.Request) {
	agents, err := s.store.ListAgents(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list agents")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": agents})
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
	writeJSON(w, http.StatusOK, agent)
}

type createRunRequest struct {
	AgentID string `json:"agent_id"`
	Input   string `json:"input"`
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	var request createRunRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	request.AgentID = strings.TrimSpace(request.AgentID)
	request.Input = strings.TrimSpace(request.Input)
	if request.AgentID == "" || request.Input == "" {
		writeError(w, http.StatusBadRequest, "agent_id and input are required")
		return
	}
	if _, err := s.store.GetAgent(r.Context(), request.AgentID); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load agent")
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) > 128 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key must be at most 128 characters")
		return
	}

	run := domain.Run{
		ID:          newID("run"),
		AgentID:     request.AgentID,
		Input:       request.Input,
		Status:      domain.RunQueued,
		MaxAttempts: s.engine.MaxAttempts(),
		CreatedAt:   time.Now().UTC(),
	}
	createdRun, isNew, err := s.store.CreateRun(r.Context(), run, idempotencyKey)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create run")
		return
	}
	if !isNew {
		if createdRun.AgentID != request.AgentID || createdRun.Input != request.Input {
			writeError(w, http.StatusConflict, "Idempotency-Key was already used with a different request")
			return
		}
		w.Header().Set("Idempotency-Replayed", "true")
		writeJSON(w, http.StatusOK, createdRun)
		return
	}
	run = createdRun
	s.events.Publish(domain.RunEvent{
		RunID: run.ID, Type: "run.queued", Message: "run queued", Timestamp: time.Now().UTC(),
	})

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
			RunID: run.ID, Type: "run.failed", Message: err.Error(), Timestamp: time.Now().UTC(),
		})
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, run)
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
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, payload)
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
		next.ServeHTTP(w, r)
		slog.Info("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("panic recovered", "error", recovered)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
