package httpapi

import (
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
	events *events.Bus
	mux    *http.ServeMux
}

func New(s store.Repository, e *engine.Engine, bus *events.Bus) *Server {
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
	s.mux.HandleFunc("GET /api/v1/runs/{id}/events", s.runEvents)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "agentmesh",
		"time":    time.Now().UTC(),
	})
}

func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type createAgentRequest struct {
	Name         string `json:"name"`
	SystemPrompt string `json:"system_prompt"`
}

func (s *Server) createAgent(w http.ResponseWriter, r *http.Request) {
	var request createAgentRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	request.Name = strings.TrimSpace(request.Name)
	if request.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	agent := domain.Agent{
		ID:           newID("agt"),
		Name:         request.Name,
		SystemPrompt: strings.TrimSpace(request.SystemPrompt),
		CreatedAt:    time.Now().UTC(),
	}
	s.store.CreateAgent(agent)
	writeJSON(w, http.StatusCreated, agent)
}

func (s *Server) listAgents(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": s.store.ListAgents()})
}

func (s *Server) getAgent(w http.ResponseWriter, r *http.Request) {
	agent, err := s.store.GetAgent(r.PathValue("id"))
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
	if _, err := s.store.GetAgent(request.AgentID); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	}

	run := domain.Run{
		ID:        newID("run"),
		AgentID:   request.AgentID,
		Input:     request.Input,
		Status:    domain.RunQueued,
		CreatedAt: time.Now().UTC(),
	}
	s.store.CreateRun(run)
	s.events.Publish(domain.RunEvent{
		RunID: run.ID, Type: "run.queued", Message: "run queued", Timestamp: time.Now().UTC(),
	})

	if err := s.engine.Enqueue(run.ID); err != nil {
		if transitionErr := run.Fail(err, time.Now()); transitionErr != nil {
			writeError(w, http.StatusInternalServerError, "could not fail run")
			return
		}
		if updateErr := s.store.UpdateRun(run); updateErr != nil {
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

func (s *Server) listRuns(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": s.store.ListRuns()})
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.GetRun(r.PathValue("id"))
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

func (s *Server) runEvents(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if _, err := s.store.GetRun(runID); errors.Is(err, store.ErrNotFound) {
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
			if event.Type == "run.succeeded" || event.Type == "run.failed" {
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
