package metrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

type Registry struct {
	mu                  sync.RWMutex
	httpRequests        map[string]uint64
	httpDurationSeconds float64
	runEvents           map[string]uint64
	routingDecisions    map[string]uint64
}

func New() *Registry {
	return &Registry{
		httpRequests: make(map[string]uint64), runEvents: make(map[string]uint64),
		routingDecisions: make(map[string]uint64),
	}
}

func (r *Registry) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, request)
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		method := request.Method
		switch method {
		case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodHead:
		default:
			method = "OTHER"
		}
		r.mu.Lock()
		r.httpRequests[method+":"+strconv.Itoa(status)]++
		r.httpDurationSeconds += time.Since(started).Seconds()
		r.mu.Unlock()
	})
}

func (r *Registry) ObserveRunEvent(eventType string) {
	if !knownRunEvent(eventType) {
		return
	}
	r.mu.Lock()
	r.runEvents[eventType]++
	r.mu.Unlock()
}

func (r *Registry) ObserveRouting(strategy string) {
	if strategy != "healthy-load-priority-created-at-id" && strategy != "unknown-fallback-load-priority-created-at-id" {
		strategy = "other"
	}
	r.mu.Lock()
	r.routingDecisions[strategy]++
	r.mu.Unlock()
}

func (r *Registry) WritePrometheus(ctx context.Context, writer io.Writer, repository store.RunRepository) error {
	runs, err := repository.ListRuns(ctx)
	if err != nil {
		return err
	}
	statuses := map[domain.RunStatus]int{
		domain.RunQueued: 0, domain.RunRunning: 0, domain.RunSucceeded: 0,
		domain.RunFailed: 0, domain.RunCanceled: 0,
	}
	for _, run := range runs {
		statuses[run.Status]++
	}
	r.mu.RLock()
	httpRequests := cloneMap(r.httpRequests)
	runEvents := cloneMap(r.runEvents)
	routingDecisions := cloneMap(r.routingDecisions)
	httpDuration := r.httpDurationSeconds
	r.mu.RUnlock()

	_, _ = io.WriteString(writer, "# HELP agentmesh_runs Current persisted Runs by status.\n# TYPE agentmesh_runs gauge\n")
	for _, status := range []domain.RunStatus{domain.RunQueued, domain.RunRunning, domain.RunSucceeded, domain.RunFailed, domain.RunCanceled} {
		_, _ = fmt.Fprintf(writer, "agentmesh_runs{status=%q} %d\n", status, statuses[status])
	}
	_, _ = fmt.Fprintf(writer, "# HELP agentmesh_queue_depth Persisted queued Runs.\n# TYPE agentmesh_queue_depth gauge\nagentmesh_queue_depth %d\n", statuses[domain.RunQueued])
	writeCounterMap(writer, "agentmesh_http_requests_total", "HTTP requests by bounded method and status.", httpRequests, func(key string) string {
		parts := strings.SplitN(key, ":", 2)
		return fmt.Sprintf("method=%q,status=%q", parts[0], parts[1])
	})
	requestCount := uint64(0)
	for _, count := range httpRequests {
		requestCount += count
	}
	_, _ = fmt.Fprintf(writer, "# HELP agentmesh_http_request_duration_seconds HTTP request duration sum.\n# TYPE agentmesh_http_request_duration_seconds summary\nagentmesh_http_request_duration_seconds_sum %g\nagentmesh_http_request_duration_seconds_count %d\n", httpDuration, requestCount)
	writeCounterMap(writer, "agentmesh_run_events_total", "Run lifecycle events by bounded type.", runEvents, func(key string) string { return fmt.Sprintf("type=%q", key) })
	writeCounterMap(writer, "agentmesh_routing_decisions_total", "Automatic routing decisions by bounded strategy.", routingDecisions, func(key string) string { return fmt.Sprintf("strategy=%q", key) })
	return nil
}

func (r *Registry) Handler(repository store.RunRepository) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, request *http.Request) {
		var output strings.Builder
		if err := r.WritePrometheus(request.Context(), &output, repository); err != nil {
			http.Error(w, "metrics are unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, output.String())
	})
	return r.HTTPMiddleware(mux)
}

func WrapBroker(inner events.Broker, registry *Registry) events.Broker {
	if registry == nil {
		return inner
	}
	return &broker{inner: inner, registry: registry}
}

type broker struct {
	inner    events.Broker
	registry *Registry
}

func (b *broker) Publish(event domain.RunEvent) {
	b.registry.ObserveRunEvent(event.Type)
	b.inner.Publish(event)
}

func (b *broker) Subscribe(runID string) (<-chan domain.RunEvent, func()) {
	return b.inner.Subscribe(runID)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(payload []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(payload)
}

func (r *statusRecorder) Flush() {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func knownRunEvent(value string) bool {
	switch value {
	case "run.queued", "run.started", "run.retrying", "run.attempt_timed_out", "run.succeeded",
		"run.failed", "run.canceled", "run.lease_lost", "run.recovered", "run.child_queued", "run.agent_call_queued":
		return true
	default:
		return false
	}
}

func cloneMap(source map[string]uint64) map[string]uint64 {
	result := make(map[string]uint64, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func writeCounterMap(writer io.Writer, name, help string, values map[string]uint64, labels func(string) string) {
	_, _ = fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		_, _ = fmt.Fprintf(writer, "%s{%s} %d\n", name, labels(key), values[key])
	}
}

var _ events.Broker = (*broker)(nil)
