package agenthealth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

func TestCheckClassifiesHTTPAgentHealth(t *testing.T) {
	for _, test := range []struct {
		name       string
		statusCode int
		want       Status
		wantReason string
	}{
		{name: "healthy", statusCode: http.StatusNoContent, want: StatusHealthy},
		{name: "unhealthy", statusCode: http.StatusServiceUnavailable, want: StatusUnhealthy, wantReason: "http_status_503"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/agent/healthz" {
					t.Errorf("unexpected health path %q", r.URL.Path)
				}
				w.WriteHeader(test.statusCode)
			}))
			defer server.Close()
			service := newTestService(t, store.NewMemory(), 100*time.Millisecond)
			state := service.check(context.Background(), remoteAgent("agt_1", server.URL+"/agent"))
			if state.Status != test.want || state.Reason != test.wantReason || state.LastCheckedAt == nil {
				t.Fatalf("unexpected state: %+v", state)
			}
		})
	}
}

func TestCheckDoesNotFollowRedirects(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusFound))
	defer redirect.Close()
	service := newTestService(t, store.NewMemory(), 100*time.Millisecond)
	state := service.check(context.Background(), remoteAgent("agt_1", redirect.URL))
	if state.Status != StatusUnhealthy || state.Reason != "http_status_302" || targetCalls.Load() != 0 {
		t.Fatalf("redirect was not rejected safely: state=%+v target_calls=%d", state, targetCalls.Load())
	}
}

func TestCheckHonorsShortTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	service := newTestService(t, store.NewMemory(), 10*time.Millisecond)
	started := time.Now()
	state := service.check(context.Background(), remoteAgent("agt_1", server.URL))
	if state.Status != StatusUnhealthy || state.Reason != "timeout" || time.Since(started) > 80*time.Millisecond {
		t.Fatalf("health timeout was not enforced: state=%+v duration=%s", state, time.Since(started))
	}
}

func TestBuildHealthURLRejectsUnsafeEndpoints(t *testing.T) {
	for _, endpoint := range []string{
		"ftp://agent.internal", "http://user:secret@agent.internal", "http://agent.internal?target=other", "relative",
	} {
		if _, err := buildHealthURL(endpoint, "/healthz"); err == nil {
			t.Fatalf("expected endpoint %q to be rejected", endpoint)
		}
	}
}

func TestServiceChecksAgentsWithBoundedWorkers(t *testing.T) {
	repository := store.NewMemory()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	remote := remoteAgent("agt_remote", server.URL)
	legacy := domain.Agent{ID: "agt_legacy", Name: "legacy", CreatedAt: time.Now().UTC()}
	for _, agent := range []domain.Agent{remote, legacy} {
		if _, err := repository.CreateAgent(context.Background(), agent); err != nil {
			t.Fatal(err)
		}
	}
	service := newTestService(t, repository, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	service.Start(ctx)
	defer func() {
		cancel()
		service.Stop()
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if state := service.State(remote.ID); state.Status == StatusHealthy {
			legacyState := service.State(legacy.ID)
			if legacyState.Status != StatusUnknown || legacyState.LastCheckedAt != nil {
				t.Fatalf("legacy Agent should remain unknown: %+v", legacyState)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for health check: %+v", service.State(remote.ID))
}

func newTestService(t *testing.T, repository store.AgentRepository, timeout time.Duration) *Service {
	t.Helper()
	service, err := New(repository, nil, Config{Path: "/healthz", Interval: time.Hour, Timeout: timeout, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func remoteAgent(id, endpoint string) domain.Agent {
	return domain.Agent{
		ID: id, Name: id, Runtime: "remote", Protocol: "http", Endpoint: endpoint, CreatedAt: time.Now().UTC(),
	}
}
