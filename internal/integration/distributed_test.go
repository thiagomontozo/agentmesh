//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/cache"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	"github.com/thiagomontozo/agentmesh/internal/store"
	postgresstore "github.com/thiagomontozo/agentmesh/internal/store/postgres"
)

func TestDistributedRunLifecycleAndIdempotency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	databaseURL := env("AGENTMESH_DATABASE_URL", "postgres://agentmesh:agentmesh@localhost:5432/agentmesh?sslmode=disable")
	natsURL := env("AGENTMESH_NATS_URL", "nats://localhost:4222")
	redisURL := env("AGENTMESH_REDIS_URL", "redis://localhost:6379/0")

	postgresRepository, err := postgresstore.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer postgresRepository.Close()
	redisCache, err := cache.NewRedis(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = redisCache.Close() }()
	natsQueue, err := queue.NewNATS(ctx, natsURL, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	repository := store.NewCached(postgresRepository, redisCache, time.Minute)
	executor := executorFunc(func(_ context.Context, agent domain.Agent, input string) (string, error) {
		return agent.Name + ":" + input, nil
	})
	runEngine := engine.New(repository, events.NewBus(), executor, natsQueue, redisCache, 2, engine.RetryPolicy{
		MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, LeaseTTL: time.Minute,
	})
	engineCtx, stopEngine := context.WithCancel(context.Background())
	runEngine.Start(engineCtx)
	defer func() {
		stopEngine()
		runEngine.Stop()
	}()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	agent := domain.Agent{ID: "agt_" + suffix, Name: "integration", CreatedAt: time.Now().UTC()}
	if _, err := repository.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	configuredAgent := domain.Agent{
		ID: "agt_configured_" + suffix, Name: "configured", Runtime: "remote", Protocol: "http",
		Endpoint: "http://agent:9000", Capabilities: []string{"testing", "debugging"}, CreatedAt: time.Now().UTC(),
	}
	if _, err := repository.CreateAgent(ctx, configuredAgent); err != nil {
		t.Fatal(err)
	}
	loadedAgent, err := repository.GetAgent(ctx, configuredAgent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loadedAgent.Runtime != configuredAgent.Runtime || loadedAgent.Protocol != configuredAgent.Protocol ||
		loadedAgent.Endpoint != configuredAgent.Endpoint || !slices.Equal(loadedAgent.Capabilities, configuredAgent.Capabilities) {
		t.Fatalf("agent execution metadata was not persisted: got=%+v want=%+v", loadedAgent, configuredAgent)
	}
	agents, err := repository.ListAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foundConfigured := false
	for _, listed := range agents {
		if listed.ID == configuredAgent.ID {
			foundConfigured = slices.Equal(listed.Capabilities, configuredAgent.Capabilities)
			break
		}
	}
	if !foundConfigured {
		t.Fatalf("configured agent was not listed with its capabilities: %+v", agents)
	}
	run := domain.Run{
		ID: "run_" + suffix, AgentID: agent.ID, Input: "hello", Status: domain.RunQueued,
		MaxAttempts: 3, CreatedAt: time.Now().UTC(),
	}
	created, isNew, err := repository.CreateRun(ctx, run, "integration-"+suffix)
	if err != nil || !isNew {
		t.Fatalf("create run: new=%v err=%v", isNew, err)
	}
	if err := runEngine.Enqueue(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, ctx, repository, created.ID, domain.RunSucceeded)

	replayed, isNew, err := repository.CreateRun(ctx, domain.Run{
		ID: "different_" + suffix, AgentID: agent.ID, Input: run.Input, Status: domain.RunQueued,
		MaxAttempts: 3, CreatedAt: time.Now().UTC(),
	}, "integration-"+suffix)
	if err != nil || isNew || replayed.ID != created.ID {
		t.Fatalf("idempotency replay: run=%+v new=%v err=%v", replayed, isNew, err)
	}
	if err := runEngine.Ready(ctx); err != nil {
		t.Fatal(err)
	}

	cancelCandidate := domain.Run{
		ID: "run_cancel_" + suffix, AgentID: agent.ID, Input: "cancel", Status: domain.RunQueued,
		MaxAttempts: 3, CreatedAt: time.Now().UTC(),
	}
	if _, _, err := repository.CreateRun(ctx, cancelCandidate, ""); err != nil {
		t.Fatal(err)
	}
	canceled, err := repository.CancelRun(ctx, cancelCandidate.ID, time.Now())
	if err != nil || canceled.Status != domain.RunCanceled {
		t.Fatalf("cancel PostgreSQL Run: run=%+v err=%v", canceled, err)
	}
	stale := cancelCandidate
	stale.Status = domain.RunSucceeded
	stale.Output = "must not win"
	if err := repository.UpdateRun(ctx, stale); !errors.Is(err, store.ErrRunCanceled) {
		t.Fatalf("expected stale PostgreSQL update to fail with ErrRunCanceled, got %v", err)
	}
	if _, err := repository.CancelRun(ctx, cancelCandidate.ID, time.Now()); !errors.Is(err, domain.ErrRunNotCancelable) {
		t.Fatalf("expected repeated cancel conflict, got %v", err)
	}
}

type executorFunc func(context.Context, domain.Agent, string) (string, error)

func (f executorFunc) Execute(ctx context.Context, agent domain.Agent, input string) (string, error) {
	return f(ctx, agent, input)
}

func waitForStatus(t *testing.T, ctx context.Context, repository store.Repository, runID string, want domain.RunStatus) {
	t.Helper()
	for {
		run, err := repository.GetRun(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
