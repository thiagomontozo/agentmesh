//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/cache"
	"github.com/thiagomontozo/agentmesh/internal/coordination"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/httpapi"
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

	fencedRun := domain.Run{
		ID: "run_fence_" + suffix, AgentID: agent.ID, Input: "fence", Status: domain.RunQueued,
		MaxAttempts: 2, CreatedAt: time.Now().UTC(),
	}
	if _, _, err := repository.CreateRun(ctx, fencedRun, ""); err != nil {
		t.Fatal(err)
	}
	firstFence, err := repository.ClaimRunExecution(ctx, fencedRun.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := fencedRun.Start(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repository.UpdateRunFenced(ctx, fencedRun, firstFence); err != nil {
		t.Fatal(err)
	}
	secondFence, err := repository.ClaimRunExecution(ctx, fencedRun.ID, 2)
	if err != nil || secondFence <= firstFence {
		t.Fatalf("claim newer PostgreSQL fence: first=%d second=%d err=%v", firstFence, secondFence, err)
	}
	staleFencedRun := fencedRun
	if err := staleFencedRun.Succeed("stale", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repository.UpdateRunFenced(ctx, staleFencedRun, firstFence); !errors.Is(err, store.ErrStaleExecution) {
		t.Fatalf("expected PostgreSQL to reject stale fence, got %v", err)
	}
	currentFencedRun := fencedRun
	if err := currentFencedRun.Succeed("current", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repository.UpdateRunFenced(ctx, currentFencedRun, secondFence); err != nil {
		t.Fatal(err)
	}
	loadedFencedRun, err := repository.GetRun(ctx, fencedRun.ID)
	if err != nil || loadedFencedRun.Output != "current" {
		t.Fatalf("current PostgreSQL fence did not win: run=%+v err=%v", loadedFencedRun, err)
	}

	recoveryRun := domain.Run{
		ID: "run_recovery_" + suffix, AgentID: agent.ID, Input: "recovery", Status: domain.RunQueued,
		MaxAttempts: 2, CreatedAt: time.Now().UTC(),
	}
	if _, _, err := repository.CreateRun(ctx, recoveryRun, ""); err != nil {
		t.Fatal(err)
	}
	activeLease, acquired, err := redisCache.Acquire(ctx, "run:"+recoveryRun.ID, 200*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("instance A recovery lease: acquired=%v err=%v", acquired, err)
	}
	activeFence, err := repository.ClaimRunExecution(ctx, recoveryRun.ID, activeLease.FencingToken())
	if err != nil {
		t.Fatal(err)
	}
	if err := recoveryRun.Start(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repository.UpdateRunFenced(ctx, recoveryRun, activeFence); err != nil {
		t.Fatal(err)
	}
	if err := runEngine.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	stillRunning, err := repository.GetRun(ctx, recoveryRun.ID)
	if err != nil || stillRunning.Status != domain.RunRunning {
		t.Fatalf("healthy PostgreSQL Run was recovered: run=%+v err=%v", stillRunning, err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := runEngine.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, ctx, repository, recoveryRun.ID, domain.RunSucceeded)
	staleRecovery := recoveryRun
	if err := staleRecovery.Succeed("crashed owner", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repository.UpdateRunFenced(ctx, staleRecovery, activeFence); !errors.Is(err, store.ErrStaleExecution) {
		t.Fatalf("recovered PostgreSQL Run accepted crashed owner: %v", err)
	}
	testRedisLeaseRenewal(t, ctx, redisCache, "integration:"+suffix)
	testDistributedEventBus(t, natsURL, "run_events_"+suffix)
	testDistributedSSE(t, natsURL, repository, runEngine, agent.ID, suffix)
}

func testDistributedSSE(
	t *testing.T,
	natsURL string,
	repository store.Repository,
	runEngine *engine.Engine,
	agentID string,
	suffix string,
) {
	t.Helper()
	run := domain.Run{
		ID: "run_sse_" + suffix, AgentID: agentID, Input: "events", Status: domain.RunQueued,
		MaxAttempts: 1, CreatedAt: time.Now().UTC(),
	}
	if _, _, err := repository.CreateRun(context.Background(), run, ""); err != nil {
		t.Fatal(err)
	}
	busA, err := events.NewNATS(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busA.Close() }()
	busB, err := events.NewNATS(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busB.Close() }()
	api := httpapi.New(repository, runEngine, busA)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+run.ID+"/events", nil)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		api.Handler().ServeHTTP(response, request)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	busB.Publish(domain.RunEvent{
		RunID: run.ID, Type: "run.succeeded", Message: "remote replica completed", Timestamp: time.Now().UTC(),
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE replica A did not receive terminal event from replica B")
	}
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, "event: run.succeeded") || !strings.Contains(body, run.ID) {
		t.Fatalf("unexpected distributed SSE response: status=%d body=%s", response.Code, body)
	}
}

func testDistributedEventBus(t *testing.T, natsURL, runID string) {
	t.Helper()
	busA, err := events.NewNATS(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busA.Close() }()
	busB, err := events.NewNATS(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busB.Close() }()

	received, unsubscribe := busA.Subscribe(runID)
	defer unsubscribe()
	local, unsubscribeLocal := busB.Subscribe(runID)
	defer unsubscribeLocal()
	wantTypes := []string{"run.queued", "run.started", "run.succeeded"}
	for _, eventType := range wantTypes {
		busB.Publish(domain.RunEvent{
			RunID: runID, Type: eventType, Message: eventType, Timestamp: time.Now().UTC(),
		})
	}
	for _, wantType := range wantTypes {
		assertDistributedEvent(t, received, runID, wantType)
		assertDistributedEvent(t, local, runID, wantType)
	}
	select {
	case duplicate := <-received:
		t.Fatalf("replica A received duplicate event: %+v", duplicate)
	case <-time.After(100 * time.Millisecond):
	}

	replay, unsubscribeReplay := busA.Subscribe(runID)
	defer unsubscribeReplay()
	for _, wantType := range wantTypes {
		assertDistributedEvent(t, replay, runID, wantType)
	}

	slowRunID := runID + "_slow"
	_, unsubscribeSlow := busA.Subscribe(slowRunID)
	defer unsubscribeSlow()
	started := time.Now()
	for i := 0; i < 64; i++ {
		busB.Publish(domain.RunEvent{
			RunID: slowRunID, Type: "run.progress", Message: fmt.Sprintf("event %d", i), Timestamp: time.Now().UTC(),
		})
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("slow subscriber blocked distributed publisher for %s", elapsed)
	}
}

func assertDistributedEvent(t *testing.T, channel <-chan domain.RunEvent, runID, eventType string) {
	t.Helper()
	select {
	case event := <-channel:
		if event.RunID != runID || event.Type != eventType {
			t.Fatalf("unexpected distributed event: got=%+v want_run=%s want_type=%s", event, runID, eventType)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for distributed event %s", eventType)
	}
}

func testRedisLeaseRenewal(t *testing.T, ctx context.Context, coordinator coordination.Coordinator, key string) {
	t.Helper()
	stale, acquired, err := coordinator.Acquire(ctx, key, 200*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("acquire Redis lease: acquired=%v err=%v", acquired, err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := stale.Renew(ctx, 400*time.Millisecond); err != nil {
		t.Fatalf("renew Redis lease: %v", err)
	}
	time.Sleep(250 * time.Millisecond)
	if _, acquired, err := coordinator.Acquire(ctx, key, time.Second); err != nil || acquired {
		t.Fatalf("renewed Redis lease expired early: acquired=%v err=%v", acquired, err)
	}
	time.Sleep(250 * time.Millisecond)
	owner, acquired, err := coordinator.Acquire(ctx, key, time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire Redis lease after expiry: acquired=%v err=%v", acquired, err)
	}
	if owner.FencingToken() <= stale.FencingToken() {
		t.Fatalf("Redis fencing token did not increase: stale=%d owner=%d", stale.FencingToken(), owner.FencingToken())
	}
	if err := stale.Renew(ctx, time.Second); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("expected old Redis owner renewal to fail, got %v", err)
	}
	if err := stale.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := coordinator.Acquire(ctx, key, time.Second); err != nil || acquired {
		t.Fatalf("old Redis owner released current lease: acquired=%v err=%v", acquired, err)
	}
	if err := owner.Release(ctx); err != nil {
		t.Fatal(err)
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
