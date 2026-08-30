//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/thiagomontozo/agentmesh/internal/cache"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/httpapi"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	"github.com/thiagomontozo/agentmesh/internal/store"
	postgresstore "github.com/thiagomontozo/agentmesh/internal/store/postgres"
)

func TestRealMultiReplicaControlPlane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	databaseURL := env("AGENTMESH_DATABASE_URL", "postgres://agentmesh:agentmesh@localhost:5432/agentmesh?sslmode=disable")
	natsURL := env("AGENTMESH_NATS_URL", "nats://localhost:4222")
	redisURL := env("AGENTMESH_REDIS_URL", "redis://localhost:6379/0")

	postgresA, err := postgresstore.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer postgresA.Close()
	postgresB, err := postgresstore.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer postgresB.Close()
	redisA, err := cache.NewRedis(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = redisA.Close() }()
	redisB, err := cache.NewRedis(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = redisB.Close() }()
	queueA, err := queue.NewNATS(ctx, natsURL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	queueB, err := queue.NewNATS(ctx, natsURL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	repositoryA := store.NewCached(postgresA, redisA, time.Minute)
	repositoryB := store.NewCached(postgresB, redisB, time.Minute)
	busA, err := events.NewPersistentNATS(natsURL, repositoryA, time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busA.Close() }()
	busB, err := events.NewPersistentNATS(natsURL, repositoryB, time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busB.Close() }()

	var callsMu sync.Mutex
	calls := make(map[string]map[string]int)
	holdStarted := make(chan struct{})
	holdRelease := make(chan struct{})
	executor := func(replica string) executorFunc {
		return func(ctx context.Context, _ domain.Agent, input string) (string, error) {
			callsMu.Lock()
			if calls[replica] == nil {
				calls[replica] = make(map[string]int)
			}
			calls[replica][input]++
			callsMu.Unlock()
			switch input {
			case "hold-on-b":
				select {
				case <-holdStarted:
				default:
					close(holdStarted)
				}
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-holdRelease:
				}
			case "force-dlq":
				return "", errors.New("intentional multi-replica failure")
			}
			return replica + ":" + input, nil
		}
	}
	retry := engine.RetryPolicy{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, LeaseTTL: 300 * time.Millisecond, AttemptTimeout: 3 * time.Second}
	engineA := engine.New(repositoryA, busA, executor("A"), queueA, redisA, 2, retry)
	engineA.SetInstanceID("replica-a")
	engineB := engine.New(repositoryB, busB, executor("B"), queueB, redisB, 2, retry)
	engineB.SetInstanceID("replica-b")
	ctxA, stopA := context.WithCancel(context.Background())
	ctxB, stopB := context.WithCancel(context.Background())
	engineAStarted := false
	engineBStopped := false
	defer func() {
		if !engineBStopped {
			stopB()
			engineB.Stop()
		}
		stopA()
		if engineAStarted {
			engineA.Stop()
		} else {
			_ = queueA.Close()
		}
	}()
	engineB.Start(ctxB)

	apiA := httptest.NewServer(httpapi.NewWithInstanceID(repositoryA, engineA, busA, "replica-a").Handler())
	defer apiA.Close()
	apiB := httptest.NewServer(httpapi.NewWithInstanceID(repositoryB, engineB, busB, "replica-b").Handler())
	defer apiB.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	agent, err := repositoryA.CreateAgent(ctx, domain.Agent{ID: "agt_multi_" + suffix, Name: "multi-replica"})
	if err != nil {
		t.Fatal(err)
	}
	normalRun := submitReplicaRun(t, apiA.URL, agent.ID, "normal-on-b", "multi-normal-"+suffix)
	completedNormal := waitReplicaRun(t, ctx, repositoryB, normalRun.ID, domain.RunSucceeded)
	if completedNormal.Output != "B:normal-on-b" {
		t.Fatalf("Run submitted to A was not executed by B: %+v", completedNormal)
	}
	replayed := submitReplicaRun(t, apiA.URL, agent.ID, "normal-on-b", "multi-normal-"+suffix)
	if replayed.ID != normalRun.ID {
		t.Fatalf("idempotency returned a different Run: first=%s replay=%s", normalRun.ID, replayed.ID)
	}
	loadedFromB := getReplicaRun(t, apiB.URL, normalRun.ID)
	loadedFromA := getReplicaRun(t, apiA.URL, normalRun.ID)
	if loadedFromA.Status != domain.RunSucceeded || loadedFromB.Status != domain.RunSucceeded {
		t.Fatalf("replicas disagree on state: A=%+v B=%+v", loadedFromA, loadedFromB)
	}
	callsMu.Lock()
	normalCalls := calls["B"]["normal-on-b"]
	callsMu.Unlock()
	if normalCalls != 1 {
		t.Fatalf("normal Run executed %d times", normalCalls)
	}

	holdRun := submitReplicaRun(t, apiA.URL, agent.ID, "hold-on-b", "multi-hold-"+suffix)
	select {
	case <-holdStarted:
	case <-ctx.Done():
		t.Fatal("replica B did not start held Run")
	}
	sseDone := make(chan string, 1)
	go func() {
		response, err := http.Get(apiA.URL + "/api/v1/runs/" + holdRun.ID + "/events")
		if err != nil {
			sseDone <- "error:" + err.Error()
			return
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		sseDone <- string(body)
	}()
	time.Sleep(100 * time.Millisecond)
	if err := engineA.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if stillRunning, err := repositoryA.GetRun(ctx, holdRun.ID); err != nil || stillRunning.Status != domain.RunRunning {
		t.Fatalf("replica A recovery interfered with valid B execution: run=%+v err=%v", stillRunning, err)
	}
	close(holdRelease)
	waitReplicaRun(t, ctx, repositoryB, holdRun.ID, domain.RunSucceeded)
	select {
	case body := <-sseDone:
		if !strings.Contains(body, "event: run.succeeded") || !strings.Contains(body, holdRun.ID) {
			t.Fatalf("SSE on A missed event produced by B: %s", body)
		}
	case <-ctx.Done():
		t.Fatal("SSE on A did not finish")
	}

	dlqConnection, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer dlqConnection.Close()
	dlqSubscription, err := dlqConnection.SubscribeSync("agentmesh.runs.dlq")
	if err != nil {
		t.Fatal(err)
	}
	if err := dlqConnection.Flush(); err != nil {
		t.Fatal(err)
	}
	failedRun := submitReplicaRun(t, apiA.URL, agent.ID, "force-dlq", "multi-dlq-"+suffix)
	waitReplicaRun(t, ctx, repositoryB, failedRun.ID, domain.RunFailed)
	dlqDeadline := time.Now().Add(3 * time.Second)
	for {
		dlqMessage, err := dlqSubscription.NextMsg(time.Until(dlqDeadline))
		if err != nil {
			t.Fatalf("DLQ did not receive failed Run %s: %v", failedRun.ID, err)
		}
		if bytes.Contains(dlqMessage.Data, []byte(failedRun.ID)) {
			break
		}
	}

	stopB()
	engineB.Stop()
	engineBStopped = true
	abandoned := domain.Run{ID: "run_abandoned_" + suffix, AgentID: agent.ID, Input: "recovered-on-a", Status: domain.RunQueued, MaxAttempts: 1, CreatedAt: time.Now().UTC()}
	if _, _, err := repositoryA.CreateRun(ctx, abandoned, "multi-abandoned-"+suffix); err != nil {
		t.Fatal(err)
	}
	crashedLease, acquired, err := redisB.Acquire(ctx, "run:"+abandoned.ID, 150*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("simulate B lease: acquired=%v err=%v", acquired, err)
	}
	fence, err := repositoryB.ClaimRunExecution(ctx, abandoned.ID, crashedLease.FencingToken())
	if err != nil {
		t.Fatal(err)
	}
	if err := abandoned.Start(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repositoryB.UpdateRunFenced(ctx, abandoned, fence); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if err := engineA.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	engineA.Start(ctxA)
	engineAStarted = true
	recovered := waitReplicaRun(t, ctx, repositoryA, abandoned.ID, domain.RunSucceeded)
	if recovered.Output != "A:recovered-on-a" {
		t.Fatalf("A did not recover abandoned B Run: %+v", recovered)
	}
}

func submitReplicaRun(t *testing.T, baseURL, agentID, input, key string) domain.Run {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"agent_id": agentID, "input": input})
	request, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/runs", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("submit Run: status=%d body=%s", response.StatusCode, body)
	}
	var run domain.Run
	if err := json.NewDecoder(response.Body).Decode(&run); err != nil {
		t.Fatal(err)
	}
	return run
}

func getReplicaRun(t *testing.T, baseURL, runID string) domain.Run {
	t.Helper()
	response, err := http.Get(baseURL + "/api/v1/runs/" + runID)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("get Run %s: status=%d", runID, response.StatusCode)
	}
	var run domain.Run
	if err := json.NewDecoder(response.Body).Decode(&run); err != nil {
		t.Fatal(err)
	}
	return run
}

func waitReplicaRun(t *testing.T, ctx context.Context, repository store.Repository, runID string, status domain.RunStatus) domain.Run {
	t.Helper()
	for {
		run, err := repository.GetRun(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == status {
			return run
		}
		if (run.Status == domain.RunSucceeded || run.Status == domain.RunFailed || run.Status == domain.RunCanceled) && run.Status != status {
			t.Fatalf("Run %s ended in %s instead of %s", runID, run.Status, status)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for Run %s to reach %s", runID, status)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
