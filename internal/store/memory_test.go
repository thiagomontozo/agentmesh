package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

func TestMemoryAgentLifecycle(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	agent := domain.Agent{ID: "agt_1", Name: "test", CreatedAt: time.Now().UTC()}
	if _, err := memory.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	loaded, err := memory.GetAgent(ctx, agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Name != agent.Name {
		t.Fatalf("expected %q, got %q", agent.Name, loaded.Name)
	}
	if loaded.Version != 1 || loaded.UpdatedAt.IsZero() {
		t.Fatalf("new Agent was not initialized for optimistic concurrency: %+v", loaded)
	}
	agents, err := memory.ListAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(agents); got != 1 {
		t.Fatalf("expected 1 agent, got %d", got)
	}
}

func TestMemoryWorkflowLifecycleAndIsolation(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	for _, agentID := range []string{"agt_a", "agt_b"} {
		if _, err := memory.CreateAgent(ctx, domain.Agent{ID: agentID, Name: agentID}); err != nil {
			t.Fatal(err)
		}
	}
	created, err := memory.CreateWorkflow(ctx, domain.Workflow{ID: "wf_1", Input: "request", Steps: []domain.WorkflowStep{
		{ID: "first", AgentID: "agt_a", InputFrom: []string{"workflow"}},
		{ID: "second", AgentID: "agt_b", DependsOn: []string{"first"}, InputFrom: []string{"first"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != domain.WorkflowPending || created.Steps[1].WorkflowID != created.ID {
		t.Fatalf("unexpected persisted workflow: %+v", created)
	}
	created.Steps[0].DependsOn = []string{"mutated"}
	loaded, err := memory.GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Steps[0].DependsOn) != 0 {
		t.Fatalf("stored workflow was mutated through returned slice: %+v", loaded)
	}
	listed, err := memory.ListWorkflows(ctx)
	if err != nil || len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("unexpected workflow list: %+v err=%v", listed, err)
	}
}

func TestMemoryWorkflowRequiresExistingAgent(t *testing.T) {
	memory := NewMemory()
	_, err := memory.CreateWorkflow(context.Background(), domain.Workflow{ID: "wf_1", Steps: []domain.WorkflowStep{
		{ID: "first", AgentID: "missing", Input: "request"},
	}})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing Agent error, got %v", err)
	}
}

func TestMemoryAtomicallyLimitsChildRuns(t *testing.T) {
	memory := NewMemory()
	ctx := context.Background()
	for _, id := range []string{"agt_parent", "agt_child"} {
		_, _ = memory.CreateAgent(ctx, domain.Agent{ID: id, Name: id})
	}
	parent := domain.Run{ID: "run_parent", AgentID: "agt_parent", Status: domain.RunRunning, CreatedAt: time.Now().UTC()}
	if _, _, err := memory.CreateRun(ctx, parent, ""); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, id := range []string{"run_child_a", "run_child_b"} {
		wait.Add(1)
		go func(id string) {
			defer wait.Done()
			_, _, err := memory.CreateChildRun(ctx, domain.Run{ID: id, AgentID: "agt_child", ParentRunID: parent.ID, Status: domain.RunQueued}, id, 1)
			results <- err
		}(id)
	}
	wait.Wait()
	close(results)
	var created, limited int
	for err := range results {
		if err == nil {
			created++
		} else if errors.Is(err, ErrChildRunLimit) {
			limited++
		} else {
			t.Fatalf("unexpected child creation error: %v", err)
		}
	}
	if created != 1 || limited != 1 {
		t.Fatalf("expected one child and one limit error, got created=%d limited=%d", created, limited)
	}
}

func TestMemoryAgentUpdateAndDeleteLifecycle(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	created, err := memory.CreateAgent(ctx, domain.Agent{ID: "agt_1", Name: "original"})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := memory.UpdateAgent(ctx, domain.Agent{
		ID: created.ID, Name: "updated", Runtime: "remote", Protocol: "http",
		Endpoint: "http://agent:9000", Capabilities: []string{"testing"},
	}, created.Version)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updated.Name != "updated" || !updated.CreatedAt.Equal(created.CreatedAt) || updated.UpdatedAt.Before(created.UpdatedAt) {
		t.Fatalf("unexpected updated Agent: %+v", updated)
	}
	if _, err := memory.UpdateAgent(ctx, domain.Agent{ID: created.ID, Name: "stale"}, created.Version); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected stale update conflict, got %v", err)
	}
	if err := memory.DeleteAgent(ctx, updated.ID, created.Version); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected stale delete conflict, got %v", err)
	}
	if err := memory.DeleteAgent(ctx, updated.ID, updated.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.GetAgent(ctx, updated.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted Agent to be absent, got %v", err)
	}
}

func TestMemoryRejectsAgentDeletionWhenRunDependsOnIt(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	agent, err := memory.CreateAgent(ctx, domain.Agent{ID: "agt_1", Name: "used"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := memory.CreateRun(ctx, domain.Run{ID: "run_1", AgentID: agent.ID, Status: domain.RunSucceeded}, ""); err != nil {
		t.Fatal(err)
	}
	if err := memory.DeleteAgent(ctx, agent.ID, agent.Version); !errors.Is(err, ErrAgentInUse) {
		t.Fatalf("expected dependent Run to protect Agent history, got %v", err)
	}
}

func TestMemoryAgentUpdatesAreOptimisticallyConcurrent(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	agent, err := memory.CreateAgent(ctx, domain.Agent{ID: "agt_1", Name: "original"})
	if err != nil {
		t.Fatal(err)
	}
	errorsChannel := make(chan error, 2)
	var wait sync.WaitGroup
	for _, name := range []string{"first", "second"} {
		wait.Add(1)
		go func(name string) {
			defer wait.Done()
			_, updateErr := memory.UpdateAgent(ctx, domain.Agent{ID: agent.ID, Name: name}, agent.Version)
			errorsChannel <- updateErr
		}(name)
	}
	wait.Wait()
	close(errorsChannel)
	var successes, conflicts int
	for updateErr := range errorsChannel {
		if updateErr == nil {
			successes++
		} else if errors.Is(updateErr, ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent update error: %v", updateErr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected one update and one conflict, got successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestMemoryPersistsAgentExecutionMetadata(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	agent := domain.Agent{
		ID: "agt_remote", Name: "legal", Runtime: "remote", Protocol: "http",
		Endpoint: "http://legal-agent:9000", Capabilities: []string{"legal-search", "legal-analysis"},
		CreatedAt: time.Now().UTC(),
	}
	created, err := memory.CreateAgent(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := memory.GetAgent(ctx, agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Runtime != created.Runtime || loaded.Protocol != created.Protocol || loaded.Endpoint != created.Endpoint {
		t.Fatalf("execution metadata was not preserved: %+v", loaded)
	}
	if len(loaded.Capabilities) != 2 || loaded.Capabilities[1] != "legal-analysis" {
		t.Fatalf("capabilities were not preserved: %#v", loaded.Capabilities)
	}
	agents, err := memory.ListAgents(ctx)
	if err != nil || len(agents) != 1 || agents[0].Runtime != "remote" {
		t.Fatalf("unexpected list result: agents=%+v err=%v", agents, err)
	}

	agent.Capabilities[0] = "mutated-input"
	loaded.Capabilities[0] = "mutated-read"
	agents[0].Capabilities[0] = "mutated-list"
	isolated, err := memory.GetAgent(ctx, agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if isolated.Capabilities[0] != "legal-search" {
		t.Fatalf("stored capabilities were mutated through an external slice: %#v", isolated.Capabilities)
	}
}

func TestMemoryListsAgentsByCanonicalCapability(t *testing.T) {
	memory := NewMemory()
	ctx := context.Background()
	for _, agent := range []domain.Agent{
		{ID: "agt_legal", Name: "Legal", Capabilities: []string{"Legal Analysis", "legal_analysis"}},
		{ID: "agt_code", Name: "Code", Capabilities: []string{"code-review"}},
	} {
		if _, err := memory.CreateAgent(ctx, agent); err != nil {
			t.Fatal(err)
		}
	}
	agents, err := memory.ListAgentsByCapability(ctx, " LEGAL_ANALYSIS ")
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 1 || agents[0].ID != "agt_legal" || len(agents[0].Capabilities) != 1 {
		t.Fatalf("unexpected capability result: %#v", agents)
	}
}

func TestMemoryFindAgentsCombinesDeclaredFilters(t *testing.T) {
	memory := NewMemory()
	ctx := context.Background()
	for _, agent := range []domain.Agent{
		{ID: "agt_http", Name: "HTTP", Runtime: "remote", Protocol: "http", Endpoint: "http://agent", Capabilities: []string{"testing"}},
		{ID: "agt_demo", Name: "Demo", Capabilities: []string{"testing"}},
		{ID: "agt_other", Name: "Other", Runtime: "remote", Protocol: "http", Endpoint: "http://other", Capabilities: []string{"debugging"}},
	} {
		if _, err := memory.CreateAgent(ctx, agent); err != nil {
			t.Fatal(err)
		}
	}
	agents, err := memory.FindAgents(ctx, AgentFilter{Capability: "TESTING", Runtime: "REMOTE", Protocol: "HTTP"})
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 1 || agents[0].ID != "agt_http" {
		t.Fatalf("unexpected combined filter result: %+v", agents)
	}
}

func TestMemoryCountsOnlyActiveRunsForRequestedAgents(t *testing.T) {
	memory := NewMemory()
	ctx := context.Background()
	for _, run := range []domain.Run{
		{ID: "run_queued", AgentID: "agt_a", Status: domain.RunQueued, MaxAttempts: 1},
		{ID: "run_running", AgentID: "agt_a", Status: domain.RunRunning, MaxAttempts: 1},
		{ID: "run_done", AgentID: "agt_a", Status: domain.RunSucceeded, MaxAttempts: 1},
		{ID: "run_other", AgentID: "agt_b", Status: domain.RunQueued, MaxAttempts: 1},
	} {
		if _, _, err := memory.CreateRun(ctx, run, ""); err != nil {
			t.Fatal(err)
		}
	}
	counts, err := memory.CountActiveRunsByAgent(ctx, []string{"agt_a", "agt_missing"})
	if err != nil {
		t.Fatal(err)
	}
	if counts["agt_a"] != 2 || counts["agt_missing"] != 0 || len(counts) != 2 {
		t.Fatalf("unexpected active counts: %#v", counts)
	}
}

func TestMemoryRunLifecycle(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	run := domain.Run{ID: "run_1", Status: domain.RunQueued, MaxAttempts: 3, CreatedAt: time.Now().UTC()}
	if _, _, err := memory.CreateRun(ctx, run, ""); err != nil {
		t.Fatal(err)
	}

	run.Status = domain.RunRunning
	if err := memory.UpdateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	loaded, err := memory.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != domain.RunRunning {
		t.Fatalf("expected %q, got %q", domain.RunRunning, loaded.Status)
	}
	runs, err := memory.ListRuns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runs); got != 1 {
		t.Fatalf("expected 1 run, got %d", got)
	}
}

func TestMemoryUpdateMissingRun(t *testing.T) {
	err := NewMemory().UpdateRun(context.Background(), domain.Run{ID: "missing"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryCancelsRunAndRejectsStaleUpdate(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	original := domain.Run{ID: "run_1", Status: domain.RunRunning, Attempt: 1, MaxAttempts: 3}
	if _, _, err := memory.CreateRun(ctx, original, ""); err != nil {
		t.Fatal(err)
	}
	canceled, err := memory.CancelRun(ctx, original.ID, time.Now())
	if err != nil || canceled.Status != domain.RunCanceled || canceled.CompletedAt == nil {
		t.Fatalf("unexpected cancellation: run=%+v err=%v", canceled, err)
	}

	stale := original
	stale.Status = domain.RunSucceeded
	stale.Output = "must not win"
	if err := memory.UpdateRun(ctx, stale); !errors.Is(err, ErrRunCanceled) {
		t.Fatalf("expected ErrRunCanceled, got %v", err)
	}
	loaded, err := memory.GetRun(ctx, original.ID)
	if err != nil || loaded.Status != domain.RunCanceled || loaded.Output != "" {
		t.Fatalf("stale update replaced cancellation: run=%+v err=%v", loaded, err)
	}
	if _, err := memory.CancelRun(ctx, original.ID, time.Now()); !errors.Is(err, domain.ErrRunNotCancelable) {
		t.Fatalf("expected terminal cancellation conflict, got %v", err)
	}
}

func TestMemoryCancelMissingRun(t *testing.T) {
	if _, err := NewMemory().CancelRun(context.Background(), "missing", time.Now()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryExecutionFenceRejectsStaleWriter(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	run := domain.Run{ID: "run_1", Status: domain.RunQueued, MaxAttempts: 3}
	if _, _, err := memory.CreateRun(ctx, run, ""); err != nil {
		t.Fatal(err)
	}
	firstFence, err := memory.ClaimRunExecution(ctx, run.ID, 10)
	if err != nil || firstFence != 10 {
		t.Fatalf("first execution claim: fence=%d err=%v", firstFence, err)
	}
	if err := run.Start(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := memory.UpdateRunFenced(ctx, run, firstFence); err != nil {
		t.Fatal(err)
	}
	secondFence, err := memory.ClaimRunExecution(ctx, run.ID, 2)
	if err != nil || secondFence <= firstFence {
		t.Fatalf("second execution claim: first=%d second=%d err=%v", firstFence, secondFence, err)
	}
	stale := run
	if err := stale.Succeed("stale", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := memory.UpdateRunFenced(ctx, stale, firstFence); !errors.Is(err, ErrStaleExecution) {
		t.Fatalf("expected stale writer rejection, got %v", err)
	}
	current := run
	if err := current.Succeed("current", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := memory.UpdateRunFenced(ctx, current, secondFence); err != nil {
		t.Fatal(err)
	}
	loaded, err := memory.GetRun(ctx, run.ID)
	if err != nil || loaded.Output != "current" || loaded.Status != domain.RunSucceeded {
		t.Fatalf("new owner did not win: run=%+v err=%v", loaded, err)
	}
	if err := memory.UpdateRun(ctx, stale); !errors.Is(err, ErrStaleExecution) {
		t.Fatalf("expected unfenced writer rejection, got %v", err)
	}
}

func TestMemoryCreateRunIsIdempotent(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	first := domain.Run{ID: "run_1", Status: domain.RunQueued, MaxAttempts: 3}
	created, isNew, err := memory.CreateRun(ctx, first, "request-1")
	if err != nil || !isNew || created.ID != first.ID {
		t.Fatalf("unexpected first creation: run=%+v new=%v err=%v", created, isNew, err)
	}
	replayed, isNew, err := memory.CreateRun(ctx, domain.Run{ID: "run_2"}, "request-1")
	if err != nil || isNew || replayed.ID != first.ID {
		t.Fatalf("unexpected replay: run=%+v new=%v err=%v", replayed, isNew, err)
	}
}

func TestMemoryPersistsBoundedRunEventHistory(t *testing.T) {
	ctx := context.Background()
	memory := NewMemory()
	run := domain.Run{ID: "run_events", Status: domain.RunQueued}
	if _, _, err := memory.CreateRun(ctx, run, ""); err != nil {
		t.Fatal(err)
	}
	for index, eventType := range []string{"run.queued", "run.started", "run.succeeded"} {
		event := domain.RunEvent{
			ID: eventType, RunID: run.ID, Type: eventType, Attempt: index,
			Timestamp: time.Now().UTC().Add(time.Duration(index) * time.Millisecond),
		}
		if err := memory.AppendRunEvent(ctx, event, time.Hour, 2); err != nil {
			t.Fatal(err)
		}
	}
	events, err := memory.ListRunEvents(ctx, run.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Type != "run.started" || events[1].Type != "run.succeeded" || events[1].Attempt != 2 {
		t.Fatalf("unexpected event history: %+v", events)
	}
}

func TestMemoryRunEventHistoryRejectsMissingRun(t *testing.T) {
	memory := NewMemory()
	event := domain.RunEvent{ID: "evt_1", RunID: "missing", Type: "run.queued", Timestamp: time.Now().UTC()}
	if err := memory.AppendRunEvent(context.Background(), event, time.Hour, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryPersistsAndListsParentChildLineage(t *testing.T) {
	memory := NewMemory()
	ctx := context.Background()
	root := domain.Run{ID: "run_root", Status: domain.RunSucceeded, MaxAttempts: 1, CreatedAt: time.Now().UTC()}
	if _, _, err := memory.CreateRun(ctx, root, ""); err != nil {
		t.Fatal(err)
	}
	child := domain.Run{
		ID: "run_child", ParentRunID: root.ID, Status: domain.RunSucceeded,
		MaxAttempts: 1, CreatedAt: root.CreatedAt.Add(time.Millisecond),
	}
	createdChild, _, err := memory.CreateRun(ctx, child, "")
	if err != nil {
		t.Fatal(err)
	}
	if createdChild.ParentRunID != root.ID || createdChild.RootRunID != root.ID {
		t.Fatalf("unexpected child lineage: %+v", createdChild)
	}
	grandchild := domain.Run{
		ID: "run_grandchild", ParentRunID: child.ID, Status: domain.RunSucceeded,
		MaxAttempts: 1, CreatedAt: root.CreatedAt.Add(2 * time.Millisecond),
	}
	createdGrandchild, _, err := memory.CreateRun(ctx, grandchild, "")
	if err != nil {
		t.Fatal(err)
	}
	if createdGrandchild.ParentRunID != child.ID || createdGrandchild.RootRunID != root.ID {
		t.Fatalf("unexpected grandchild lineage: %+v", createdGrandchild)
	}
	children, err := memory.ListChildRuns(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].ID != child.ID {
		t.Fatalf("children query must be direct only: %+v", children)
	}
}

func TestMemoryRejectsInvalidRunLineageWithoutPoisoningIdempotency(t *testing.T) {
	memory := NewMemory()
	ctx := context.Background()
	invalid := domain.Run{ID: "run_invalid", ParentRunID: "run_invalid", Status: domain.RunQueued, MaxAttempts: 1}
	if _, _, err := memory.CreateRun(ctx, invalid, "lineage-key"); err == nil {
		t.Fatal("expected self-parent rejection")
	}
	valid := domain.Run{ID: "run_valid", Status: domain.RunQueued, MaxAttempts: 1}
	created, isNew, err := memory.CreateRun(ctx, valid, "lineage-key")
	if err != nil || !isNew || created.ID != valid.ID {
		t.Fatalf("invalid creation poisoned idempotency: run=%+v new=%v err=%v", created, isNew, err)
	}
	if _, err := memory.ListChildRuns(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing parent error, got %v", err)
	}
}
