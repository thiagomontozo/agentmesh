package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/cache"
	"github.com/thiagomontozo/agentmesh/internal/domain"
)

type Cached struct {
	inner Repository
	cache cache.Cache
	ttl   time.Duration
}

func NewCached(inner Repository, c cache.Cache, ttl time.Duration) *Cached {
	return &Cached{inner: inner, cache: c, ttl: ttl}
}

func (c *Cached) CreateAgent(ctx context.Context, agent domain.Agent) (domain.Agent, error) {
	created, err := c.inner.CreateAgent(ctx, agent)
	if err != nil {
		return domain.Agent{}, err
	}
	c.set(ctx, agentKey(created.ID), created)
	return created, nil
}

func (c *Cached) GetAgent(ctx context.Context, id string) (domain.Agent, error) {
	var agent domain.Agent
	found, err := c.cache.Get(ctx, agentKey(id), &agent)
	if err != nil {
		slog.Warn("agent cache read failed", "agent_id", id, "error", err)
	}
	if found && agent.Version >= 1 {
		return agent, nil
	}
	agent, err = c.inner.GetAgent(ctx, id)
	if err != nil {
		return domain.Agent{}, err
	}
	c.set(ctx, agentKey(id), agent)
	return agent, nil
}

func (c *Cached) UpdateAgent(ctx context.Context, agent domain.Agent, expectedVersion int64) (domain.Agent, error) {
	updated, err := c.inner.UpdateAgent(ctx, agent, expectedVersion)
	if err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
			c.delete(ctx, agentKey(agent.ID))
		}
		return domain.Agent{}, err
	}
	c.set(ctx, agentKey(updated.ID), updated)
	return updated, nil
}

func (c *Cached) DeleteAgent(ctx context.Context, id string, expectedVersion int64) error {
	err := c.inner.DeleteAgent(ctx, id, expectedVersion)
	if err == nil || errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		c.delete(ctx, agentKey(id))
	}
	return err
}

func (c *Cached) ListAgents(ctx context.Context) ([]domain.Agent, error) {
	return c.inner.ListAgents(ctx)
}

func (c *Cached) ListAgentsByCapability(ctx context.Context, capability string) ([]domain.Agent, error) {
	return c.inner.ListAgentsByCapability(ctx, capability)
}

func (c *Cached) FindAgents(ctx context.Context, filter AgentFilter) ([]domain.Agent, error) {
	return c.inner.FindAgents(ctx, filter)
}

func (c *Cached) CreateRun(ctx context.Context, run domain.Run, idempotencyKey string) (domain.Run, bool, error) {
	created, isNew, err := c.inner.CreateRun(ctx, run, idempotencyKey)
	if err != nil {
		return domain.Run{}, false, err
	}
	c.set(ctx, runKey(created.ID), created)
	return created, isNew, nil
}

func (c *Cached) CreateChildRun(ctx context.Context, run domain.Run, idempotencyKey string, maxChildren int) (domain.Run, bool, error) {
	created, isNew, err := c.inner.CreateChildRun(ctx, run, idempotencyKey, maxChildren)
	if err != nil {
		return domain.Run{}, false, err
	}
	c.set(ctx, runKey(created.ID), created)
	return created, isNew, nil
}

func (c *Cached) GetRun(ctx context.Context, id string) (domain.Run, error) {
	var run domain.Run
	found, err := c.cache.Get(ctx, runKey(id), &run)
	if err != nil {
		slog.Warn("run cache read failed", "run_id", id, "error", err)
	}
	if found {
		return run, nil
	}
	run, err = c.inner.GetRun(ctx, id)
	if err != nil {
		return domain.Run{}, err
	}
	c.set(ctx, runKey(id), run)
	return run, nil
}

func (c *Cached) GetRunByIdempotencyKey(ctx context.Context, key string) (domain.Run, error) {
	return c.inner.GetRunByIdempotencyKey(ctx, key)
}

func (c *Cached) UpdateRun(ctx context.Context, run domain.Run) error {
	if err := c.inner.UpdateRun(ctx, run); err != nil {
		if errors.Is(err, ErrRunCanceled) || errors.Is(err, ErrStaleExecution) {
			c.delete(ctx, runKey(run.ID))
		}
		return err
	}
	c.set(ctx, runKey(run.ID), run)
	return nil
}

func (c *Cached) ClaimRunExecution(ctx context.Context, id string, minimumFence int64) (int64, error) {
	fence, err := c.inner.ClaimRunExecution(ctx, id, minimumFence)
	if err != nil {
		c.delete(ctx, runKey(id))
	}
	return fence, err
}

func (c *Cached) UpdateRunFenced(ctx context.Context, run domain.Run, fence int64) error {
	if err := c.inner.UpdateRunFenced(ctx, run, fence); err != nil {
		if errors.Is(err, ErrRunCanceled) || errors.Is(err, ErrStaleExecution) {
			c.delete(ctx, runKey(run.ID))
		}
		return err
	}
	c.set(ctx, runKey(run.ID), run)
	return nil
}

func (c *Cached) CancelRun(ctx context.Context, id string, at time.Time) (domain.Run, error) {
	run, err := c.inner.CancelRun(ctx, id, at)
	if run.ID != "" {
		c.set(ctx, runKey(run.ID), run)
	}
	return run, err
}

func (c *Cached) ListRuns(ctx context.Context) ([]domain.Run, error) {
	return c.inner.ListRuns(ctx)
}

func (c *Cached) ListChildRuns(ctx context.Context, parentRunID string) ([]domain.Run, error) {
	return c.inner.ListChildRuns(ctx, parentRunID)
}

func (c *Cached) CountActiveRunsByAgent(ctx context.Context, agentIDs []string) (map[string]int, error) {
	return c.inner.CountActiveRunsByAgent(ctx, agentIDs)
}

func (c *Cached) ListPendingRuns(ctx context.Context) ([]PendingRun, error) {
	return c.inner.ListPendingRuns(ctx)
}

func (c *Cached) RecoverRun(ctx context.Context, id string, minimumFence int64) (bool, error) {
	recovered, err := c.inner.RecoverRun(ctx, id, minimumFence)
	if err != nil {
		return false, err
	}
	if recovered {
		c.delete(ctx, runKey(id))
	}
	return recovered, nil
}

func (c *Cached) AppendRunEvent(ctx context.Context, event domain.RunEvent, retention time.Duration, maxPerRun int) error {
	return c.inner.AppendRunEvent(ctx, event, retention, maxPerRun)
}

func (c *Cached) ListRunEvents(ctx context.Context, runID string, limit int) ([]domain.RunEvent, error) {
	return c.inner.ListRunEvents(ctx, runID, limit)
}

func (c *Cached) CreateWorkflow(ctx context.Context, workflow domain.Workflow) (domain.Workflow, error) {
	return c.inner.CreateWorkflow(ctx, workflow)
}

func (c *Cached) GetWorkflow(ctx context.Context, id string) (domain.Workflow, error) {
	return c.inner.GetWorkflow(ctx, id)
}

func (c *Cached) ListWorkflows(ctx context.Context) ([]domain.Workflow, error) {
	return c.inner.ListWorkflows(ctx)
}

func (c *Cached) UpdateWorkflow(ctx context.Context, workflow domain.Workflow, expectedVersion int64) (domain.Workflow, error) {
	return c.inner.UpdateWorkflow(ctx, workflow, expectedVersion)
}

func (c *Cached) ListRunningWorkflows(ctx context.Context) ([]domain.Workflow, error) {
	return c.inner.ListRunningWorkflows(ctx)
}

func (c *Cached) AppendWorkflowEvent(ctx context.Context, event domain.WorkflowEvent, retention time.Duration, maxPerWorkflow int) error {
	return c.inner.AppendWorkflowEvent(ctx, event, retention, maxPerWorkflow)
}

func (c *Cached) ListWorkflowEvents(ctx context.Context, workflowID string, limit int) ([]domain.WorkflowEvent, error) {
	return c.inner.ListWorkflowEvents(ctx, workflowID, limit)
}

func (c *Cached) AppendAuditEvent(ctx context.Context, event domain.AuditEvent, retention time.Duration, maxEvents int) error {
	return c.inner.AppendAuditEvent(ctx, event, retention, maxEvents)
}

func (c *Cached) ListAuditEvents(ctx context.Context, limit int) ([]domain.AuditEvent, error) {
	return c.inner.ListAuditEvents(ctx, limit)
}

func (c *Cached) CreateApproval(ctx context.Context, approval domain.Approval, retention time.Duration) (domain.Approval, error) {
	return c.inner.CreateApproval(ctx, approval, retention)
}

func (c *Cached) GetApproval(ctx context.Context, id string) (domain.Approval, error) {
	return c.inner.GetApproval(ctx, id)
}

func (c *Cached) ListApprovals(ctx context.Context, status domain.ApprovalStatus, limit int) ([]domain.Approval, error) {
	return c.inner.ListApprovals(ctx, status, limit)
}

func (c *Cached) DecideApproval(ctx context.Context, id string, approve bool, actor string, now time.Time) (domain.Approval, error) {
	return c.inner.DecideApproval(ctx, id, approve, actor, now)
}

func (c *Cached) ConsumeApproval(ctx context.Context, id, serverID, toolName, argumentsHash string, now time.Time) (domain.Approval, error) {
	return c.inner.ConsumeApproval(ctx, id, serverID, toolName, argumentsHash, now)
}

func (c *Cached) Ping(ctx context.Context) error {
	if err := c.inner.Ping(ctx); err != nil {
		return err
	}
	if err := c.cache.Ping(ctx); err != nil {
		return fmt.Errorf("cache: %w", err)
	}
	return nil
}

func agentKey(id string) string { return "agentmesh:agent:" + id }
func runKey(id string) string   { return "agentmesh:run:" + id }

func (c *Cached) set(ctx context.Context, key string, value any) {
	if err := c.cache.Set(ctx, key, value, c.ttl); err != nil {
		slog.Warn("cache write failed", "key", key, "error", err)
	}
}

func (c *Cached) delete(ctx context.Context, keys ...string) {
	if err := c.cache.Delete(ctx, keys...); err != nil {
		slog.Warn("cache invalidation failed", "error", err)
	}
}

var _ Repository = (*Cached)(nil)
