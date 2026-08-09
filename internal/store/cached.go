package store

import (
	"context"
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
	if found {
		return agent, nil
	}
	agent, err = c.inner.GetAgent(ctx, id)
	if err != nil {
		return domain.Agent{}, err
	}
	c.set(ctx, agentKey(id), agent)
	return agent, nil
}

func (c *Cached) ListAgents(ctx context.Context) ([]domain.Agent, error) {
	return c.inner.ListAgents(ctx)
}

func (c *Cached) CreateRun(ctx context.Context, run domain.Run, idempotencyKey string) (domain.Run, bool, error) {
	created, isNew, err := c.inner.CreateRun(ctx, run, idempotencyKey)
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

func (c *Cached) UpdateRun(ctx context.Context, run domain.Run) error {
	if err := c.inner.UpdateRun(ctx, run); err != nil {
		return err
	}
	c.set(ctx, runKey(run.ID), run)
	return nil
}

func (c *Cached) ListRuns(ctx context.Context) ([]domain.Run, error) {
	return c.inner.ListRuns(ctx)
}

func (c *Cached) RecoverPendingRuns(ctx context.Context) ([]string, error) {
	ids, err := c.inner.RecoverPendingRuns(ctx)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, runKey(id))
	}
	if err := c.cache.Delete(ctx, keys...); err != nil {
		slog.Warn("run cache invalidation failed", "error", err)
	}
	return ids, nil
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

var _ Repository = (*Cached)(nil)
