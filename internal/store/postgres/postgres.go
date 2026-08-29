package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thiagomontozo/agentmesh/internal/domain"
	"github.com/thiagomontozo/agentmesh/internal/store"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Repository struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	repository := &Repository{pool: pool}
	if err := repository.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := repository.Migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return repository, nil
}

func (r *Repository) Migrate(ctx context.Context) error {
	if _, err := r.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migrations: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext('agentmesh_migrations'))"); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}

	entries, err := fs.ReadDir(migrations, "migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var applied bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", entry.Name()).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", entry.Name(), err)
		}
		if applied {
			continue
		}
		sql, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", entry.Name()); err != nil {
			return fmt.Errorf("record migration %s: %w", entry.Name(), err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func (r *Repository) CreateAgent(ctx context.Context, agent domain.Agent) (domain.Agent, error) {
	if err := agent.NormalizeAndValidate(); err != nil {
		return domain.Agent{}, err
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO agents (
			id, name, system_prompt, runtime, protocol, endpoint, capabilities, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		agent.ID, agent.Name, agent.SystemPrompt, agent.Runtime, agent.Protocol,
		agent.Endpoint, agent.Capabilities, agent.CreatedAt,
	)
	if err != nil {
		return domain.Agent{}, fmt.Errorf("insert agent: %w", err)
	}
	return agent, nil
}

func (r *Repository) GetAgent(ctx context.Context, id string) (domain.Agent, error) {
	var agent domain.Agent
	err := r.pool.QueryRow(ctx,
		`SELECT id, name, system_prompt, runtime, protocol, endpoint, capabilities, created_at
		 FROM agents WHERE id = $1`, id,
	).Scan(
		&agent.ID, &agent.Name, &agent.SystemPrompt, &agent.Runtime, &agent.Protocol,
		&agent.Endpoint, &agent.Capabilities, &agent.CreatedAt,
	)
	if err == pgx.ErrNoRows {
		return domain.Agent{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Agent{}, fmt.Errorf("select agent: %w", err)
	}
	return agent, nil
}

func (r *Repository) ListAgents(ctx context.Context) ([]domain.Agent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, system_prompt, runtime, protocol, endpoint, capabilities, created_at
		FROM agents ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	defer rows.Close()
	result := make([]domain.Agent, 0)
	for rows.Next() {
		var agent domain.Agent
		if err := rows.Scan(
			&agent.ID, &agent.Name, &agent.SystemPrompt, &agent.Runtime, &agent.Protocol,
			&agent.Endpoint, &agent.Capabilities, &agent.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, agent)
	}
	return result, rows.Err()
}

func (r *Repository) CreateRun(ctx context.Context, run domain.Run, idempotencyKey string) (domain.Run, bool, error) {
	var key any
	if idempotencyKey != "" {
		key = idempotencyKey
	}
	command, err := r.pool.Exec(ctx, `
		INSERT INTO runs (
			id, agent_id, input, output, status, error, attempt, max_attempts,
			idempotency_key, created_at, started_at, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT DO NOTHING`,
		run.ID, run.AgentID, run.Input, run.Output, run.Status, run.Error, run.Attempt,
		run.MaxAttempts, key, run.CreatedAt, run.StartedAt, run.CompletedAt,
	)
	if err != nil {
		return domain.Run{}, false, fmt.Errorf("insert run: %w", err)
	}
	if command.RowsAffected() == 1 {
		return run, true, nil
	}
	if idempotencyKey == "" {
		return domain.Run{}, false, fmt.Errorf("run already exists")
	}
	existing, err := scanRun(r.pool.QueryRow(ctx, runSelect+" WHERE idempotency_key = $1", idempotencyKey))
	if err != nil {
		return domain.Run{}, false, fmt.Errorf("select idempotent run: %w", err)
	}
	return existing, false, nil
}

const runSelect = `SELECT id, agent_id, input, output, status, error, attempt, max_attempts,
	created_at, started_at, completed_at FROM runs`

func (r *Repository) GetRun(ctx context.Context, id string) (domain.Run, error) {
	run, err := scanRun(r.pool.QueryRow(ctx, runSelect+" WHERE id = $1", id))
	if err == pgx.ErrNoRows {
		return domain.Run{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Run{}, fmt.Errorf("select run: %w", err)
	}
	return run, nil
}

func (r *Repository) UpdateRun(ctx context.Context, run domain.Run) error {
	command, err := r.pool.Exec(ctx, `
		UPDATE runs SET output = $2, status = $3, error = $4, attempt = $5,
			max_attempts = $6, started_at = $7, completed_at = $8
		WHERE id = $1 AND status <> 'canceled' AND execution_fence = 0`,
		run.ID, run.Output, run.Status, run.Error, run.Attempt, run.MaxAttempts,
		run.StartedAt, run.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("update run: %w", err)
	}
	if command.RowsAffected() == 0 {
		return r.classifyRunWriteFailure(ctx, run.ID, 0)
	}
	return nil
}

func (r *Repository) ClaimRunExecution(ctx context.Context, id string, minimumFence int64) (int64, error) {
	var fence int64
	err := r.pool.QueryRow(ctx, `
		UPDATE runs
		SET execution_fence = GREATEST(execution_fence + 1, $2)
		WHERE id = $1 AND status IN ('queued', 'running')
		RETURNING execution_fence`, id, minimumFence).Scan(&fence)
	if err == nil {
		return fence, nil
	}
	if err != pgx.ErrNoRows {
		return 0, fmt.Errorf("claim run execution: %w", err)
	}
	status, _, stateErr := r.runStateAndFence(ctx, id)
	if stateErr != nil {
		return 0, stateErr
	}
	if status == domain.RunCanceled {
		return 0, store.ErrRunCanceled
	}
	return 0, fmt.Errorf("%w from status %s", store.ErrRunNotExecutable, status)
}

func (r *Repository) UpdateRunFenced(ctx context.Context, run domain.Run, fence int64) error {
	command, err := r.pool.Exec(ctx, `
		UPDATE runs SET output = $2, status = $3, error = $4, attempt = $5,
			max_attempts = $6, started_at = $7, completed_at = $8
		WHERE id = $1 AND status <> 'canceled' AND execution_fence = $9`,
		run.ID, run.Output, run.Status, run.Error, run.Attempt, run.MaxAttempts,
		run.StartedAt, run.CompletedAt, fence,
	)
	if err != nil {
		return fmt.Errorf("update fenced run: %w", err)
	}
	if command.RowsAffected() == 0 {
		return r.classifyRunWriteFailure(ctx, run.ID, fence)
	}
	return nil
}

func (r *Repository) classifyRunWriteFailure(ctx context.Context, id string, expectedFence int64) error {
	status, currentFence, err := r.runStateAndFence(ctx, id)
	if err != nil {
		return err
	}
	if status == domain.RunCanceled {
		return store.ErrRunCanceled
	}
	if currentFence != expectedFence || expectedFence <= 0 {
		return fmt.Errorf("%w: current=%d provided=%d", store.ErrStaleExecution, currentFence, expectedFence)
	}
	return fmt.Errorf("update run affected no rows")
}

func (r *Repository) runStateAndFence(ctx context.Context, id string) (domain.RunStatus, int64, error) {
	var status domain.RunStatus
	var fence int64
	err := r.pool.QueryRow(ctx, "SELECT status, execution_fence FROM runs WHERE id = $1", id).Scan(&status, &fence)
	if err == pgx.ErrNoRows {
		return "", 0, store.ErrNotFound
	}
	if err != nil {
		return "", 0, fmt.Errorf("select run execution fence: %w", err)
	}
	return status, fence, nil
}

func (r *Repository) CancelRun(ctx context.Context, id string, at time.Time) (domain.Run, error) {
	run, err := scanRun(r.pool.QueryRow(ctx, `
		UPDATE runs SET status = 'canceled', output = '', error = '', completed_at = $2
		WHERE id = $1 AND status IN ('queued', 'running')
		RETURNING id, agent_id, input, output, status, error, attempt, max_attempts,
			created_at, started_at, completed_at`, id, at.UTC()))
	if err == nil {
		return run, nil
	}
	if err != pgx.ErrNoRows {
		return domain.Run{}, fmt.Errorf("cancel run: %w", err)
	}
	existing, getErr := r.GetRun(ctx, id)
	if getErr != nil {
		return domain.Run{}, getErr
	}
	return existing, fmt.Errorf("%w from status %s", domain.ErrRunNotCancelable, existing.Status)
}

func (r *Repository) ListRuns(ctx context.Context) ([]domain.Run, error) {
	rows, err := r.pool.Query(ctx, runSelect+" ORDER BY created_at, id")
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()
	result := make([]domain.Run, 0)
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, run)
	}
	return result, rows.Err()
}

func (r *Repository) RecoverPendingRuns(ctx context.Context) ([]string, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		UPDATE runs SET status = 'queued', started_at = NULL, attempt = GREATEST(attempt - 1, 0)
		WHERE status = 'running'`); err != nil {
		return nil, fmt.Errorf("reset running runs: %w", err)
	}
	rows, err := tx.Query(ctx, "SELECT id FROM runs WHERE status = 'queued' ORDER BY created_at, id")
	if err != nil {
		return nil, err
	}
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		result = append(result, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Repository) Ping(ctx context.Context) error {
	if err := r.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return nil
}

func (r *Repository) Close() { r.pool.Close() }

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRun(row rowScanner) (domain.Run, error) {
	var run domain.Run
	err := row.Scan(
		&run.ID, &run.AgentID, &run.Input, &run.Output, &run.Status, &run.Error,
		&run.Attempt, &run.MaxAttempts, &run.CreatedAt, &run.StartedAt, &run.CompletedAt,
	)
	return run, err
}

var _ store.Repository = (*Repository)(nil)
