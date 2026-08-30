CREATE TABLE IF NOT EXISTS workflows (
    id TEXT PRIMARY KEY,
    input TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')),
    error TEXT NOT NULL DEFAULT '',
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS workflow_steps (
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    input TEXT NOT NULL DEFAULT '',
    input_from TEXT[] NOT NULL DEFAULT '{}',
    input_aggregation TEXT NOT NULL DEFAULT '',
    depends_on TEXT[] NOT NULL DEFAULT '{}',
    status TEXT NOT NULL CHECK (status IN ('pending', 'queued', 'running', 'succeeded', 'failed', 'canceled')),
    run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
    output TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (workflow_id, id),
    UNIQUE (workflow_id, position)
);

CREATE INDEX IF NOT EXISTS workflows_status_created_idx ON workflows(status, created_at, id);
CREATE INDEX IF NOT EXISTS workflow_steps_agent_idx ON workflow_steps(agent_id);
CREATE INDEX IF NOT EXISTS workflow_steps_run_idx ON workflow_steps(run_id) WHERE run_id IS NOT NULL;
