ALTER TABLE agents
    ADD COLUMN max_concurrency INTEGER NOT NULL DEFAULT 0 CHECK (max_concurrency >= 0),
    ADD COLUMN priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000);

CREATE INDEX runs_active_agent_idx ON runs (agent_id)
    WHERE status IN ('queued', 'running');
