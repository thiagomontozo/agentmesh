CREATE TABLE approvals (
    id TEXT PRIMARY KEY,
    server_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments JSONB NOT NULL,
    arguments_hash TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected', 'consumed')),
    requested_by TEXT NOT NULL,
    decided_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    decided_at TIMESTAMPTZ,
    consumed_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 1
);

CREATE INDEX approvals_status_created_idx ON approvals (status, created_at DESC);
CREATE INDEX approvals_expires_idx ON approvals (expires_at);
