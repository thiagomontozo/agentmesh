CREATE TABLE audit_events (
    sequence BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    timestamp TIMESTAMPTZ NOT NULL,
    request_id TEXT NOT NULL DEFAULT '',
    instance_id TEXT NOT NULL DEFAULT '',
    subject TEXT NOT NULL,
    roles TEXT[] NOT NULL DEFAULT '{}',
    agent_id TEXT NOT NULL DEFAULT '',
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    status INTEGER NOT NULL
);

CREATE INDEX audit_events_timestamp_idx ON audit_events (timestamp DESC);
