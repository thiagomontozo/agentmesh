CREATE TABLE IF NOT EXISTS workflow_events (
    sequence BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_id TEXT,
    run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
    type TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS workflow_events_workflow_sequence_idx
    ON workflow_events(workflow_id, sequence);
CREATE INDEX IF NOT EXISTS workflow_events_timestamp_idx
    ON workflow_events(timestamp);
