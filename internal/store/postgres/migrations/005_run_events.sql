CREATE TABLE run_events (
    sequence BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
    type TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX run_events_run_sequence_idx ON run_events (run_id, sequence);
CREATE INDEX run_events_timestamp_idx ON run_events (timestamp);
