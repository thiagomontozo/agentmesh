ALTER TABLE runs
    ADD COLUMN request_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN duration_ms BIGINT NOT NULL DEFAULT 0 CHECK (duration_ms >= 0);
