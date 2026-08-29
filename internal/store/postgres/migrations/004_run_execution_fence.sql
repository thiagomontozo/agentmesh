ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS execution_fence BIGINT NOT NULL DEFAULT 0
    CHECK (execution_fence >= 0);
