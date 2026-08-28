ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_status_check;

ALTER TABLE runs
    ADD CONSTRAINT runs_status_check
    CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'canceled'));
