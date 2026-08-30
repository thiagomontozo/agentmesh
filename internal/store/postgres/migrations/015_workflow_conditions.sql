ALTER TABLE workflow_steps
    DROP CONSTRAINT IF EXISTS workflow_steps_status_check;

ALTER TABLE workflow_steps
    ADD CONSTRAINT workflow_steps_status_check
    CHECK (status IN ('pending', 'queued', 'running', 'succeeded', 'skipped', 'failed', 'canceled'));

ALTER TABLE workflow_steps
    ADD COLUMN IF NOT EXISTS condition_source TEXT,
    ADD COLUMN IF NOT EXISTS condition_operator TEXT,
    ADD COLUMN IF NOT EXISTS condition_value TEXT;

ALTER TABLE workflow_steps
    ADD CONSTRAINT workflow_steps_condition_shape_check CHECK (
        (condition_source IS NULL AND condition_operator IS NULL AND condition_value IS NULL)
        OR
        (condition_source IS NOT NULL AND condition_operator IN ('equals', 'not-equals', 'contains', 'not-contains') AND condition_value IS NOT NULL)
    );
