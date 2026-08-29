ALTER TABLE agents
    ADD COLUMN updated_at TIMESTAMPTZ,
    ADD COLUMN version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1);

UPDATE agents SET updated_at = created_at WHERE updated_at IS NULL;

ALTER TABLE agents ALTER COLUMN updated_at SET NOT NULL;
