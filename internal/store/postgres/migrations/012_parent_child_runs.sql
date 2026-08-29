ALTER TABLE runs
    ADD COLUMN parent_run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
    ADD COLUMN root_run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
    ADD CONSTRAINT runs_parent_not_self CHECK (parent_run_id IS NULL OR parent_run_id <> id),
    ADD CONSTRAINT runs_root_not_self CHECK (root_run_id IS NULL OR root_run_id <> id),
    ADD CONSTRAINT runs_lineage_shape CHECK (
        (parent_run_id IS NULL AND root_run_id IS NULL) OR
        (parent_run_id IS NOT NULL AND root_run_id IS NOT NULL)
    );

CREATE INDEX runs_parent_created_idx ON runs (parent_run_id, created_at, id)
    WHERE parent_run_id IS NOT NULL;
CREATE INDEX runs_root_idx ON runs (root_run_id)
    WHERE root_run_id IS NOT NULL;

ALTER TABLE run_events
    ADD COLUMN child_run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
    ADD COLUMN parent_run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
    ADD COLUMN root_run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT;
