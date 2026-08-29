ALTER TABLE runs
    ADD COLUMN required_capabilities TEXT[] NOT NULL DEFAULT '{}';
