ALTER TABLE agents
ADD COLUMN effect_idempotency TEXT NOT NULL DEFAULT '';

ALTER TABLE agents
ADD CONSTRAINT agents_effect_idempotency_check
CHECK (effect_idempotency IN ('', 'required'));
