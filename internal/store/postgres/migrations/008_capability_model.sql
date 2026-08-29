WITH normalized AS (
    SELECT id, array_agg(capability ORDER BY first_position) AS capabilities
    FROM (
        SELECT id, capability, min(position) AS first_position
        FROM (
            SELECT
                agents.id,
                btrim(
                    regexp_replace(
                        regexp_replace(lower(btrim(item.value)), '[[:space:]_]+', '-', 'g'),
                        '-+', '-', 'g'
                    ),
                    '-'
                ) AS capability,
                item.position
            FROM agents
            CROSS JOIN LATERAL unnest(agents.capabilities) WITH ORDINALITY AS item(value, position)
        ) canonical
        WHERE capability <> ''
        GROUP BY id, capability
    ) deduplicated
    GROUP BY id
)
UPDATE agents
SET capabilities = normalized.capabilities
FROM normalized
WHERE agents.id = normalized.id;

CREATE INDEX agents_capabilities_gin_idx ON agents USING GIN (capabilities);
