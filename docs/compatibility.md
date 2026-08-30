# Rolling-upgrade and compatibility policy

AgentMesh CI maintains an executable compatibility baseline in
`compatibility/baseline.txt`. The current baseline is commit `401de1f`, the
consolidated schema-016 release immediately before inbound security, audit,
secret rotation, and metrics were added.

The compatibility job builds both source revisions and exercises one shared
PostgreSQL, Redis, and NATS deployment:

1. the baseline initializes schema 016, creates an Agent and completes a Run;
2. a current API starts, applies newer migrations, and reads baseline state;
3. a baseline Worker executes a Run submitted by the current API;
4. a current Worker executes a Run submitted by a restarted baseline API;
5. both versions read the same terminal state and replay one idempotency key.

This is a real mixed-binary window, not two instances compiled from the same
source. CI fails if an additive migration, repository query, queue payload,
lease contract, or HTTP representation breaks either direction.

## Storage policy

- migrations are forward-only, ordered, embedded, and protected by the existing
  PostgreSQL advisory migration lock;
- changes inside the supported rolling window must be additive, with defaults
  for columns an older binary does not write;
- repository SQL names explicit columns rather than relying on `SELECT *`;
- a column/table cannot be renamed, repurposed, or removed until its oldest
  supported baseline has been retired;
- rollback means restoring the previous binary while keeping the expanded
  schema. Automatic down-migrations are deliberately unsupported.

| Binary | Schema initialized | Reads current expanded schema | Queue/API interop |
| --- | --- | --- | --- |
| Baseline `401de1f` | 001–016 | CI-proven through current migration 017 | CI-proven in both API/Worker directions |
| Current | 001–017 | Native | CI-proven against baseline |

## Agent Protocol policy

Agent Protocol major version `1` remains authoritative. Optional fields and
headers are additive; legacy Agents may omit `effect_idempotency_key` unless
their definition declares `effect_idempotency: "required"`. An incompatible
wire change requires a new major schema package and explicit resolver support;
it must not silently alter V1 semantics.

`TestLegacyV1PayloadsRemainValid` proves old request/response shapes still pass
current validation. `TestLegacyV1ConsumerCanIgnoreCurrentAdditiveFields` proves
the required V1 core survives decoding by the previous field set. V1 consumers
must ignore unknown additive JSON fields; strict unknown-field rejection is not
compatible with this policy.

## Limits

The job validates the declared baseline and current revision, not every historic
commit or arbitrary future downgrade. It does not simulate network partitions
or long-running production load. Before a release removes a deprecated contract,
advance the baseline only after the documented support window and keep a CI run
covering the oldest version still allowed in production.
