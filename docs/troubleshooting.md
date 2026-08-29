# Troubleshooting

## `unlinkat ... httpapi.test.exe` on Windows

If all package tests report `ok` and the error occurs only while Go removes a temporary `.test.exe`, first treat it as Windows file locking rather than an AgentMesh failure. Antivirus scanning and indexing can briefly retain newly created executables.

Recommended checks:

1. Run `go test -count=1 ./...` again.
2. Confirm that no `.test.exe` process remains active.
3. Temporarily set `GOTMPDIR` to a dedicated development temp directory.
4. Review antivirus history before adding any exclusion.

Do not disable security software globally.

## `/readyz` returns 503

In distributed mode, verify all three URLs and dependencies:

```bash
docker compose ps
docker compose logs postgres nats redis agentmesh
```

PostgreSQL must accept connections, NATS must have JetStream enabled, and Redis must answer `PING`.

## Docker daemon is unavailable

Start Docker Desktop and wait until its Linux engine is ready. `docker compose config` validates the file without starting the stack; `docker compose up --build` requires the engine.

## A run stays queued after restart

Check NATS connectivity and the `AGENTMESH_RUNS` stream. AgentMesh republishes pending run IDs during startup, but cannot do so while JetStream is unavailable. Readiness will remain `503` in that condition.

## A run is retried more than once

JetStream provides at-least-once delivery. Duplicate delivery is expected after acknowledgement loss. AgentMesh treats terminal runs as already processed, while future side-effecting executors must make their own external actions idempotent.

## A run fails with `runtime panic`

AgentMesh recovered a panic raised inside `Runtime.Execute`. Search structured logs for `runtime panic recovered` and the matching `run_id`; the record includes `agent_id`, `attempt`, panic value, and stack trace. The Run follows normal retry and DLQ policy. Fix the runtime implementation rather than treating recovery as successful execution. `worker_id` is not currently available because queue handlers do not propagate it to the Engine.

## A canceled Run is still active on another replica

The `canceled` state is durable and stale workers cannot replace it, but active context cancellation is process-local. If the API request reached a different replica from the worker, the external call may remain active until it returns or reaches `AGENTMESH_ATTEMPT_TIMEOUT`; its result is discarded. Use Agent Protocol idempotency to control side effects. Cross-replica cancellation signaling remains pending.

## A Run reports `run.lease_lost`

The worker could not renew the execution lease or Redis reported that another token owns it. AgentMesh cancels the runtime context and deliberately leaves the Run non-terminal so the queue can redeliver it. Check Redis connectivity and latency, then confirm `AGENTMESH_LEASE_TTL` is comfortably above transient network delays. A runtime that ignores context can continue side effects after ownership is lost; fencing protection is tracked separately.
