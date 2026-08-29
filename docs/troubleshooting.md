# Troubleshooting

## Correlating an API request with asynchronous execution

Capture the `X-Request-ID` response header or provide your own safe value. JSON logs preserve it as `request_id` on Run processing records and combine it with `instance_id`, `worker_id`, `run_id`, `agent_id`, and `attempt` when available. In production, configure a stable unique `AGENTMESH_INSTANCE_ID` on each replica; generated IDs change after restart.

## Agent health is unknown after restart

This is expected because operational Agent health is derived per replica and is not persisted. The background workers will probe remote HTTP Agents shortly after startup. Calling `GET /api/v1/agents/{id}/health` also schedules a non-blocking refresh. Confirm the Agent exposes the configured `AGENTMESH_AGENT_HEALTH_PATH` and returns a `2xx` response.

## Agent update or delete returns 409

For updates, fetch the Agent again and retry with its latest strong `ETag` in `If-Match`; another writer changed the definition. Deletion also returns `409` when any Run references the Agent. That protection is intentional and applies to terminal Runs so historical execution records never point to a removed Agent.

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

The worker could not renew the execution lease or Redis reported that another token owns it. AgentMesh cancels the runtime context and deliberately leaves the Run non-terminal so the queue can redeliver it. Check Redis connectivity and latency, then confirm `AGENTMESH_LEASE_TTL` is comfortably above transient network delays. A runtime that ignores context can continue external side effects after ownership is lost, but its fencing token prevents it from overwriting Run state after a newer owner claims execution.

## A worker reports `stale run execution fence`

A newer lease owner claimed the Run before this worker attempted to persist state. The write was deliberately rejected; do not retry it with an unfenced repository update. Confirm whether a lease expired, Redis was unavailable, or duplicate workers used inconsistent coordination infrastructure. The current Run state belongs to the highest repository-issued fence.

## A running Run is not requeued when another replica starts

This is intentional while its execution lease remains owned. Startup recovery no longer resets every `running` row. The new replica skips a healthy owner's Run; after the lease expires, a recovery pass can acquire ownership, advance the fence, and requeue it. Check Redis TTL/renewal state when an actually abandoned Run remains `running` longer than `AGENTMESH_LEASE_TTL`.

## Capability routing returns 429

At least one healthy/unknown Agent matched every requested capability, but all matches had queued/running counts at or above their effective `max_concurrency`. Wait for a Run to reach a terminal state, increase the Agent's declared capacity using its ETag, add another equivalent Agent, or submit an explicit `agent_id` if deliberately bypassing advisory routing capacity. Reusing an existing `Idempotency-Key` replays before this capacity check.

## An SSE client receives duplicate events after reconnecting

Distributed event history survives restart in PostgreSQL and every event has a stable `event_id`. AgentMesh currently replays the configured bounded history rather than applying the request's `Last-Event-ID`. Deduplicate by `event_id`, and query `GET /api/v1/runs/{id}` for authoritative final state. Native `Last-Event-ID` filtering remains pending.
