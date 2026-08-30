# Configuration and Operations

AgentMesh reads configuration from environment variables. Copy `.env.example` when preparing a local environment, but do not commit credentials or production connection strings.

## Runtime modes

### Memory

Memory mode is the default and requires only Go 1.23+:

```bash
go run ./cmd/agentmesh
```

Agents, runs, queue messages, and events disappear when the process exits. Use this mode for development, API exploration, and unit tests.

### Distributed

Distributed mode requires PostgreSQL, NATS with JetStream enabled, and Redis. The supplied Compose file configures all three:

```bash
docker compose up --build
```

The API is available at `http://localhost:8080`. PostgreSQL migrations run automatically during application startup.

## Environment variables

| Variable | Default | Validation and effect |
| --- | --- | --- |
| `AGENTMESH_ADDR` | `:8080` | HTTP listen address |
| `AGENTMESH_INSTANCE_ID` | generated | Replica identifier included in logs; configure a stable unique value in production |
| `AGENTMESH_MODE` | `memory` | Must be `memory` or `distributed` |
| `AGENTMESH_WORKERS` | `4` | Must be at least 1 |
| `AGENTMESH_WORKFLOW_CONCURRENCY` | `4` | Maximum queued/running Steps per Workflow; must be at least 1 |
| `AGENTMESH_AGENT_CALL_MAX_DEPTH` | `8` | Maximum depth of a child Run requested by an Agent |
| `AGENTMESH_AGENT_CALL_MAX_CHILDREN` | `16` | Atomic direct-child limit per parent Run for Agent calls |
| `AGENTMESH_QUEUE_SIZE` | `128` | Bounded queue size in memory mode; must be at least 1 |
| `AGENTMESH_EXECUTION_DELAY` | `750ms` | Artificial latency of the demo executor |
| `AGENTMESH_ATTEMPT_TIMEOUT` | `30s` | Maximum runtime duration for each attempt; must be positive |
| `AGENTMESH_SHUTDOWN_TIMEOUT` | `10s` | Maximum graceful HTTP shutdown period |
| `AGENTMESH_MAX_ATTEMPTS` | `3` | Executor attempts before a run fails and is dead-lettered |
| `AGENTMESH_RETRY_INITIAL_BACKOFF` | `250ms` | First retry delay |
| `AGENTMESH_RETRY_MAX_BACKOFF` | `5s` | Backoff cap; cannot be below the initial delay |
| `AGENTMESH_NATS_ACK_WAIT` | `2m` | JetStream acknowledgement window; keep above the maximum expected execution time |
| `AGENTMESH_CACHE_TTL` | `30s` | Redis TTL for agents and runs; must be positive |
| `AGENTMESH_LEASE_TTL` | `5m` | Per-run execution lease; renewed every third of the TTL and must be positive |
| `AGENTMESH_EVENT_RETENTION` | `168h` | Maximum age of persisted Run events; must be positive |
| `AGENTMESH_EVENT_HISTORY_LIMIT` | `1000` | Maximum persisted/replayed events per Run; must be at least 1 |
| `AGENTMESH_AGENT_HEALTH_PATH` | `/healthz` | Relative health path appended to a remote HTTP Agent base endpoint |
| `AGENTMESH_AGENT_HEALTH_INTERVAL` | `30s` | Background scan interval; must be positive |
| `AGENTMESH_AGENT_HEALTH_TIMEOUT` | `2s` | Timeout for one Agent probe; must be positive |
| `AGENTMESH_AGENT_HEALTH_WORKERS` | `2` | Fixed number of probe workers; must be at least 1 |
| `AGENTMESH_DATABASE_URL` | none | Required in distributed mode |
| `AGENTMESH_NATS_URL` | none | Required in distributed mode |
| `AGENTMESH_REDIS_URL` | none | Required in distributed mode |

## Remote HTTP Agents

Register a remote Agent with `runtime: "remote"`, `protocol: "http"`, and an HTTP or HTTPS base `endpoint`. AgentMesh appends `/v1/runs` and sends [Agent Protocol V1](agent-protocol-v1.md). `AGENTMESH_ATTEMPT_TIMEOUT` controls both the execution context and application HTTP client timeout; responses are limited to 1 MiB.

Redirects, URL credentials, query strings, fragments, proxies, automatic response
decompression, and non-HTTP schemes are rejected. Private networks remain
allowed by default because AgentMesh is designed to call internal services,
while link-local/metadata addresses are denied. Dial-time address checks,
allowlisted hosts, denied CIDRs, TLS requirements, and body limits are described
in [HTTP Runtime security](http-runtime-security.md).

`AGENTMESH_AGENT_AUTH_CONFIG` maps Agent IDs to Bearer/API-key configuration
whose `secret_env` points to a separate environment variable. No secret is stored
in AgentMesh persistence or returned through its API. See
[Agent request authentication](agent-authentication.md).

Example without Compose:

```bash
export AGENTMESH_MODE=distributed
export AGENTMESH_DATABASE_URL='postgres://agentmesh:password@localhost:5432/agentmesh?sslmode=require'
export AGENTMESH_NATS_URL='nats://localhost:4222'
export AGENTMESH_REDIS_URL='redis://localhost:6379/0'
go run ./cmd/agentmesh
```

For PowerShell, use `$env:AGENTMESH_MODE = "distributed"` and the corresponding `$env:` assignments.

## Readiness and dependency behavior

- `/healthz` reports that the process is alive.
- `/readyz` checks PostgreSQL, Redis, and NATS in distributed mode.
- Redis read failures fall back to PostgreSQL, but readiness remains degraded until Redis recovers.
- A NATS or PostgreSQL failure makes readiness return `503`.

## Retry and dead-letter behavior

Every runtime call receives a child context with `AGENTMESH_ATTEMPT_TIMEOUT`. A timeout emits `run.attempt_timed_out` and consumes one attempt. If attempts remain, normal exponential backoff and retry apply; otherwise the Run is marked `failed` and dead-lettered with a timeout error. A parent-context cancellation, such as process shutdown, interrupts the attempt without converting the recoverable running Run into a terminal timeout failure.

Other executor failures use exponential backoff up to `AGENTMESH_RETRY_MAX_BACKOFF`. After the configured attempt count, the run is marked `failed` and a JSON record is published to `agentmesh.runs.dlq`. Infrastructure errors are negatively acknowledged so JetStream can redeliver them.

A panic raised by a runtime or legacy executor is recovered only at the runtime-call boundary. It is logged with execution identifiers and a stack trace, then treated as a normal attempt failure. Repeated panics therefore exhaust `AGENTMESH_MAX_ATTEMPTS`, fail the Run, and reach the DLQ without terminating the worker process.

Queued and running Runs can be canceled through the API. Cancellation is persisted before local execution is interrupted, so a stale completion cannot replace it. With multiple replicas, only an execution owned by the API replica receives immediate context cancellation; another replica continues until its current call returns or times out, then its update is rejected. Distributed signaling is intentionally deferred rather than presenting local-only cancellation as a complete multi-node guarantee.

Each acquired lease has a monotonic fencing token. The Engine claims a newer PostgreSQL/Memory execution fence before running and includes that fence in every lifecycle write. An executor from an older lease cannot publish a late success or failure after a newer owner claims the Run. This guarantee applies to AgentMesh state only; remote Agent side effects must still honor the protocol idempotency key.

Runs interrupted by process shutdown remain recoverable. On startup, queued work is republished using the Run ID as its JetStream deduplication key. Running work is requeued only after the recovering instance acquires its expired/missing lease and advances the execution fence; a Run still owned by another healthy replica is left untouched.

## Production notes

- Replace the development passwords from `compose.yml`.
- Set a unique, stable `AGENTMESH_INSTANCE_ID` for every replica so logs remain attributable across restarts.
- Enable TLS and authentication for PostgreSQL, NATS, and Redis.
- Do not expose dependency ports publicly.
- Restrict who can register or change remote Agent endpoints, and apply outbound network policy to AgentMesh.
- Keep `AGENTMESH_NATS_ACK_WAIT` above the longest expected executor attempt so JetStream does not redeliver healthy work early.
- Keep `AGENTMESH_LEASE_TTL` long enough to tolerate ordinary Redis latency. Active leases renew automatically, but one failed renewal conservatively stops execution.
- Preserve Redis AOF data in distributed deployments so coordination counters remain monotonic operationally; the repository claim remains authoritative for Run writes.
- Runtimes must honor context cancellation; AgentMesh does not detach runtime calls into goroutines to force-stop implementations that ignore context.
- Back up PostgreSQL and the JetStream storage directory.
- Distributed SSE uses NATS pub/sub across replicas. Keep NATS available; publish failures are logged and only the publisher's local subscribers see the affected event.
- Run event history is persisted in PostgreSQL and bounded by both age and count. SSE reconnects replay that bounded history; clients should still query the Run resource for authoritative final state.
- `Last-Event-ID` filtering is not consumed yet. Clients may receive already processed events after reconnecting and should deduplicate by `event_id`.
- Agent health is an in-memory, per-replica observation. Expect `unknown` after restart. Router V1 excludes `unhealthy`, prefers `healthy`, and explicitly falls back to `unknown`, so replicas may temporarily choose different candidates while probes converge.
- Router load is computed from persisted queued/running Runs. `max_concurrency` and `priority` belong to each Agent definition rather than environment configuration. Capacity is a routing hint, not a distributed semaphore; direct `agent_id` submissions and concurrent routing races can exceed it.
