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
| `AGENTMESH_MODE` | `memory` | Must be `memory` or `distributed` |
| `AGENTMESH_WORKERS` | `4` | Must be at least 1 |
| `AGENTMESH_QUEUE_SIZE` | `128` | Bounded queue size in memory mode; must be at least 1 |
| `AGENTMESH_EXECUTION_DELAY` | `750ms` | Artificial latency of the demo executor |
| `AGENTMESH_ATTEMPT_TIMEOUT` | `30s` | Maximum runtime duration for each attempt; must be positive |
| `AGENTMESH_SHUTDOWN_TIMEOUT` | `10s` | Maximum graceful HTTP shutdown period |
| `AGENTMESH_MAX_ATTEMPTS` | `3` | Executor attempts before a run fails and is dead-lettered |
| `AGENTMESH_RETRY_INITIAL_BACKOFF` | `250ms` | First retry delay |
| `AGENTMESH_RETRY_MAX_BACKOFF` | `5s` | Backoff cap; cannot be below the initial delay |
| `AGENTMESH_NATS_ACK_WAIT` | `2m` | JetStream acknowledgement window; keep above the maximum expected execution time |
| `AGENTMESH_CACHE_TTL` | `30s` | Redis TTL for agents and runs; must be positive |
| `AGENTMESH_LEASE_TTL` | `5m` | Per-run distributed execution lease; keep above the longest executor attempt |
| `AGENTMESH_DATABASE_URL` | none | Required in distributed mode |
| `AGENTMESH_NATS_URL` | none | Required in distributed mode |
| `AGENTMESH_REDIS_URL` | none | Required in distributed mode |

## Remote HTTP Agents

Register a remote Agent with `runtime: "remote"`, `protocol: "http"`, and an HTTP or HTTPS base `endpoint`. AgentMesh appends `/v1/runs` and sends [Agent Protocol V1](agent-protocol-v1.md). `AGENTMESH_ATTEMPT_TIMEOUT` controls both the execution context and application HTTP client timeout; responses are limited to 1 MiB.

Redirects, URL credentials, query strings, fragments, and non-HTTP schemes are rejected. Private network addresses are intentionally allowed because AgentMesh is designed to call internal services. Consequently, Agent registration is a privileged trust boundary: an untrusted registrant could use endpoints for SSRF, DNS-rebinding, or cloud metadata access. Network allow/deny policy is not implemented yet and must be enforced at the deployment network layer until the dedicated HTTP-runtime security stage.

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

Runs interrupted by process shutdown remain recoverable. On the next distributed startup, `running` runs are reset to `queued` and queued work is republished using the run ID as its JetStream deduplication key.

## Production notes

- Replace the development passwords from `compose.yml`.
- Enable TLS and authentication for PostgreSQL, NATS, and Redis.
- Do not expose dependency ports publicly.
- Restrict who can register or change remote Agent endpoints, and apply outbound network policy to AgentMesh.
- Set both `AGENTMESH_NATS_ACK_WAIT` and `AGENTMESH_LEASE_TTL` above the longest expected executor attempt.
- Runtimes must honor context cancellation; AgentMesh does not detach runtime calls into goroutines to force-stop implementations that ignore context.
- Back up PostgreSQL and the JetStream storage directory.
- The current SSE event bus is process-local; use sticky routing for SSE clients until durable cross-replica events are implemented.
