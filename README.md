# AgentMesh

[![CI](https://github.com/thiagomontozo/agentmesh/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/thiagomontozo/agentmesh/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Stage: v0.2](https://img.shields.io/badge/stage-v0.2-2563eb)
![Go](https://img.shields.io/badge/Go-control_plane-00ADD8?logo=go&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-durable_store-4169E1?logo=postgresql&logoColor=white)
![NATS JetStream](https://img.shields.io/badge/NATS-JetStream-27AAE1?logo=natsdotio&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-coordination-DC382D?logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-containerized-2496ED?logo=docker&logoColor=white)

**Distributed AI Agent Control Plane written in Go.**

AgentMesh is a portfolio-grade control-plane project for registering agents, submitting asynchronous runs, processing work through a concurrent worker pool, and streaming run lifecycle events to clients.

> **Current stage: v0.2.** AgentMesh supports a zero-infrastructure memory mode, a durable distributed mode backed by PostgreSQL, NATS JetStream, and Redis, and language-neutral remote Agents through Agent Protocol V1 over HTTP.

## Why this project exists

The goal is to explore the engineering behind agent infrastructure rather than build another chatbot: concurrency, queues, lifecycle state, event streams, graceful shutdown, durable execution, observability, and distributed systems.

## Architecture

```mermaid
flowchart LR
    C[Client] -->|REST| API[Go HTTP API]
    API --> S[(Repository)]
    API --> AR[Capability Router]
    AR --> S
    API --> WF[Workflow DAG Manager]
    WF --> S
    WF --> Q
    API --> Q[Queue]
    Q --> W1[Worker 1]
    Q --> W2[Worker 2]
    Q --> WN[Worker N]
    W1 --> RR[Runtime Resolver]
    W2 --> RR
    WN --> RR
    RR --> D[Demo Runtime]
    RR --> H[HTTP Runtime]
    H --> A[Remote Agent]
    D --> S
    W1 --> B[Event Bus]
    W2 --> B
    WN --> B
    B -->|SSE| C
    S -. distributed .-> PG[(PostgreSQL)]
    S -. cache .-> R[(Redis)]
    Q -. distributed .-> N[NATS JetStream]
```

The Engine resolves the already-selected Agent's runtime through a concurrency-safe registry. Legacy Agents and Agents declaring `runtime: "demo"` use the deterministic `DemoExecutor` through an adapter. Agents declaring `runtime: "remote"` and `protocol: "http"` are invoked over Agent Protocol V1 without runtime-specific branching in the Engine.

## Features

- Go standard-library HTTP server (`net/http`)
- Agent registration and lookup
- Remote HTTP Agent execution through Agent Protocol V1
- Asynchronous run submission
- Configurable concurrent worker pool
- Explicit run state machine: `queued → running → succeeded/failed/canceled`
- Server-Sent Events (SSE) for lifecycle events
- Graceful shutdown
- Environment-based configuration
- Unit/API tests
- Race-detector-friendly synchronization
- Multi-stage Docker build
- GitHub Actions CI
- Zero third-party Go dependencies in v0.1
- PostgreSQL persistence and embedded migrations
- NATS JetStream durable run delivery and dead-letter subject
- Redis read-through cache with graceful database fallback
- Idempotent run creation through `Idempotency-Key`
- Configurable retry with exponential backoff
- Configurable per-attempt execution timeout
- Runtime panic isolation at the execution boundary
- Explicit cancellation for queued and running Runs
- Automatic execution-lease renewal for long-running Runs
- Monotonic fencing tokens for stale-worker write protection
- Lease-aware multi-replica recovery for abandoned Runs
- Cross-replica Run events and SSE through NATS pub/sub
- Bounded PostgreSQL event history with stable SSE event IDs and restart replay
- JSON operational logs correlated by request, instance, worker, Run, Agent, and attempt
- Persisted request correlation and explicit Run duration
- Derived `unknown`/`healthy`/`unhealthy` status for remote HTTP Agents
- Versioned Agent update/delete with optimistic concurrency and Run-history protection
- Normalized, deduplicated Agent capabilities with exact indexed lookup
- Deterministic Agent discovery by capability, runtime, protocol, and derived health
- Deterministic capability Router with health exclusion and explicit unknown fallback
- Load-aware routing by active Runs, declared capacity, and deterministic priority
- Immutable parent/root Run lineage with direct-child lookup and events
- Persisted Workflow V1 DAG definitions with explicit input sources
- Bounded Workflow DAG execution with sequential, fan-out, and fan-in Steps
- Deterministic Workflow conditions and branching without `eval`
- Control-plane-mediated Agent-to-Agent child Runs with bounded depth and fan-out
- Restart recovery for queued/running work

## API

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Readiness |
| `POST` | `/api/v1/agents` | Create an agent |
| `GET` | `/api/v1/agents` | Discover agents by exact capability/runtime/protocol/health filters |
| `GET` | `/api/v1/agents/{id}` | Get an agent |
| `PUT` | `/api/v1/agents/{id}` | Replace an Agent definition using `If-Match` |
| `DELETE` | `/api/v1/agents/{id}` | Delete an unused Agent using `If-Match` |
| `GET` | `/api/v1/agents/{id}/health` | Get derived Agent health and schedule refresh |
| `POST` | `/api/v1/runs` | Submit a Run by explicit Agent ID or required capabilities |
| `GET` | `/api/v1/runs` | List runs |
| `GET` | `/api/v1/runs/{id}` | Get run status/result |
| `GET` | `/api/v1/runs/{id}/children` | List direct child Runs |
| `POST` | `/api/v1/runs/{id}/children` | Request an Agent-to-Agent child Run through AgentMesh |
| `POST` | `/api/v1/runs/{id}/cancel` | Cancel a queued or running Run |
| `GET` | `/api/v1/runs/{id}/events` | Stream lifecycle events via SSE |
| `POST` | `/api/v1/workflows` | Create a validated Workflow DAG definition |
| `GET` | `/api/v1/workflows` | List Workflow definitions |
| `GET` | `/api/v1/workflows/{id}` | Get a Workflow definition |
| `POST` | `/api/v1/workflows/{id}/start` | Start a sequential Workflow |
| `POST` | `/api/v1/workflows/{id}/cancel` | Cancel a pending/running Workflow |
| `GET` | `/api/v1/workflows/{id}/events` | Stream persisted Workflow lifecycle events |

## Run locally

Requires Go 1.23+.

```bash
go run ./cmd/agentmesh
```

Then:

```bash
curl http://localhost:8080/healthz
```

### Create an agent

```bash
curl -X POST http://localhost:8080/api/v1/agents \
  -H "Content-Type: application/json" \
  -d '{"name":"Researcher","system_prompt":"Be concise and evidence-oriented."}'
```

### Submit a run

```bash
curl -X POST http://localhost:8080/api/v1/runs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: explain-control-planes-1" \
  -d '{"agent_id":"agt_REPLACE_ME","input":"Explain control planes."}'
```

Reusing the same idempotency key and payload returns the original run with `Idempotency-Replayed: true`. Reusing it with a different payload returns `409 Conflict`.

### Windows PowerShell smoke test

With the server running in one terminal:

```powershell
.\scripts\smoke.ps1
```

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `AGENTMESH_ADDR` | `:8080` | HTTP bind address |
| `AGENTMESH_INSTANCE_ID` | generated | Stable replica name; set explicitly in production |
| `AGENTMESH_MODE` | `memory` | `memory` or `distributed` runtime |
| `AGENTMESH_WORKERS` | `4` | Worker goroutines |
| `AGENTMESH_WORKFLOW_CONCURRENCY` | `4` | Maximum active Steps per Workflow |
| `AGENTMESH_AGENT_CALL_MAX_DEPTH` | `8` | Maximum Agent-to-Agent ancestry depth |
| `AGENTMESH_AGENT_CALL_MAX_CHILDREN` | `16` | Maximum direct Agent-call children per parent Run |
| `AGENTMESH_QUEUE_SIZE` | `128` | In-memory run queue capacity |
| `AGENTMESH_EXECUTION_DELAY` | `750ms` | Demo executor latency |
| `AGENTMESH_ATTEMPT_TIMEOUT` | `30s` | Maximum duration of each execution attempt |
| `AGENTMESH_SHUTDOWN_TIMEOUT` | `10s` | HTTP graceful-shutdown timeout |
| `AGENTMESH_MAX_ATTEMPTS` | `3` | Executor attempts before dead-lettering |
| `AGENTMESH_RETRY_INITIAL_BACKOFF` | `250ms` | Initial retry delay |
| `AGENTMESH_RETRY_MAX_BACKOFF` | `5s` | Maximum exponential retry delay |
| `AGENTMESH_DATABASE_URL` | — | PostgreSQL URL required in distributed mode |
| `AGENTMESH_NATS_URL` | — | NATS URL required in distributed mode |
| `AGENTMESH_REDIS_URL` | — | Redis URL required in distributed mode |
| `AGENTMESH_NATS_ACK_WAIT` | `2m` | JetStream acknowledgement timeout |
| `AGENTMESH_CACHE_TTL` | `30s` | Redis cache lifetime |
| `AGENTMESH_LEASE_TTL` | `5m` | Distributed per-run execution lease |
| `AGENTMESH_EVENT_RETENTION` | `168h` | Maximum age of persisted Run events |
| `AGENTMESH_EVENT_HISTORY_LIMIT` | `1000` | Maximum persisted/replayed events per Run |
| `AGENTMESH_AGENT_HEALTH_PATH` | `/healthz` | Health path appended to remote Agent endpoints |
| `AGENTMESH_AGENT_HEALTH_INTERVAL` | `30s` | Background health scan interval |
| `AGENTMESH_AGENT_HEALTH_TIMEOUT` | `2s` | Per-probe timeout |
| `AGENTMESH_AGENT_HEALTH_WORKERS` | `2` | Fixed probe worker count |

## Test

```bash
go test ./...
go test -race ./...
go vet ./...
```

Distributed integration tests require the Compose dependencies:

```bash
docker compose up -d --wait postgres nats redis
go test -tags=integration -count=1 ./internal/integration
docker compose down -v
```

## Docker

`docker compose up --build` starts the complete distributed stack. For the lightweight local mode, use `go run ./cmd/agentmesh`.

## Project structure

```text
cmd/agentmesh/          application entrypoint
internal/config/        environment configuration
internal/domain/        core domain models
internal/engine/        queue, workers and executor abstraction
internal/runtime/       runtime request/result contract and legacy adapter
internal/protocol/v1/   language-neutral Agent Protocol V1 wire types
internal/events/        local/NATS event broker + persistent replay
internal/httpapi/       REST + SSE transport
internal/queue/         memory and NATS JetStream queues
internal/cache/         Redis cache adapter
internal/store/         memory, cached and PostgreSQL repositories
scripts/                PowerShell developer utilities
docs/                   roadmap and architecture notes
.github/workflows/      CI pipeline
```

## Documentation

- [Architecture and delivery guarantees](docs/architecture.md)
- [Agent Protocol V1](docs/agent-protocol-v1.md)
- [External HTTP Agents](docs/external-agents.md)
- [Configuration and operations](docs/configuration.md)
- [API usage and examples](docs/api.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Semantic / LLM Router analysis](docs/semantic-router-analysis.md)
- [Roadmap](docs/roadmap.md)

## Next milestones

The next iterations add the real agent runtime and production operations:

1. Real LLM provider abstraction
2. MCP tool gateway and tool policies
3. Human approval gates
4. OpenTelemetry and Prometheus
5. Authentication, RBAC and audit log
6. Next.js operations dashboard
7. Kubernetes/Helm deployment

See [`docs/roadmap.md`](docs/roadmap.md).

## Design principles

- Keep the control plane independent of any single LLM vendor.
- Make concurrency explicit and observable.
- Prefer interfaces at infrastructure boundaries.
- Add distributed infrastructure only when the local behavior is testable.
- Treat failure, retry, cancellation and idempotency as first-class design concerns.

## License

MIT
