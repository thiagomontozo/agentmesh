# AgentMesh Roadmap

## v0.1 — Local control plane
- [x] REST API
- [x] Agent registry
- [x] Asynchronous run queue
- [x] Worker pool
- [x] Run lifecycle
- [x] SSE run events
- [x] Graceful shutdown
- [x] Tests and CI
- [x] Container image

## v0.2 — Durable distributed execution
- [x] PostgreSQL repository
- [x] NATS JetStream queue
- [x] Redis cache / distributed coordination
- [x] Database migrations
- [x] Idempotency keys
- [x] Retry and exponential backoff
- [x] Dead-letter handling

## v0.3 — Real agent runtime
- [x] Agent Protocol V1
- [x] Remote HTTP runtime
- [x] Multi-Agent HTTP interoperability test
- [x] Per-attempt runtime timeout
- [x] Runtime panic isolation
- [x] Persisted Run cancellation with stale-writer protection
- [x] Ownership-safe execution lease renewal in Memory and Redis
- [x] Monotonic execution fencing enforced by Run persistence
- [x] Lease-aware, fenced multi-replica Run recovery
- [x] Cross-replica Run event bus and SSE over NATS pub/sub
- [x] Bounded PostgreSQL Run event history and cross-restart SSE replay
- [x] LLM provider interface
- [x] OpenAI-compatible provider
- [x] MCP client and tool registry
- [x] Tool policies and timeouts
- [x] Human approval gates

## v0.4 — Observability and security
- [x] OpenTelemetry traces and metrics
- [x] Bounded Prometheus operational metrics
- [x] Structured JSON logs, request correlation, instance/worker IDs, and Run duration
- [x] Bounded background health checks for remote HTTP Agents
- [x] Versioned Agent update/delete with dependency protection
- [x] Normalized capabilities and exact Agent discovery filters
- [x] Deterministic capability Router V1 with health-aware fallback
- [x] Load-aware Router ranking with declared capacity and priority
- [x] Semantic/LLM Router architectural analysis (no implementation)
- [x] Immutable parent/root Run lineage and direct-child queries
- [x] Persisted Workflow V1 DAG model and definition API
- [x] Sequential Workflow execution through Runs
- [x] Workflow fan-out/fan-in execution with bounded concurrency and fail-fast cancellation
- [x] Deterministic Workflow conditions and branching with skipped Steps
- [x] Control-plane-mediated Agent-to-Agent child Runs with bounded recursion/fan-out
- [x] Real two-replica distributed acceptance test with PostgreSQL, Redis, and NATS
- [x] API/Worker process separation architectural analysis (no implementation)
- [x] Policy-controlled HTTP Runtime security and SSRF hardening
- [x] Outbound Agent Bearer/API-key authentication abstraction
- [x] Request-time secret provider with mounted-file rotation
- [x] Formal Agent Protocol version compatibility and unsupported-version errors
- [x] Opt-in stable effect idempotency across external Agent retries
- [x] Evidence-based architectural reassessment after runtime, routing, Workflow, and distributed increments
- [x] Independent API/Worker process roles and crash/restart acceptance test
- [x] Cross-replica Run cancellation with event signaling and persistence fallback
- [x] Renewable per-Workflow scheduler ownership and replica takeover
- [x] Inbound API authentication
- [x] RBAC with cryptographic Agent-to-Agent caller identity
- [x] Bounded audit log
- [x] Mixed-version rolling-upgrade and schema compatibility CI
- [x] Dependency outage/recovery and sustained multi-worker load CI

## v0.5 — UI and cloud native
- [x] Next.js dashboard
- [x] WebSocket/SSE live run view
- [x] Kubernetes manifests / Helm chart
- [x] Horizontal scaling
- [x] GitOps example
