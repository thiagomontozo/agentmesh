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
- [ ] LLM provider interface
- [ ] OpenAI-compatible provider
- [ ] MCP client and tool registry
- [ ] Tool policies and timeouts
- [ ] Human approval gates

## v0.4 — Observability and security
- [ ] OpenTelemetry traces and metrics
- [ ] Prometheus metrics
- [x] Structured JSON logs, request correlation, instance/worker IDs, and Run duration
- [x] Bounded background health checks for remote HTTP Agents
- [x] Versioned Agent update/delete with dependency protection
- [ ] Authentication
- [ ] RBAC
- [ ] Audit log

## v0.5 — UI and cloud native
- [ ] Next.js dashboard
- [ ] WebSocket/SSE live run view
- [ ] Kubernetes manifests / Helm chart
- [ ] Horizontal scaling
- [ ] GitOps example
