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
- [ ] LLM provider interface
- [ ] OpenAI-compatible provider
- [ ] MCP client and tool registry
- [ ] Tool policies and timeouts
- [ ] Human approval gates

## v0.4 — Observability and security
- [ ] OpenTelemetry traces and metrics
- [ ] Prometheus metrics
- [ ] Structured request IDs
- [ ] Authentication
- [ ] RBAC
- [ ] Audit log

## v0.5 — UI and cloud native
- [ ] Next.js dashboard
- [ ] WebSocket/SSE live run view
- [ ] Kubernetes manifests / Helm chart
- [ ] Horizontal scaling
- [ ] GitOps example
