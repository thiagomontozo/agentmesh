# Architectural reassessment

This reassessment uses only the current code, manifests, and tests. Scores
follow the project rubric: `0` absent, `1` initial concept,
`2` partial, `3` functionally basic, `4` functionally solid, `5` mature. A score
is not increased merely because related code exists.

> Current update: the original Item 34 checkpoint was followed by separate
> API/Worker process tests, cross-replica cancellation, renewable Workflow
> ownership, external-effect idempotency, inbound RBAC, durable audit records,
> secret rotation, bounded Prometheus metrics, OpenTelemetry, mixed-version and
> dependency-failure CI gates, an operations dashboard, and Helm/GitOps
> deployment. The classification below incorporates those code-backed
> increments rather than preserving the historical checkpoint.

## Executive conclusion

AgentMesh is a consolidated **Level 7 — Distributed Agent Platform** at a
functional, pre-production-maturity level.

It is more than an Agent executor or control plane: independent HTTP Agents use
a language-neutral protocol; deterministic routing can choose by capability,
health, load, capacity, and priority; persisted DAG Workflows execute sequential,
fan-out/fan-in, and conditional branches through normal Runs; and Agent-to-Agent
calls remain mediated by the control plane.

The distributed claim rests on shared PostgreSQL, Redis, and NATS, with leases,
fencing, durable work/events, ownership-safe recovery, independently executable
API and Worker roles, cross-process acceptance tests, rolling-version and
dependency-outage gates, and a horizontally scalable Kubernetes topology. This
does not imply exactly-once remote effects or production maturity: external
Agents must honor effect idempotency, and real-cluster capacity, partition, soak,
backup, restore, and disaster-recovery exercises remain operator obligations.

## Scorecard

| Component | Score | Evidence-based assessment |
| --- | ---: | --- |
| Agent Registry | 4/5 | Persisted CRUD, optimistic versions, protected deletion, execution metadata, normalized capabilities, discovery filters, and derived health. No independent self-registration/lease-based Agent membership. |
| Dispatcher | 4/5 | Bounded memory queue or durable JetStream consumer, configurable workers, retries/backoff, timeout, cancellation, panic boundary, DLQ, leases, and recovery. No priority queue or admission-control policy beyond queue/capacity limits. |
| Agent Runtime | 4/5 | Runtime interface, resolver registry, demo adapter, remote HTTP implementation, context/timeout, security policy, and authentication. A new external HTTP Agent needs data/configuration, not Go recompilation; a new transport runtime still requires code. |
| Agent Protocol | 4/5 | Explicit V1 JSON types, structured error, idempotency identity, typed version compatibility, controlled unsupported-version code, and language-neutral tests. No streaming, heartbeat, capability negotiation, or V2 negotiation. |
| Router | 4/5 | Exact multi-capability matching; unhealthy exclusion; unknown fallback; normalized load, capacity, priority, creation time, and ID ranking; explicit Agent ID remains supported. No semantic inference from unstructured text. |
| Orchestrator | 4/5 | Persisted DAGs, sequential execution, fan-out/fan-in, deterministic conditions, cancellation, events, parent/child Runs, mediated Agent-to-Agent calls, renewable scheduler ownership, recovery scanning, and replica takeover. No loops, compensation, dynamic DAG mutation, or planner. |
| Observability | 4/5 | Structured correlated logs, Run/Agent/instance/worker/attempt IDs, duration, bounded Prometheus metrics, opt-in OTLP traces/metrics, persisted events, SSE replay, and cross-replica event transport. No opinionated alert/SLO package or trace storage backend. |
| Persistence | 4/5 | PostgreSQL repositories and ordered migrations cover Agents, Runs, events, lineage, routing, Workflows, and conditions; Memory implements the same repository interfaces; Redis is a cache/coordination layer. No archival/partitioning strategy or schema compatibility matrix. |
| Fault tolerance | 4/5 | Retry/backoff, attempt timeout, panic isolation, distributed cancellation, lease renewal, fencing, stale-writer rejection, idempotency, DLQ, and abandoned-Run recovery. Remote side effects remain outside the fence and require Agent-side idempotency. |
| Distributed execution | 4/5 | Shared JetStream work, PostgreSQL state, Redis leases/fencing, NATS events, independent API/Worker processes, cross-process crash/recovery, rolling-version, dependency-outage, concurrent-load tests, and an HPA-enabled Helm topology are functional. Adverse partition and long-duration cluster soak evidence remains external to CI. |
| Extensibility | 4/5 | Remote Python/Node/other Agents can share HTTP V1, capabilities, authentication, and routing without Engine changes. New transport families, protocol majors, and secret providers require adapter code by design. |

## Level classification

**Level consolidated: 7 — Distributed Agent Platform**

Evidence:

- `queue.NATS` provides durable JetStream delivery and DLQ publication.
- Redis coordination implements token-owned leases and renewal; PostgreSQL and
  Memory enforce monotonic execution fences.
- `Engine.Recover` acquires ownership before recovering a running Run.
- persistent NATS events make Run lifecycle and SSE visible across replicas.
- `TestRealMultiReplicaControlPlane` validates two independently assembled
  replica stacks against real PostgreSQL, Redis, and NATS dependencies.
- `TestSeparateProcessesExecutePreserveAndRecoverRuns` proves process-boundary
  execution, SSE/cancellation, valid-owner preservation, crash recovery, and API
  restart; the load test exercises two Worker processes without duplicate starts.
- `workflow.Manager` renews ownership and supports replica takeover while
  continuing to use fenced Runs for every Step.
- `deploy/helm/agentmesh` maps those roles to independent, autoscaled Deployments
  with probes, disruption budgets, topology spreading, and external durable
  dependencies.

**Level partially implemented: none.** Level 7 is the highest stage in the
project rubric; the 4/5 component scores identify maturity work rather than an
unimplemented next architectural level.

## Residual boundaries

There are no unchecked functional items in the versioned roadmap. Remaining
work is deployment-specific validation: immutable production images and secret
provisioning, real-cluster capacity/load/soak tests, backup/restore and disaster
recovery drills, asymmetric network-partition exercises, SLOs/alerts, and an
external telemetry backend. Richer Workflow compensation or dynamic composition
should only be added for a concrete use case; the finite DAG remains the default.

## Evidence index

| Conclusion | File | Type/function/test | Evidence |
| --- | --- | --- | --- |
| Registry is persisted and mutable | `internal/domain/agent.go`, `internal/store/repository.go`, `internal/store/postgres/postgres.go` | `Agent`, `AgentRepository`, `CreateAgent`, `UpdateAgent`, `DeleteAgent` | Definition carries runtime/protocol/endpoint/capabilities/capacity/priority/version; PostgreSQL implements versioned CRUD and dependency protection. |
| Discovery and health are operational | `internal/discovery/service.go`, `internal/agenthealth/service.go` | `Service.Search`, `Registry`, `Service.Start` | Deterministic filters and bounded derived health checks are separate from persisted configuration. |
| Dispatcher owns execution lifecycle | `internal/engine/engine.go`, `internal/queue/memory.go`, `internal/queue/nats.go` | `Engine.Start`, `Engine.execute`, `Engine.Recover`, `Enqueue`, `DeadLetter` | Worker consumption, attempts, timeout, retry/backoff, panic isolation, DLQ, leases, and recovery are implemented. |
| Runtime is extensible | `internal/runtime/runtime.go`, `internal/runtime/resolver.go`, `internal/runtime/http.go` | `Runtime`, `ExecutionRequest`, `Registry.Resolve`, `HTTPRuntime.Execute` | Engine resolves an already-selected Agent to a runtime without transport-specific branching. |
| HTTP execution is hardened/authenticated | `internal/runtime/http.go`, `internal/runtime/auth.go` | `HTTPPolicy`, `NewSecureHTTPRuntime`, `RequestAuthenticator`, `StaticAuthenticator` | Dial-time IP policy, payload bounds, TLS floor, redirect/decompression rejection, Bearer/API-key headers, and secret-safe configuration. |
| Protocol is language-neutral and versioned | `internal/protocol/version.go`, `internal/protocol/v1/types.go` | `ValidateVersion`, `UnsupportedVersionError`, `RunRequest`, `RunResponse`, `EffectIdempotencyKey` | Stable V1 schema, attempt/effect idempotency identities, structured errors, supported-version registry, and controlled incompatibility code. |
| External languages share one contract | `internal/integration/external_agents_test.go` | `TestTwoExternalAgentsUseTheSameProtocolAndRuntime` | Two independent endpoints execute through the same HTTP Runtime without Agent-specific Engine code. |
| Router selects automatically | `internal/router/router.go` | `Router.Select`, `rankAvailable` | Capability, health, load/capacity, priority, creation time, and ID produce a reproducible decision. |
| Workflow orchestration is native | `internal/domain/workflow.go`, `internal/workflow/manager.go` | `Workflow`, `WorkflowStep`, `WorkflowCondition`, `Manager.StartWorkflow`, `Manager.Recover` | Persisted DAG lifecycle reuses Runs and supports sequential, parallel, fan-in, branching, cancellation, and recovery. |
| Agent-to-Agent remains mediated | `internal/httpapi/server.go`, `internal/store/repository.go` | `createAgentChildRun`, `CreateChildRun` | Child admission is bounded/idempotent and passes through persisted lineage, queueing, and Engine execution. |
| Events survive and cross replicas | `internal/events/nats.go`, `internal/store/postgres/postgres.go` | `NewPersistentNATS`, `Publish`, `Subscribe`, `AppendRunEvent`, `ListRunEvents` | NATS transports live events while PostgreSQL retains bounded replay history. |
| Logs are correlated | `internal/observability/context.go`, `internal/engine/engine.go`, `internal/httpapi/server.go` | `ContextAttrs`, `runLogAttrs`, request middleware | Request, instance, worker, Run, Agent, attempt, and duration fields are emitted when available. |
| Leases/fencing reject stale owners | `internal/cache/redis.go`, `internal/engine/engine.go`, `internal/store/postgres/postgres.go` | `Acquire`, `redisLease.Renew`, `ClaimRunExecution`, `UpdateRunFenced`, `RecoverRun` | Ownership is renewed and every terminal state write must carry the current execution fence. |
| Distributed baseline is tested | `internal/integration/multi_replica_test.go` | `TestRealMultiReplicaControlPlane` | Cross-replica execution/read/SSE, normal non-duplication, DLQ, valid-owner recovery protection, expired-lease recovery, and idempotency use real dependencies. |
| Separate-process behavior | `internal/integration/process_replicas_test.go` | `TestSeparateProcessesExecutePreserveAndRecoverRuns` | API and worker binaries prove cross-process execution/SSE/cancellation, valid-owner preservation, hard-kill recovery after lease expiry, and API restart. |
| Metrics and tracing | `internal/metrics/metrics.go`, `internal/telemetry/telemetry.go`, `internal/httpapi/server.go`, `internal/engine/engine.go` | `Registry.WritePrometheus`, `telemetry.New`, inbound/outbound instrumentation, Run/attempt spans | Bounded Prometheus metrics and opt-in OTLP HTTP trace/metric export are covered by focused tests and CI. |
| Immediate distributed cancellation | `internal/engine/engine.go`, `internal/integration/process_replicas_test.go` | `Engine.Cancel`, `watchCancellation`, `TestSeparateProcessesExecutePreserveAndRecoverRuns` | Proven for context-aware runtimes across API and worker processes through NATS events with persisted polling fallback. |
| External effects reuse one retry identity | `internal/runtime/http.go`, `internal/integration/external_agents_test.go` | `HTTPRuntime.Execute`, `TestExternalEffectIsAppliedOnceAcrossRetry` | Attempt keys change while the stable effect key is reused and strictly acknowledged; the reference Agent commits one effect across a `500` retry. |
| Inbound identity and authorization are enforced | `internal/apiauth/auth.go`, `internal/httpapi/server.go` | `Authenticator.Middleware`, `authorized`, `createAgentChildRun` | Constant-time Bearer identity carries bounded roles; Agent-bound identity replaces the spoofable caller header when enabled. |
| Mutations are auditable | `internal/domain/audit.go`, `internal/store/postgres/postgres.go` | `AuditEvent`, `AppendAuditEvent`, `ListAuditEvents` | Bounded mutation records persist subject, role, correlation, operation, and result without request bodies or secrets. |
| Secrets rotate without restart | `internal/runtime/auth.go` | `SecretProvider`, `NewEnvironmentFileSecretProvider`, `Authenticate` | Credential references resolve on every request; bounded absolute mounted files may be atomically replaced. |
| Operational metrics are bounded | `internal/metrics/metrics.go` | `Registry`, `WrapBroker`, `WritePrometheus` | Finite HTTP/event/routing labels plus persisted Run-state gauges avoid Run/Agent/path cardinality. |
| Mixed-version rolling upgrades are tested | `.github/workflows/ci.yml`, `scripts/rolling-upgrade-test.sh` | `compatibility` job | Schema-016 and current binaries cross API/Worker roles in both directions on shared PostgreSQL, Redis, and NATS. |
| Dependency failure and load are exercised | `.github/workflows/ci.yml`, `scripts/dependency-resilience-test.sh`, `internal/integration/process_replicas_test.go` | `resilience` job, `TestSeparateProcessesSustainConcurrentLoadWithoutDuplicateAttempts` | A stalled database and stopped Redis/NATS drive bounded unready/recovery behavior; 120 concurrent Runs produce exactly 120 starts across two worker processes. |
| Cloud-native topology is reproducible | `deploy/helm/agentmesh`, `deploy/gitops/argocd/agentmesh.yaml`, `.github/workflows/ci.yml` | Helm templates, `autoscaling/v2` HPAs, Argo CD `Application`, `cloud-native` job | Separate API/Worker Deployments, probes, PDBs, topology spreading, optional dashboard, external Secret references, deterministic GitOps sync, and default/variant chart rendering are validated in CI. |
