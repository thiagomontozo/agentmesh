# Architectural reassessment after Items 1–33

This reassessment uses only the code and tests present after Agent Protocol
versioning. Scores follow the project rubric: `0` absent, `1` initial concept,
`2` partial, `3` functionally basic, `4` functionally solid, `5` mature. A score
is not increased merely because related code exists.

> Post-audit update: the critical platform increment added separate API/Worker
> processes with crash/restart acceptance coverage, cross-replica cancellation,
> renewable Workflow scheduler ownership, and opt-in external-effect
> idempotency. The historical scores below describe the Item 34 checkpoint;
> resolved critical gaps are tracked here, in the roadmap, and in current
> architecture documentation.

## Executive conclusion

AgentMesh is a consolidated **Level 6 — Multi-Agent Orchestrator** and a partial
**Level 7 — Distributed Agent Platform**.

It is more than an Agent executor or control plane: independent HTTP Agents use
a language-neutral protocol; deterministic routing can choose by capability,
health, load, capacity, and priority; persisted DAG Workflows execute sequential,
fan-out/fan-in, and conditional branches through normal Runs; and Agent-to-Agent
calls remain mediated by the control plane.

The Level 7 classification is not consolidated. Production components use
shared PostgreSQL, Redis, and NATS, with leases, fencing, durable work, distributed
events, and recovery. However, the acceptance test runs two logical replicas in
one Go process rather than separate processes/containers. Distributed
cancellation is persisted but not signaled immediately to a remote worker,
Workflow scheduler ownership is not coordinated across process roles, and
external Agent side effects cannot be fenced by AgentMesh.

## Scorecard

| Component | Score | Evidence-based assessment |
| --- | ---: | --- |
| Agent Registry | 4/5 | Persisted CRUD, optimistic versions, protected deletion, execution metadata, normalized capabilities, discovery filters, and derived health. No independent self-registration/lease-based Agent membership. |
| Dispatcher | 4/5 | Bounded memory queue or durable JetStream consumer, configurable workers, retries/backoff, timeout, cancellation, panic boundary, DLQ, leases, and recovery. No priority queue or admission-control policy beyond queue/capacity limits. |
| Agent Runtime | 4/5 | Runtime interface, resolver registry, demo adapter, remote HTTP implementation, context/timeout, security policy, and authentication. A new external HTTP Agent needs data/configuration, not Go recompilation; a new transport runtime still requires code. |
| Agent Protocol | 4/5 | Explicit V1 JSON types, structured error, idempotency identity, typed version compatibility, controlled unsupported-version code, and language-neutral tests. No streaming, heartbeat, capability negotiation, or V2 negotiation. |
| Router | 4/5 | Exact multi-capability matching; unhealthy exclusion; unknown fallback; normalized load, capacity, priority, creation time, and ID ranking; explicit Agent ID remains supported. No semantic inference from unstructured text. |
| Orchestrator | 3/5 | Persisted DAGs, sequential execution, fan-out/fan-in, deterministic conditions, cancellation, recovery, events, parent/child Runs, and mediated Agent-to-Agent calls. No loops, compensation, dynamic DAG mutation, planner, or distributed scheduler ownership. |
| Observability | 3/5 | Structured correlated logs, Run/Agent/instance/worker/attempt IDs, duration, persisted events, SSE replay, and cross-replica event transport. No operational metrics or distributed tracing. |
| Persistence | 4/5 | PostgreSQL repositories and ordered migrations cover Agents, Runs, events, lineage, routing, Workflows, and conditions; Memory implements the same repository interfaces; Redis is a cache/coordination layer. No archival/partitioning strategy or schema compatibility matrix. |
| Fault tolerance | 4/5 | Retry/backoff, attempt timeout, panic isolation, cancellation, lease renewal, fencing, stale-writer rejection, idempotency, DLQ, and abandoned-Run recovery. Remote side effects remain outside the fence and cancellation signaling is not fully distributed. |
| Distributed execution | 3/5 | Shared JetStream work, PostgreSQL state, Redis leases/fencing sequence, NATS events, and a two-replica logical acceptance test are functional. Separate-process/container, network-partition, rolling-upgrade, and sustained-load evidence is absent. |
| Extensibility | 4/5 | Remote Python/Node/other Agents can share HTTP V1, capabilities, authentication, and routing without Engine changes. New transport families, protocol majors, and secret providers require adapter code by design. |

## Level classification

**Level consolidated: 6 — Multi-Agent Orchestrator**

Evidence:

- `router.Router.Select` automatically chooses an eligible Agent from declared
  capabilities and operational state.
- `workflow.Manager` turns persisted DAG Steps into ordinary Runs, propagates
  outputs, reconciles parallel branches, conditions, failures, cancellation, and
  restart recovery.
- `POST /api/v1/runs/{id}/children` creates bounded, idempotent Agent-to-Agent
  child Runs through the same Engine and observability path.
- `runtime.HTTPRuntime` and Agent Protocol V1 execute independently implemented
  external Agents through one contract.

**Level partially implemented: 7 — Distributed Agent Platform**

Evidence:

- `queue.NATS` provides durable JetStream delivery and DLQ publication.
- Redis coordination implements token-owned leases and renewal; PostgreSQL and
  Memory enforce monotonic execution fences.
- `Engine.Recover` acquires ownership before recovering a running Run.
- persistent NATS events make Run lifecycle and SSE visible across replicas.
- `TestRealMultiReplicaControlPlane` validates two independently assembled
  replica stacks against real PostgreSQL, Redis, and NATS dependencies.

The missing separate-process and adverse-network evidence prevents a consolidated
Level 7 claim.

## Next gap

### Critical

1. ~~Add a process/container-level multi-replica acceptance test:~~ independent
   AgentMesh A/B processes, process kill, restart, valid-owner preservation,
   cross-replica SSE, idempotency, and lease/fence behavior are now exercised by
   `TestSeparateProcessesExecutePreserveAndRecoverRuns`; DLQ remains covered by
   the real-dependency logical-replica test.
2. ~~Add distributed Run cancellation signaling.~~ Resolved by per-Run event
   subscription with persisted polling fallback and a process-level test.
3. ~~Define and enforce Workflow scheduler ownership.~~ Resolved with renewable
   per-Workflow coordination leases, periodic recovery scans, and takeover tests.
4. ~~Require and test Agent Protocol idempotency for irreversible external
   effects.~~ Resolved with a stable per-Run effect key, opt-in strict Agent
   acknowledgement, and a retry test that applies the reference external effect
   once. AgentMesh still cannot prove atomic behavior inside a remote service.

### Important

1. Add inbound API authentication/authorization and replace the trusted
   Agent-to-Agent caller header assertion with cryptographic identity.
2. ~~Integrate an external secret provider/rotation path behind
   `RequestAuthenticator`.~~ Resolved with request-time `SecretProvider`
   resolution and atomically replaceable mounted files.
3. ~~Add bounded operational metrics for queue depth, active Runs, latency,
   attempts, failures, lease loss, recovery, and routing decisions.~~ Resolved
   through the dependency-free Prometheus endpoint and bounded event/HTTP labels.
4. Validate rolling upgrades and storage/protocol backward compatibility with
   mixed AgentMesh versions.

### Desirable

1. Add deployment-level network partition, dependency latency, and load tests.
2. Add tracing only when cross-service latency diagnosis justifies its cost.
3. Add Kubernetes/Helm examples after process-role and readiness semantics are
   implemented and proven.
4. Evaluate richer Workflow compensation or dynamic composition only from a
   concrete use case; the finite DAG model should remain the default.

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
| Metrics and tracing | repository-wide code | — | **NOT PROVEN:** no metrics exporter or distributed tracing implementation exists. |
| Immediate distributed cancellation | `internal/engine/engine.go`, `internal/integration/process_replicas_test.go` | `Engine.Cancel`, `watchCancellation`, `TestSeparateProcessesExecutePreserveAndRecoverRuns` | Proven for context-aware runtimes across API and worker processes through NATS events with persisted polling fallback. |
| External effects reuse one retry identity | `internal/runtime/http.go`, `internal/integration/external_agents_test.go` | `HTTPRuntime.Execute`, `TestExternalEffectIsAppliedOnceAcrossRetry` | Attempt keys change while the stable effect key is reused and strictly acknowledged; the reference Agent commits one effect across a `500` retry. |
