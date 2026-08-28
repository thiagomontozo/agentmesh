# AgentMesh Architecture

AgentMesh has two runtime modes that share the same HTTP, domain, engine, and executor layers.

## Agent definitions

Agent definitions can optionally declare `runtime`, `protocol`, `endpoint`, and `capabilities`. These fields describe how an agent may be executed by future runtime adapters; capabilities are metadata only. Legacy definitions without execution metadata remain valid and continue to use the deterministic demo executor.

The current engine behavior is intentionally unchanged: execution metadata is persisted and exposed by the API, but it is not resolved or invoked yet.

## Runtime contract

`internal/runtime` defines the runtime-facing boundary for one execution attempt:

```text
ExecutionRequest (Run ID, Agent, Attempt, Input)
        ↓
Runtime.Execute(context.Context, request)
        ↓
ExecutionResult (Output) or error
```

`Agent.ID` is the single source of truth for the Agent ID inside the request. The package also provides `AdaptLegacy`, which wraps executors using the current `engine.Executor` method shape. This keeps `DemoExecutor` compatible while avoiding an import dependency from the runtime package to the engine.

The Engine still invokes its existing low-level `Executor` directly. Runtime resolution is deliberately not part of this stage and will be introduced separately.

## Memory mode

Memory mode is the default. It uses a mutex-protected repository and bounded Go channel, requires no external services, and is intended for development and unit tests. State is process-local.

## Distributed mode

Distributed mode is enabled with `AGENTMESH_MODE=distributed`:

- PostgreSQL stores agents, run state, attempt counters, timestamps, and idempotency keys. Embedded, ordered SQL migrations run at startup.
- NATS JetStream durably stores run work. A named stream and durable pull consumer use explicit acknowledgements. Executor failures are retried by the engine; exhausted runs are published to `agentmesh.runs.dlq` before being marked failed.
- Redis caches agent and run reads and provides token-protected execution leases, preventing two replicas from executing the same run concurrently. Cache failures fall back to PostgreSQL, while coordination failures stop delivery and make readiness fail.

At startup, runs left in `running` state are reset to `queued`; queued runs are republished with their run ID as the JetStream deduplication key. This closes the common restart gap between database state and queue delivery without introducing a second scheduler.

## Delivery guarantees

Run submission is idempotent when clients send `Idempotency-Key`. PostgreSQL enforces uniqueness, so concurrent duplicate requests return the same run. JetStream delivery is at least once; state-transition validation and run IDs make duplicate delivery safe at the control-plane boundary.

The current demo executor is deterministic. Side-effecting future executors must independently make their external actions idempotent because no queue can make an arbitrary external side effect exactly once.
