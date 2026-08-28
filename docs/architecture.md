# AgentMesh Architecture

AgentMesh has two runtime modes that share the same HTTP, domain, engine, and executor layers.

## Agent definitions

Agent definitions can optionally declare `runtime`, `protocol`, `endpoint`, and `capabilities`. These fields describe how an agent may be executed by future runtime adapters; capabilities are metadata only. Legacy definitions without execution metadata remain valid and continue to use the deterministic demo executor.

The Engine now resolves this metadata before starting an attempt. An empty runtime preserves legacy behavior by resolving to `demo`; an unknown runtime fails explicitly and is not silently executed by the demo implementation.

## Runtime contract

`internal/runtime` defines the runtime-facing boundary for one execution attempt:

```text
ExecutionRequest (Run ID, Agent, Attempt, Input)
        ↓
Runtime.Execute(context.Context, request)
        ↓
ExecutionResult (Output) or error
```

`Agent.ID` is the single source of truth for the Agent ID inside the request. The package also provides `AdaptLegacy`, which wraps executors using the existing `engine.Executor` method shape. This keeps `DemoExecutor` compatible while avoiding an import dependency from the runtime package to the engine.

`runtime.Registry` is a concurrency-safe resolver keyed by normalized runtime name:

```text
Engine.execute
        ↓ Agent already selected by Run.AgentID
Runtime Resolver
        ├── demo → AdaptLegacy(DemoExecutor)
        └── remote + protocol http → HTTP Runtime → Agent endpoint /v1/runs
```

`engine.New` preserves the original constructor and installs the demo adapter. The application uses `engine.NewWithResolver` to register both the demo adapter and the HTTP runtime. The Engine only constructs `ExecutionRequest`, calls `Resolver.Resolve(agent)`, and invokes the returned `Runtime`; it contains no runtime-specific switch and performs no Agent routing.

## Agent Protocol V1

`internal/protocol/v1` defines explicit JSON request, response, status, and structured error types for remote execution. The contract carries protocol version, Run and Agent identities, attempt, idempotency identity, input, output, status, and retryability. It is independent from the internal Go `Runtime` interface and is documented in [Agent Protocol V1](agent-protocol-v1.md), so non-Go Agents can implement it.

`runtime.HTTPRuntime` maps the internal execution request to Agent Protocol V1 and posts it to the registered Agent endpoint. It accepts `runtime: "remote"` with `protocol: "http"`; the endpoint is a base URL and `/v1/runs` is appended. Redirects are not followed, response bodies are bounded, and protocol responses are validated before their output is accepted.

Transport failures are classified as temporary, permanent, timeout, canceled, or protocol errors. HTTP `408`, `429`, and `5xx` are temporary; other non-`200` statuses are permanent. A V1 failed response uses its `error.retryable` flag for classification. The Engine retains ownership of retry policy and, at this stage, continues its existing attempt policy for every non-context execution error.

Each runtime attempt runs synchronously under a child context bounded by `AGENTMESH_ATTEMPT_TIMEOUT`. Attempt timeout is distinct from cancellation of the Engine's parent context: timeout consumes an attempt and may retry, while shutdown cancellation exits execution and leaves the running Run recoverable. No watchdog goroutine is created, so runtimes that honor context cannot leak execution goroutines or indefinitely block graceful worker shutdown.

The language boundary is covered by an integration test with two independent HTTP endpoints. Both Agents are registered through the public API and share one HTTP runtime; adding the second Agent introduces only data, not another executor implementation. See [External HTTP Agents](external-agents.md).

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
