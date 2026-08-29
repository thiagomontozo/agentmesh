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

`Runtime.Execute` is also a panic boundary. A recovered panic becomes an explicit execution error, follows the existing retry/dead-letter lifecycle, and cannot terminate the process or the consuming worker. The structured error log includes Run ID, Agent ID, attempt, panic value, and stack trace. Queue handlers do not currently propagate worker identity to the Engine, so `worker_id` is not yet available at this boundary. Lease release and queue semaphore cleanup remain protected by their existing defers outside the runtime call.

## Run cancellation

`POST /api/v1/runs/{id}/cancel` atomically moves a queued or running Run to `canceled`. The Engine keeps cancel functions only for active executions in its own process, so local runtime contexts—including outbound HTTP requests—are interrupted immediately and retry backoff stops. Queued messages are not removed from the queue; consumers acknowledge them without execution after observing the terminal state.

PostgreSQL and Memory reject stale updates to a canceled Run. This prevents a worker in another replica from overwriting cancellation with a late success or failure. There is not yet a distributed cancellation signal: a remote worker may continue its current external call until it returns or reaches the attempt timeout, but its result is discarded. The resulting lifecycle events are distributed to all API replicas through NATS.

The language boundary is covered by an integration test with two independent HTTP endpoints. Both Agents are registered through the public API and share one HTTP runtime; adding the second Agent introduces only data, not another executor implementation. See [External HTTP Agents](external-agents.md).

## Memory mode

Memory mode is the default. It uses a mutex-protected repository and bounded Go channel, requires no external services, and is intended for development and unit tests. State is process-local.

## Distributed mode

Distributed mode is enabled with `AGENTMESH_MODE=distributed`:

- PostgreSQL stores agents, run state, attempt counters, timestamps, idempotency keys, and bounded Run event history. Embedded, ordered SQL migrations run at startup.
- NATS JetStream durably stores run work. A named stream and durable pull consumer use explicit acknowledgements. Executor failures are retried by the engine; exhausted runs are published to `agentmesh.runs.dlq` before being marked failed.
- NATS core pub/sub transports live Run events between replicas on one ordered subject. Each event is assigned a stable `event_id` and persisted before live publication. `NoEcho` prevents a publisher's own NATS subscription from duplicating the event, while ID-based local deduplication protects replay/live overlap.
- Redis caches agent and run reads and provides token-protected execution leases. The Engine renews an active lease every third of its TTL, so a context-aware Run can safely exceed the original lease duration. Memory leases implement the same ownership and expiration contract. Cache failures fall back to PostgreSQL, while coordination failures stop delivery and make readiness fail.

Lease renewal is conservative: if renewal fails or ownership is lost, the Engine cancels the runtime context, emits `run.lease_lost`, does not finalize the Run, and returns an error so the queue can redeliver it. Renewal stops before lease release on every execution exit.

Every successful lease acquisition also carries a monotonic fencing token. Redis allocates it atomically in the same Lua operation that creates the lease; Memory uses the same monotonic contract. After acquisition, the repository atomically claims the Run and advances its persisted `execution_fence` to at least that token. Every Engine state write must present the exact current fence. If a newer owner claims the Run, PostgreSQL or Memory rejects the old worker with `store.ErrStaleExecution`, including when the old runtime ignored cancellation or when independent coordinators temporarily admitted both workers. The fence is intentionally internal and is not exposed as Agent or client data.

Fencing protects AgentMesh Run state, not arbitrary side effects already performed by an external Agent. Agent Protocol idempotency remains required for those effects. Redis persistence is enabled in Compose, but PostgreSQL's atomic claim still advances above its stored fence if a restored Redis sequence is lower.

At startup, queued Runs are republished with their Run ID as the JetStream deduplication key. A running Run is never reset globally: the recovering Engine first attempts to acquire that Run's execution lease. A healthy owner keeps renewing the lease, so another replica skips its Run. After a crashed owner's lease expires, one recovering replica acquires ownership, atomically advances the persisted execution fence, resets the Run to `queued`, releases the recovery lease, and republishes work. A stale owner can no longer finalize after recovery. The operation is idempotent; competing recovery instances either fail lease acquisition or observe that the Run is no longer `running`.

## Distributed events

Memory mode retains the original in-process event bus. Distributed mode implements the same `events.Broker` contract with NATS pub/sub. An SSE client connected to replica A receives lifecycle events published by a worker on replica B. NATS preserves a publisher's message order on the shared Run-events subject; the local bus preserves arrival order per Run.

Subscriber channels are bounded and sends are non-blocking, so a slow SSE client cannot stall execution. In distributed mode PostgreSQL retains the most recent `AGENTMESH_EVENT_HISTORY_LIMIT` events per Run and removes events older than `AGENTMESH_EVENT_RETENTION`. A fresh replica loads this ordered history and atomically merges it with events already received over NATS, deduplicating by `event_id`.

SSE frames expose the stable event identity through the standard `id:` field and JSON `event_id`. Reconnection currently replays the configured bounded history from its beginning; request-side `Last-Event-ID` filtering is intentionally deferred, but the durable identity and ordering needed for it now exist. Persistence errors are logged and do not block local/NATS delivery, so the Run state remains authoritative if event storage is temporarily unavailable.

## Delivery guarantees

Run submission is idempotent when clients send `Idempotency-Key`. PostgreSQL enforces uniqueness, so concurrent duplicate requests return the same run. JetStream delivery is at least once; state-transition validation and run IDs make duplicate delivery safe at the control-plane boundary.

The current demo executor is deterministic. Side-effecting future executors must independently make their external actions idempotent because no queue can make an arbitrary external side effect exactly once.

## Basic observability

The application emits JSON through `log/slog`. HTTP middleware accepts a safe `X-Request-ID` or generates one, returns it in the response, and records method, path, status, response size, and duration. Run creation persists that correlation ID so asynchronous execution logs can retain the originating request context after queue delivery.

Every process has an `instance_id`: `AGENTMESH_INSTANCE_ID` when configured, otherwise hostname plus a random suffix. Memory workers use stable IDs such as `memory-1`; JetStream deliveries acquire bounded worker slots such as `nats-1`. Engine lifecycle, retry, timeout, panic, lease, success, and failure logs add the identifiers available at their boundary: `request_id`, `instance_id`, `worker_id`, `run_id`, `agent_id`, and `attempt`.

Terminal Runs expose persisted `duration_ms`, measured from execution start or, when execution never started, creation time. Recovery clears the partial duration before requeue. This increment intentionally does not add metrics, tracing, or OpenTelemetry; logs and persisted lifecycle fields are the operational baseline for evaluating those later.
