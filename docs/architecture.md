# AgentMesh Architecture

AgentMesh has two runtime modes that share the same HTTP, domain, engine, and executor layers.

## Agent definitions

Agent definitions can optionally declare `runtime`, `protocol`, `endpoint`, and `capabilities`. Capability keys have one deterministic lowercase/hyphen form and are deduplicated on write. Memory performs exact scans, while PostgreSQL uses its `TEXT[]` representation and a GIN containment index for exact capability lookup. This is a lightweight declared model, not a centrally governed taxonomy, semantic matcher, or Agent router. Legacy definitions without execution metadata remain valid and continue to use the deterministic demo executor.

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

## Agent health

`internal/agenthealth.Service` keeps operational availability separate from persisted Agent configuration. Only Agents declaring `runtime: "remote"`, `protocol: "http"`, and an endpoint are probed. Their base endpoint is joined with the configured `/healthz` convention, redirects are rejected, and only `2xx` is healthy. Controlled reasons distinguish timeout, cancellation, unreachable endpoint, invalid endpoint, and HTTP status without exposing raw transport errors through the API.

One scheduler and a fixed worker pool perform checks; there is no goroutine per Agent. The queue is bounded and refresh submission is non-blocking, so slow or numerous Agents cannot block normal API handlers. `GET /api/v1/agents/{id}/health` returns the current state immediately and requests a refresh.

Health is derived and process-local. A restarted or different replica initially reports `unknown` until its own probe completes. It is not persisted, does not modify Agent configuration, and does not remove unhealthy Agents. Runtime resolution does not consult it; Router V1 uses it only to exclude `unhealthy`, prefer `healthy`, and apply the documented `unknown` fallback.

## Agent discovery

`internal/discovery.Service` combines exact persisted filters (`capability`, `runtime`, and `protocol`) with the operational state exposed by `agenthealth.Registry`. Configuration filters run in the repository first; health filtering and pagination then operate on that bounded result without changing the Agent definition. Memory and PostgreSQL return a deterministic creation-time/ID order, and the service enforces the order again at its boundary.

The public Agent listing exposes those filters with optional `limit`/`offset` pagination. A zero limit preserves the original unbounded list behavior; explicit pages are capped at 200. Discovery returns candidates only. It contains no task interpretation, ranking, fallback, load balancing, or Run creation.

## Deterministic Agent Router V1

`internal/router.Router` reuses discovery when Run submission supplies normalized `required_capabilities` instead of an explicit `agent_id`. It requires an Agent to declare every capability, excludes `unhealthy`, prefers `healthy`, and uses `unknown` as an explicit fallback.

The load-aware rank reads one aggregate snapshot of queued/running Runs for the matching Agent IDs. An Agent at effective capacity is excluded. Remaining candidates are ordered by normalized utilization (`active / capacity`), descending priority, remaining slots, creation time, then Agent ID. A missing `max_concurrency` has effective capacity one for routing compatibility. Decision logs add active count, effective capacity, and priority.

The API still supports direct `agent_id`, and rejects requests that mix the two selection modes. The Run persists both the chosen Agent ID and routing requirements. This makes the decision auditable and allows an idempotency replay to return its original selection rather than rerouting after a health change.

The capacity check is advisory rather than an atomic reservation: simultaneous replicas can observe the same snapshot and choose the same Agent. Explicit `agent_id` also bypasses capacity. Enforcing a hard distributed concurrency quota would require an atomic reservation/lease and is outside this increment.

The Router remains deterministic and declared-input-only: it does not inspect natural-language input, use LLMs or embeddings, or discover capabilities implicitly. Health remains replica-local, so replicas can make different decisions while their probe state converges; the persisted Run and idempotency contract remain authoritative after creation. Idempotency lookup occurs before routing so a saturated original Agent cannot block replay.

Semantic routing remains analysis-only. The comparison, security constraints, evaluation gates, and recommended gated hybrid are documented in [Semantic / LLM Router Analysis](semantic-router-analysis.md).

## Parent and child Runs

A Run may optionally reference one immutable `parent_run_id`. AgentMesh derives `root_run_id` from the existing parent: direct children use the parent's ID, and deeper descendants inherit the parent's root. Top-level Runs retain empty lineage fields, preserving all existing JSON and persistence behavior.

Memory and PostgreSQL validate the relationship on creation. PostgreSQL adds self-referential foreign keys, shape/self-reference checks, and indexes for direct-child and root lookup. Since a parent must already exist and lineage is never updated, every edge points to an older Run and cycles cannot be constructed; domain checks also reject direct self-parent/root cases and corrupt inherited lineage.

`GET /api/v1/runs/{id}/children` returns direct children only, ordered by creation time and ID. Creation emits a lineage-bearing `run.queued` event on the child and `run.child_queued` on the parent. Subsequent Engine lifecycle events retain the child's parent/root fields, and distributed PostgreSQL event history persists them.

This is composition groundwork, not a workflow engine. Parent completion does not trigger children, outputs are not propagated, failure/cancellation does not cascade, and there are no dependencies, DAG scheduling, fan-out/fan-in, conditions, or cycle traversal.

## Workflow Model V1

`domain.Workflow` is a persisted DAG definition containing ordered `WorkflowStep` records. Every Step references an existing Agent, declares deterministic dependencies, and chooses either literal input or explicit `input_from` sources. One source uses `single`; multiple sources must opt into the controlled `json-array` aggregation. Step IDs are normalized, duplicate and missing references are rejected, and Kahn traversal rejects cycles before persistence. Definitions are limited to 100 Steps.

Memory stores isolated deep copies. PostgreSQL migration `013_workflows.sql` stores the Workflow and its ordered Steps transactionally with Agent foreign keys. Agents referenced by Workflow definitions cannot be deleted. The cached repository deliberately forwards Workflow reads to the authoritative store rather than introducing a second invalidation surface.

The API can create, list, and fetch definitions. At this increment every Workflow and Step remains `pending`: there is no start endpoint, scheduler, Run creation, output propagation, recovery, or cancellation yet. Runtime state columns are present to preserve one representation when execution is added, but their lifecycle cannot be mutated through the API.

## Agent Registry lifecycle

Agent definitions have immutable `id` and `created_at`, mutable execution metadata, `updated_at`, and a monotonic `version`. `PUT` performs a validated full replacement, while Memory and PostgreSQL compare the supplied version atomically before incrementing it. API mutations require a strong numeric `If-Match`, so concurrent writers cannot silently overwrite each other. Redis is refreshed after success and invalidated after conflicts to avoid retaining stale versions.

`DELETE` uses the same optimistic version contract. Memory checks Run references while holding its repository mutex. PostgreSQL locks the Agent row, checks dependent Runs, and deletes in one transaction. Any Run reference blocks deletion regardless of lifecycle state, preserving Agent identity and execution history. Operational health remains outside this persisted configuration lifecycle and is refreshed or forgotten after mutations.
