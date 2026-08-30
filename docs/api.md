# API Guide

The API uses JSON under `/api/v1`. Errors have the form:

```json
{"error":{"message":"description"}}
```

Every response includes `X-Request-ID`. A client-provided value is preserved when it is at most 128 characters and contains only letters, digits, `-`, `_`, `.`, or `:`; otherwise AgentMesh generates a safe ID. A newly created Run persists this value as `request_id` for asynchronous correlation.

Production deployments can require Bearer authentication with bounded
`reader`, `operator`, `admin`, and Agent-bound roles. See
[API authentication and authorization](api-authentication.md). When enabled,
only `/healthz` and `/readyz` remain public.

## Health

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

`healthz` is liveness. `readyz` returns `503` when a configured dependency is unavailable.

## Agents

Create an agent:

```bash
curl -X POST http://localhost:8080/api/v1/agents \
  -H 'Content-Type: application/json' \
  -d '{"name":"Researcher","system_prompt":"Be concise and evidence-oriented."}'
```

Legacy agent definitions remain valid and continue to use the demo execution behavior. A remote HTTP Agent declares execution metadata as follows:

```bash
curl -X POST http://localhost:8080/api/v1/agents \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"Legal Agent",
    "runtime":"remote",
    "protocol":"http",
    "endpoint":"http://legal-agent:9000",
    "capabilities":["legal-search","legal-analysis","summarization"],
    "effect_idempotency":"required",
    "max_concurrency":8,
    "priority":25
  }'
```

`runtime` and `protocol` are extensible lowercase identifiers. For remote HTTP execution, use `runtime: "remote"` and `protocol: "http"`; `endpoint` is an HTTP or HTTPS base URL and AgentMesh calls its `/v1/runs` path using [Agent Protocol V1](agent-protocol-v1.md).

An OpenAI-compatible LLM Agent uses `runtime: "llm"`, `protocol: "openai"`,
an HTTP(S) endpoint base, and a non-empty `model`. Its Run input is sent as the
user message and its optional `system_prompt` is sent as a system message. See
[LLM providers](llm-providers.md).

`effect_idempotency` is optional. The only strict policy is `"required"`: all retries receive one stable Run effect key, and the HTTP Runtime rejects a response that does not echo it. Empty/omitted preserves legacy behavior. The Agent remains responsible for atomically deduplicating its own irreversible effect; see [External effect idempotency](external-effect-idempotency.md).

Capabilities are normalized identifier keys: case is folded to lowercase, spaces/underscores become hyphens, repeated separators collapse, and duplicates are removed while preserving declaration order. For example, `"Legal Analysis"`, `"legal_analysis"`, and `"legal-analysis"` all become `"legal-analysis"`. They remain declared metadata rather than an automatic routing decision.

`max_concurrency` is an optional routing capacity declaration. Zero means unspecified and has an effective Router capacity of one; positive values declare the maximum number of queued/running Runs the Router should assign. `priority` ranges from `-1000` to `1000`, defaults to zero, and only breaks equal-utilization ties. Neither field blocks Runs submitted through explicit `agent_id`.

Agent responses include `version`, `created_at`, `updated_at`, and a strong numeric `ETag`. Updates are full replacements and require the current ETag:

```bash
curl -X PUT http://localhost:8080/api/v1/agents/agt_REPLACE_ME \
  -H 'Content-Type: application/json' \
  -H 'If-Match: "1"' \
  -d '{
    "name":"Legal Agent v2",
    "runtime":"remote",
    "protocol":"http",
    "endpoint":"http://legal-agent:9001",
    "capabilities":["legal-search","legal-analysis"]
  }'
```

Successful update increments `version` and returns the new ETag. A stale ETag returns `409 Conflict`; missing, weak, or malformed `If-Match` returns `400 Bad Request`.

Delete an Agent with the current ETag:

```bash
curl -X DELETE http://localhost:8080/api/v1/agents/agt_REPLACE_ME \
  -H 'If-Match: "2"'
```

Deletion returns `204 No Content`. Any Agent referenced by a queued, running, succeeded, failed, or canceled Run is protected and returns `409 Conflict`, preserving historical Run references. Agent health state is forgotten after a successful update target change or deletion.

List or fetch agents:

```bash
curl http://localhost:8080/api/v1/agents
curl 'http://localhost:8080/api/v1/agents?capability=legal-analysis'
curl 'http://localhost:8080/api/v1/agents?capability=legal-analysis&runtime=remote&protocol=http&health=healthy&limit=20&offset=0'
curl http://localhost:8080/api/v1/agents/agt_REPLACE_ME
```

Discovery accepts exact `capability`, `runtime`, and `protocol` filters, plus derived `health` (`unknown`, `healthy`, or `unhealthy`). `status` is accepted as an alias for `health`; conflicting values are rejected. `limit` may be `0` for the backward-compatible unbounded listing or between `1` and `200`; `offset` is zero-based. Responses add deterministic `total`, `limit`, and `offset` metadata and are ordered by creation time then Agent ID.

PostgreSQL backs capability lookup with a GIN index and runtime/protocol lookup with partial B-tree indexes. Health is filtered against the current replica's derived state after persisted filters are applied. Discovery performs exact matching only: it does not interpret input text, rank candidates, or select an Agent for a Run.

Read the derived operational status of an Agent:

```bash
curl http://localhost:8080/api/v1/agents/agt_REPLACE_ME/health
```

The response contains `unknown`, `healthy`, or `unhealthy`, plus `last_checked_at` and a controlled failure `reason` when available. Reading the endpoint is non-blocking: it returns the cached state and schedules a refresh. Legacy/demo and non-HTTP Agents remain `unknown`. Health is deliberately separate from the persisted Agent definition; Router V1 consumes this derived state using its documented healthy/unknown tiers.

## MCP tools

List configured servers and their non-secret local policies:

```bash
curl http://localhost:8080/api/v1/tools/servers
```

Discover the tools still visible after allow/deny filtering:

```bash
curl 'http://localhost:8080/api/v1/tools?server_id=search'
```

Invoke an allowed tool:

```bash
curl -X POST http://localhost:8080/api/v1/tools/call \
  -H 'Content-Type: application/json' \
  -d '{"server_id":"search","name":"search","arguments":{"q":"AgentMesh"}}'
```

Unknown servers return `404`, locally denied tools return `403`, deadline
expiration returns `504`, and upstream protocol/transport failures return
`502`. See [MCP tool gateway](mcp-tools.md) for registry configuration,
authentication, protocol revision, and the synchronous JSON boundary.

## Runs

Submit an asynchronous run:

```bash
curl -i -X POST http://localhost:8080/api/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: research-request-001' \
  -d '{"agent_id":"agt_REPLACE_ME","input":"Summarize durable queues."}'
```

A new run returns `202 Accepted`. Repeating the same key and payload returns `200 OK`, the original run, and `Idempotency-Replayed: true`. Reusing a key with a different payload returns `409 Conflict`.

Alternatively, request deterministic capability routing without supplying `agent_id`:

```bash
curl -i -X POST http://localhost:8080/api/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: legal-analysis-001' \
  -d '{
    "required_capabilities":["legal-analysis","summarization"],
    "input":"Analyze this legal case."
  }'
```

`agent_id` and `required_capabilities` are mutually exclusive. The router requires every declared capability, excludes `unhealthy` Agents, prefers `healthy` Agents, and explicitly falls back to `unknown`. Within a health tier it excludes Agents at effective capacity, chooses the lowest `active_runs / effective_capacity`, then higher `priority`, more remaining slots, oldest `created_at`, and Agent ID. No capability match returns `422 Unprocessable Entity`; matches that are all saturated return `429 Too Many Requests`. The selected `agent_id` and normalized `required_capabilities` are persisted on the Run, so idempotency replay is checked before routing and retains the original decision even if health or load later changes.

The router does not infer requirements from `input`, use an LLM, or replace direct `agent_id` selection.

Create a child Run by referencing an existing parent:

```bash
curl -i -X POST http://localhost:8080/api/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{
    "agent_id":"agt_REPLACE_ME",
    "parent_run_id":"run_PARENT",
    "input":"Review the parent result."
  }'
```

`parent_run_id` is optional and can be combined with either explicit Agent selection or capability routing. AgentMesh derives `root_run_id`: a direct child points to its parent as root, while deeper descendants inherit the original root. Clients cannot supply or mutate `root_run_id`. Missing parents return `404`; changing the parent while replaying an idempotency key returns `409`.

List direct children in deterministic creation order:

```bash
curl http://localhost:8080/api/v1/runs/run_PARENT/children
```

The endpoint does not recursively return grandchildren. A parent stream receives `run.child_queued` containing `child_run_id`, and child lifecycle events carry `parent_run_id` and `root_run_id`.

### Agent-to-Agent child Runs

A currently running Agent may ask AgentMesh to execute another Agent without calling that Agent directly:

```bash
curl -X POST http://localhost:8080/api/v1/runs/run_PARENT/children \
  -H 'Content-Type: application/json' \
  -H 'X-AgentMesh-Caller-Agent-ID: agt_CALLER' \
  -H 'Idempotency-Key: lookup-legal-records-1' \
  -d '{
    "required_capabilities":["legal-search"],
    "input":"Find related decisions."
  }'
```

The request may use `agent_id` instead of `required_capabilities`. The parent must be `running`, the asserted caller must match its Agent ID, and `Idempotency-Key` is mandatory. Successful creation returns `202`; an identical replay returns `200` with `Idempotency-Replayed: true`.

AgentMesh inherits `request_id`, derives parent/root lineage, prevents an Agent ID from repeating in the ancestry, enforces configured depth/direct-child limits, and emits `run.agent_call_queued` on the parent. Direct-child admission is atomic in Memory and PostgreSQL.

With inbound authentication enabled, an `agent` Bearer credential supplies the
caller identity and the server ignores `X-AgentMesh-Caller-Agent-ID`. Without
inbound authentication, the header remains a legacy trusted-network assertion.

Run status progresses through `queued`, `running`, and a terminal state: `succeeded`, `failed`, or `canceled`. The response includes `request_id`, `attempt`, `max_attempts`, lifecycle timestamps, and `duration_ms`. Duration is explicit after a terminal transition and measures execution time from `started_at`, or total queued lifetime if execution never started.

```bash
curl http://localhost:8080/api/v1/runs
curl http://localhost:8080/api/v1/runs/run_REPLACE_ME
```

Cancel a queued or running Run:

```bash
curl -X POST http://localhost:8080/api/v1/runs/run_REPLACE_ME/cancel
```

Successful cancellation returns the canceled Run with `200 OK`. A terminal Run
returns `409 Conflict`; an unknown Run returns `404 Not Found`. Cancellation
clears output/error, records `completed_at`, and prevents further retries. The
`run.canceled` event interrupts a context-aware runtime even when it is executing
in another replica; persisted-state polling provides a fallback when live event
delivery is unavailable.

## Server-Sent Events

Stream a run lifecycle:

```bash
curl -N http://localhost:8080/api/v1/runs/run_REPLACE_ME/events
```

Typical event names are:

- `run.queued`
- `run.started`
- `run.retrying`
- `run.attempt_timed_out`
- `run.succeeded`
- `run.failed`
- `run.canceled`

The stream closes after a terminal event. Every frame contains an SSE `id:` and its JSON body contains the same `event_id`, plus `run_id`, `type`, `message`, `attempt`, and `timestamp`.

In distributed mode, NATS pub/sub makes live events visible on every API replica, while PostgreSQL provides ordered replay across reconnects and restarts. Replay is bounded by `AGENTMESH_EVENT_HISTORY_LIMIT` and `AGENTMESH_EVENT_RETENTION`. AgentMesh does not yet filter replay from an incoming `Last-Event-ID`, so reconnecting clients should deduplicate by `event_id`. PostgreSQL Run state remains authoritative.

## Workflows

Create a validated DAG definition:

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H 'Content-Type: application/json' \
  -d '{
    "input":"document to process",
    "steps":[
      {"id":"extract","agent_id":"agt_EXTRACT","input_from":["workflow"]},
      {"id":"search","agent_id":"agt_SEARCH","depends_on":["extract"],"input_from":["extract"]},
      {"id":"review","agent_id":"agt_REVIEW","depends_on":["extract","search"],"input_from":["extract","search"],"input_aggregation":"json-array"}
    ]
  }'
```

`input_from` accepts `workflow` or Step IDs. A Step output source must also be declared in `depends_on`. Multiple sources require `input_aggregation: "json-array"`; their declaration order is preserved for deterministic future aggregation. A Step may instead provide a literal `input`, but cannot mix literal and sourced input. Definitions reject missing Agents, duplicate/invalid Step IDs, unknown dependencies, self-dependencies, cycles, ambiguous input, and more than 100 Steps.

```bash
curl http://localhost:8080/api/v1/workflows
curl http://localhost:8080/api/v1/workflows/wf_REPLACE_ME
```

Creating a definition does not execute it. Start a chain-shaped Workflow explicitly:

```bash
curl -X POST http://localhost:8080/api/v1/workflows/wf_REPLACE_ME/start
```

The start response is `202 Accepted`. Workflow state progresses `pending → running → succeeded|failed|canceled`; Step state progresses `pending → queued → running → succeeded|failed|canceled`. Each Step becomes an ordinary Run and therefore inherits configured retries, timeout, runtime selection, leases, recovery, cancellation, and observability.

Ready Steps execute concurrently up to `AGENTMESH_WORKFLOW_CONCURRENCY` (default `4`). Fan-out children can run in parallel. A fan-in Step waits for every declared dependency; multiple `input_from` outputs become a JSON string array in declaration order. One branch failure uses fail-fast behavior: active sibling Runs and pending descendants are canceled, and the Workflow becomes failed.

Add deterministic branching with a condition on the Workflow input or a declared dependency:

```json
{
  "id": "legal-branch",
  "agent_id": "agt_LEGAL",
  "depends_on": ["classifier"],
  "input": "process selected branch",
  "condition": {
    "source": "classifier",
    "operator": "equals",
    "value": "legal"
  }
}
```

Operators are `equals`, `not-equals`, `contains`, and `not-contains`. Matching is case-sensitive and deterministic. A false condition produces `status: "skipped"`, emits `workflow.step_skipped`, and does not create a Run. The condition source must be `workflow` or a Step already present in `depends_on`. Arbitrary expressions, regex, `eval`, and executable user input are rejected/not supported.

Cancel a pending or running Workflow:

```bash
curl -X POST http://localhost:8080/api/v1/workflows/wf_REPLACE_ME/cancel
```

Cancellation persists Workflow/Step state first and cancels any active Run through the normal Run cancellation path. Terminal Workflows return `409 Conflict`.

Stream persisted lifecycle events:

```bash
curl -N http://localhost:8080/api/v1/workflows/wf_REPLACE_ME/events
```

Events include `workflow.started`, `workflow.step_queued`, `workflow.step_running`, Step terminal transitions, and one terminal `workflow.succeeded`, `workflow.failed`, or `workflow.canceled`. Frames contain stable `event_id`, `workflow_id`, optional `step_id`/`run_id`, type, message, and timestamp. History is bounded to 1,000 events and seven days. The endpoint polls persistent history, so a client on one replica can observe state produced by another; there is not yet a live NATS Workflow-event transport.

## Request validation

- JSON bodies are limited to 1 MiB.
- Unknown JSON fields and multiple JSON objects are rejected.
- Agent names, agent IDs, and run input cannot be blank.
- `Idempotency-Key` is optional and limited to 128 characters.
