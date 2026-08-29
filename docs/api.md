# API Guide

The API uses JSON under `/api/v1`. Errors have the form:

```json
{"error":{"message":"description"}}
```

Every response includes `X-Request-ID`. A client-provided value is preserved when it is at most 128 characters and contains only letters, digits, `-`, `_`, `.`, or `:`; otherwise AgentMesh generates a safe ID. A newly created Run persists this value as `request_id` for asynchronous correlation.

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
    "capabilities":["legal-search","legal-analysis","summarization"]
  }'
```

`runtime` and `protocol` are extensible lowercase identifiers. For remote HTTP execution, use `runtime: "remote"` and `protocol: "http"`; `endpoint` is an HTTP or HTTPS base URL and AgentMesh calls its `/v1/runs` path using [Agent Protocol V1](agent-protocol-v1.md).

Capabilities are normalized identifier keys: case is folded to lowercase, spaces/underscores become hyphens, repeated separators collapse, and duplicates are removed while preserving declaration order. For example, `"Legal Analysis"`, `"legal_analysis"`, and `"legal-analysis"` all become `"legal-analysis"`. They remain declared metadata rather than an automatic routing decision.

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
curl http://localhost:8080/api/v1/agents/agt_REPLACE_ME
```

The optional `capability` filter performs an exact lookup on the normalized key. PostgreSQL backs this query with a GIN index. It does not perform semantic matching or select an Agent for a Run.

Read the derived operational status of an Agent:

```bash
curl http://localhost:8080/api/v1/agents/agt_REPLACE_ME/health
```

The response contains `unknown`, `healthy`, or `unhealthy`, plus `last_checked_at` and a controlled failure `reason` when available. Reading the endpoint is non-blocking: it returns the cached state and schedules a refresh. Legacy/demo and non-HTTP Agents remain `unknown`. Health is deliberately separate from the persisted Agent definition and is not used for routing yet.

## Runs

Submit an asynchronous run:

```bash
curl -i -X POST http://localhost:8080/api/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: research-request-001' \
  -d '{"agent_id":"agt_REPLACE_ME","input":"Summarize durable queues."}'
```

A new run returns `202 Accepted`. Repeating the same key and payload returns `200 OK`, the original run, and `Idempotency-Replayed: true`. Reusing a key with a different payload returns `409 Conflict`.

Run status progresses through `queued`, `running`, and a terminal state: `succeeded`, `failed`, or `canceled`. The response includes `request_id`, `attempt`, `max_attempts`, lifecycle timestamps, and `duration_ms`. Duration is explicit after a terminal transition and measures execution time from `started_at`, or total queued lifetime if execution never started.

```bash
curl http://localhost:8080/api/v1/runs
curl http://localhost:8080/api/v1/runs/run_REPLACE_ME
```

Cancel a queued or running Run:

```bash
curl -X POST http://localhost:8080/api/v1/runs/run_REPLACE_ME/cancel
```

Successful cancellation returns the canceled Run with `200 OK`. A terminal Run returns `409 Conflict`; an unknown Run returns `404 Not Found`. Cancellation clears output/error, records `completed_at`, stops local execution context, and prevents further retries. In multi-replica mode, persisted cancellation cannot be overwritten by a stale worker, but immediate interruption is limited to a worker in the same process until distributed cancellation signaling exists.

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

## Request validation

- JSON bodies are limited to 1 MiB.
- Unknown JSON fields and multiple JSON objects are rejected.
- Agent names, agent IDs, and run input cannot be blank.
- `Idempotency-Key` is optional and limited to 128 characters.
