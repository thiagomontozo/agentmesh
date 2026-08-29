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

`runtime` and `protocol` are extensible lowercase identifiers. For remote HTTP execution, use `runtime: "remote"` and `protocol: "http"`; `endpoint` is an HTTP or HTTPS base URL and AgentMesh calls its `/v1/runs` path using [Agent Protocol V1](agent-protocol-v1.md). Capabilities remain metadata only: AgentMesh does not route by capability.

List or fetch agents:

```bash
curl http://localhost:8080/api/v1/agents
curl http://localhost:8080/api/v1/agents/agt_REPLACE_ME
```

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
