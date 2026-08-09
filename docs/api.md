# API Guide

The API uses JSON under `/api/v1`. Errors have the form:

```json
{"error":{"message":"description"}}
```

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

Run status progresses through `queued`, `running`, and either `succeeded` or `failed`. The response includes `attempt`, `max_attempts`, and lifecycle timestamps.

```bash
curl http://localhost:8080/api/v1/runs
curl http://localhost:8080/api/v1/runs/run_REPLACE_ME
```

## Server-Sent Events

Stream a run lifecycle:

```bash
curl -N http://localhost:8080/api/v1/runs/run_REPLACE_ME/events
```

Typical event names are:

- `run.queued`
- `run.started`
- `run.retrying`
- `run.succeeded`
- `run.failed`

The stream closes after a terminal event. The current event history is bounded and process-local; PostgreSQL remains the source of truth for final run state.

## Request validation

- JSON bodies are limited to 1 MiB.
- Unknown JSON fields and multiple JSON objects are rejected.
- Agent names, agent IDs, and run input cannot be blank.
- `Idempotency-Key` is optional and limited to 128 characters.
