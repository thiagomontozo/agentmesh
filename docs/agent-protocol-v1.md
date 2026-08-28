# Agent Protocol V1

Agent Protocol V1 is the language-neutral HTTP/JSON contract between AgentMesh and a remote Agent. Agents registered with `runtime: "remote"`, `protocol: "http"`, and a base `endpoint` are invoked through the HTTP runtime.

## Transport

- Method: `POST`
- Path: `/v1/runs`
- Request and response content type: `application/json`
- Protocol version: `1`
- One response completes one execution attempt; V1 has no streaming.

## Request

```json
{
  "protocol_version": "1",
  "run_id": "run_123",
  "agent_id": "agent_456",
  "attempt": 1,
  "idempotency_key": "run_123:1",
  "input": "Analyze this document"
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `protocol_version` | string | yes | Must be `"1"`. |
| `run_id` | string | yes | AgentMesh Run identifier. |
| `agent_id` | string | yes | Definition selected for this Run. |
| `attempt` | integer | yes | Current attempt, starting at 1. |
| `idempotency_key` | string | yes | Stable identity for this attempt; AgentMesh uses `<run_id>:<attempt>`. |
| `input` | string | yes | Opaque input for the Agent; an empty string is valid at protocol level. |

## Successful response

```json
{
  "protocol_version": "1",
  "run_id": "run_123",
  "status": "succeeded",
  "output": "Analysis completed"
}
```

## Failed response

```json
{
  "protocol_version": "1",
  "run_id": "run_123",
  "status": "failed",
  "error": {
    "code": "agent_overloaded",
    "message": "Try again later",
    "retryable": true
  }
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `protocol_version` | string | yes | Must be `"1"`. |
| `run_id` | string | yes | Must match the request `run_id`. |
| `status` | string | yes | `succeeded` or `failed`. |
| `output` | string | no | Agent output on success; omitted when empty and forbidden on failure. |
| `error` | object | on failure | Structured failure; forbidden on success. |
| `error.code` | string | on failure | Stable machine-readable code chosen by the Agent. |
| `error.message` | string | on failure | Human-readable diagnostic without secrets. |
| `error.retryable` | boolean | on failure | Whether retrying may succeed. This is advisory; AgentMesh owns retry policy. |

The AgentMesh HTTP runtime validates the HTTP status, JSON syntax, response size, media type, protocol version, and matching `run_id`. It does not follow redirects.

## HTTP status

- `200 OK`: a syntactically valid `succeeded` or `failed` protocol response.
- `400 Bad Request`: malformed JSON or invalid required fields.
- `409 Conflict`: an idempotency key was reused with a different request identity.
- `422 Unprocessable Entity`: the Agent cannot process a valid request.
- `429 Too Many Requests`: temporary Agent capacity limit.
- `500`–`599`: temporary or permanent server failure as determined by the eventual HTTP runtime.

Non-`200` responses should use the same structured `error` object when possible, but callers must not assume that an intermediary always returns JSON.

## Idempotency

The pair `(run_id, attempt)` identifies one attempt and produces the stable `idempotency_key` `<run_id>:<attempt>`. A remote Agent should store or otherwise deduplicate completed side effects for a retention period appropriate to its workload and return the same outcome when it receives the same key again. A new AgentMesh retry attempt has a new attempt number and therefore a new key.

V1 does not promise exactly-once delivery. The remote Agent remains responsible for making its own side effects idempotent.

## Interoperability example

Python, Node.js, Go, or any other implementation can serve `POST /v1/runs`, decode these JSON fields, and return one of the response shapes above. No Go interface or AgentMesh internal package is part of the wire contract.

## Out of scope for V1

- streaming, SSE, or WebSocket messages;
- workflows and multi-Agent routing;
- MCP, tool calling, or planners;
- authentication and authorization (reserved for a later protocol-compatible extension).
