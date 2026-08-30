# Agent Protocol V1

Agent Protocol V1 is the language-neutral HTTP/JSON contract between AgentMesh and a remote Agent. Agents registered with `runtime: "remote"`, `protocol: "http"`, and a base `endpoint` are invoked through the HTTP runtime.

Transport authentication is orthogonal to the JSON contract. AgentMesh can add
an Agent-specific Bearer or API-key header without changing the request body; see
[Agent request authentication](agent-authentication.md). Agents must never echo
credentials in protocol errors or outputs.

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
  "effect_idempotency_key": "run_123",
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
| `effect_idempotency_key` | string | no (sent by AgentMesh) | Stable identity for externally visible effects; reused by every attempt of the Run. |
| `input` | string | yes | Opaque input for the Agent; an empty string is valid at protocol level. |

## Successful response

```json
{
  "protocol_version": "1",
  "run_id": "run_123",
  "status": "succeeded",
  "output": "Analysis completed",
  "effect_idempotency_key": "run_123"
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
| `effect_idempotency_key` | string | for strict Agents | Echoes the request key to acknowledge the effect-deduplication contract. |
| `error.code` | string | on failure | Stable machine-readable code chosen by the Agent. |
| `error.message` | string | on failure | Human-readable diagnostic without secrets. |

## Version compatibility

V1 uses the required body field and the additive
`Agent-Protocol-Version: 1` request header. Unsupported versions use the stable
`unsupported_protocol_version` error code and are never silently downgraded.
Backward-compatibility rules and the future V2 extension path are specified in
[Agent Protocol versioning](protocol-versioning.md).
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

V1 exposes two different identities:

- `(run_id, attempt)` produces `idempotency_key` `<run_id>:<attempt>`. It identifies one transport/execution attempt and changes when AgentMesh retries.
- `effect_idempotency_key` identifies the logical Run effect and is reused unchanged by every retry. AgentMesh also sends it as `Agent-Effect-Idempotency-Key`.

An Agent registered with `effect_idempotency: "required"` must atomically store or deduplicate its irreversible effect using `effect_idempotency_key`, then echo the key in every valid protocol response. A missing or different acknowledgement is a non-retryable protocol error. Legacy Agents may omit the response field.

This is a cooperative at-most-once-effect contract, not a claim that AgentMesh can inspect an external database or guarantee global exactly-once delivery. The remote Agent owns the atomic relationship between its effect and its stored outcome, including a suitable retention policy. See [External effect idempotency](external-effect-idempotency.md).

## Interoperability example

Python, Node.js, Go, or any other implementation can serve `POST /v1/runs`, decode these JSON fields, and return one of the response shapes above. No Go interface or AgentMesh internal package is part of the wire contract.

## Out of scope for V1

- streaming, SSE, or WebSocket messages;
- workflows and multi-Agent routing;
- MCP, tool calling, or planners;
- authentication and authorization (reserved for a later protocol-compatible extension).
