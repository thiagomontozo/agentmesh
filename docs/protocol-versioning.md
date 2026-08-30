# Agent Protocol versioning

Agent Protocol uses explicit integer-string major versions. Version `"1"` is the
only version currently implemented and supported. The version is carried in the
required `protocol_version` JSON field and repeated in the
`Agent-Protocol-Version` request header for early transport inspection. The JSON
field is authoritative.

## Compatibility rules

- additive optional fields may be introduced within V1 when old receivers can
  safely ignore them and new receivers preserve old semantics;
- required fields cannot be removed or reinterpreted within V1;
- enum values are closed unless their field is explicitly documented as open;
- changing success/error meaning, idempotency semantics, or required fields
  requires a new major version;
- AgentMesh sends only versions it explicitly supports and never silently
  downgrades or guesses a version;
- a response must use the same supported major contract as the request.

Existing V1 Agents remain compatible: the request JSON is unchanged, and the new
HTTP header is additive. Implementations should ignore unknown headers and
unknown optional JSON fields, but must reject an unknown `protocol_version`.

## Incompatible versions

The shared `internal/protocol` package owns the supported-version set and returns
a typed `UnsupportedVersionError` wrapping `ErrUnsupportedVersion`. Runtime
errors use the stable code:

```text
unsupported_protocol_version
```

An Agent receiving an unsupported request version should return HTTP `400` with
a response encoded in a version it supports when the Run identity can be read:

```json
{
  "protocol_version": "1",
  "run_id": "run_123",
  "status": "failed",
  "error": {
    "code": "unsupported_protocol_version",
    "message": "unsupported protocol version; supported versions: 1",
    "retryable": false
  }
}
```

`v1.UnsupportedVersionResponse` constructs this controlled response. Messages
must not echo arbitrary payloads, headers, credentials, or endpoint details.

AgentMesh rejects a response declaring an unknown version as a non-retryable
protocol error with the same stable code. A transport `400` remains a permanent
HTTP error because AgentMesh currently sends only V1.

## Future version procedure

A future V2 should be a separate `internal/protocol/v2` wire package. Adding it
requires:

1. implementing and testing the V2 request/response schema;
2. adding `"2"` to the shared supported-version registry;
3. choosing a version from explicit Agent configuration or negotiation metadata;
4. dispatching encoding/validation to the matching version adapter;
5. retaining the V1 adapter while supported;
6. documenting downgrade policy and deprecation before removing V1.

No V2 behavior or negotiation is implemented in this increment. Centralizing
compatibility errors and keeping schemas in versioned packages prevents V2 from
requiring changes to the Engine's runtime contract.
