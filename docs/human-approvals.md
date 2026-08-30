# Human approval gates

AgentMesh can require a persisted, single-use human approval before invoking
selected MCP tools. The policy is declarative per MCP server:

```json
[
  {
    "id": "operations",
    "endpoint": "https://tools.example.com/mcp",
    "allowed_tools": ["inspect", "deploy"],
    "approval_required_tools": ["deploy"],
    "timeout": "10s"
  }
]
```

An approval is bound to the server ID, exact tool name, and SHA-256 hash of the
canonical JSON arguments. It cannot authorize a different operation. Records
are persisted in Memory or PostgreSQL and use atomic state transitions so two
replicas cannot consume the same approval.

## Lifecycle

1. An operator creates a pending request with `POST /api/v1/approvals`.
2. An administrator approves or rejects it with
   `POST /api/v1/approvals/{id}/approve` or `/reject`.
3. The caller supplies the approved `approval_id` to `POST /api/v1/tools/call`.
4. AgentMesh atomically changes `approved` to `consumed` before invoking MCP.

```json
{
  "server_id": "operations",
  "tool_name": "deploy",
  "arguments": {"target": "production"},
  "reason": "release 2026.08"
}
```

```json
{
  "server_id": "operations",
  "name": "deploy",
  "arguments": {"target": "production"},
  "approval_id": "apr_REPLACE_ME"
}
```

Missing approval returns `428`; mismatched, rejected, pending, or already used
approval returns `409`; expired approval returns `410`. The approval remains
consumed if the downstream call fails. This fail-closed behavior prevents an
ambiguous external effect from being replayed with the same authorization.

`AGENTMESH_APPROVAL_TTL` defaults to `15m` and bounds the decision/use window.
`AGENTMESH_APPROVAL_RETENTION` defaults to `720h` (30 days); expired historical
records older than that window are pruned during creation. With API
authentication enabled, create/call requires operator or admin, reads accept
reader/operator/admin, and approve/reject requires admin.

The gate currently protects MCP tool effects. It does not pause Runs or
workflow steps, and it is not a general-purpose authorization system.
