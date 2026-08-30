# MCP tool gateway

AgentMesh includes a bounded client and registry for stateless Streamable HTTP
servers following MCP revision `2026-07-28`. That revision makes every request
self-describing and requires protocol/method/tool routing headers; the
[official release notes](https://blog.modelcontextprotocol.io/posts/2026-07-28/)
and [Tools specification](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
define the implemented wire shape.

## Server registry

Servers are configured at process startup. Configuration contains no secrets:

```bash
export AGENTMESH_MCP_SERVERS='[
  {
    "id":"search",
    "endpoint":"https://tools.example.com/mcp",
    "allowed_tools":["search","fetch"],
    "denied_tools":["delete"],
    "timeout":"5s"
  }
]'
```

IDs and tool names are case-sensitive, bounded to 128 characters, and limited
to letters, numbers, dots, hyphens, and underscores. Duplicate server IDs,
invalid endpoints, non-positive timeouts, unknown configuration fields, and
malformed policies prevent startup. `denied_tools` always wins; a non-empty
`allowed_tools` list is an allowlist, while an empty list permits all tools not
explicitly denied.

`AGENTMESH_MCP_DEFAULT_TIMEOUT` defaults to `10s`. Per-server timeout overrides
are enforced with `context.WithTimeout`; request and response sizes reuse the
HTTP Runtime limits. The shared secure HTTP client also applies redirect,
scheme, TLS, DNS/IP, CIDR, link-local, and proxy restrictions.

## Discovery and calls

```text
GET  /api/v1/tools/servers
GET  /api/v1/tools?server_id=search&cursor=opaque
POST /api/v1/tools/call
```

Call body:

```json
{
  "server_id": "search",
  "name": "search",
  "arguments": {"q": "AgentMesh"}
}
```

Discovery sends `tools/list`, preserves pagination/cache metadata, validates
tool names, and removes tools denied by local policy. Invocation sends
`tools/call`. Tool-level `isError` remains part of the successful MCP result so
an LLM or caller can inspect it; JSON-RPC, transport, timeout, policy, and schema
failures become controlled HTTP errors.

Every request carries `MCP-Protocol-Version`, `Mcp-Method`, optional `Mcp-Name`,
and the required client `_meta`. This first gateway accepts synchronous
`application/json` responses. It returns `input_required` MRTR data to the API
caller but does not automatically answer it. Streaming `text/event-stream`,
subscriptions, resources, prompts, and MCP Tasks remain outside this bounded
tools increment.

## Authentication

The existing rotating outbound credential map can authenticate an MCP server.
Use the synthetic identity `mcp:<server-id>`:

```json
{
  "mcp:search": {
    "type": "bearer",
    "secret_file": "/run/secrets/search-mcp-token"
  }
}
```

The credential is resolved on every call and is never persisted, logged, or
returned through the API. When inbound AgentMesh authentication is enabled,
registry/discovery require a read-capable role and invocation requires operator
or admin, following the existing method-level RBAC policy.
