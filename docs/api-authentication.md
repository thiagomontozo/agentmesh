# API authentication and authorization

Inbound API security is opt-in to preserve existing local deployments. Set
`AGENTMESH_API_AUTH_CONFIG` to a JSON object keyed by stable subject. Each entry
references a token environment variable; token values are never placed in the
JSON configuration, persistence, logs, events, or API responses.

```text
ADMIN_TOKEN=replace-me
OPERATOR_TOKEN=replace-me
LEGAL_AGENT_TOKEN=replace-me
AGENTMESH_API_AUTH_CONFIG={"admin":{"secret_env":"ADMIN_TOKEN","roles":["admin"]},"automation":{"secret_env":"OPERATOR_TOKEN","roles":["operator"]},"legal-agent":{"secret_env":"LEGAL_AGENT_TOKEN","roles":["agent"],"agent_id":"agt_legal"}}
```

Clients send `Authorization: Bearer <token>`. AgentMesh stores only a SHA-256
digest of each resolved token in memory and compares presented credentials in
constant time. Duplicate tokens, unknown roles, missing secrets, and an `agent`
role without a bound `agent_id` prevent startup.

## Roles

| Role | Access |
| --- | --- |
| `reader` | Read Agents, Runs, Workflows, health state, and event streams. |
| `operator` | Reader access plus normal create/update/delete/start/cancel operations. |
| `admin` | Operator access plus the audit-event endpoint. |
| `agent` | Only `POST /api/v1/runs/{parent}/children`; the bound Agent must own the running parent. |

`GET /healthz` and `GET /readyz` remain public for orchestrator probes. Every
other endpoint is authenticated when configuration is non-empty. Authentication
returns `401` with a Bearer challenge; an authenticated but unauthorized subject
receives `403`.

## Agent-to-Agent identity

With API authentication enabled, AgentMesh derives the caller from the Bearer
credential and ignores `X-AgentMesh-Caller-Agent-ID`. A token bound to
`agt_other` cannot impersonate `agt_legal` by changing a header. The authenticated
Agent ID must equal the parent Run's persisted `agent_id`.

When inbound authentication is disabled, the historical caller header remains
accepted for backward compatibility. That mode is appropriate only for local
development or a trusted authenticated gateway.
