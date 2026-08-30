# Agent request authentication

AgentMesh can authenticate Agent Protocol requests with either a Bearer token or
a custom API-key header. Authentication is selected by `agent_id` and applied at
the HTTP Runtime boundary after the protocol request is encoded.

Credentials are not fields of `Agent`, are not accepted or returned by the REST
API, are not persisted in Memory/PostgreSQL/Redis, and are not included in Agent
Protocol JSON, SSE, or structured errors. The initial credential source is the
process environment so deployments can inject values from their existing secret
manager.

## Configuration

`AGENTMESH_AGENT_AUTH_CONFIG` is a JSON object keyed by Agent ID. Each entry
contains a type and the name of another environment variable holding the secret:

```env
LEGAL_AGENT_TOKEN=replace-through-secret-manager
CODE_AGENT_KEY=replace-through-secret-manager
AGENTMESH_AGENT_AUTH_CONFIG={"agt_legal":{"type":"bearer","secret_env":"LEGAL_AGENT_TOKEN"},"agt_code":{"type":"api_key","secret_env":"CODE_AGENT_KEY","header":"X-Agent-Key"}}
```

Bearer authentication always emits:

```http
Authorization: Bearer <secret>
```

API-key authentication defaults to `X-API-Key`; a safe custom header can be
configured. Protocol-owned and hop-by-hop headers such as `Content-Type`,
`Content-Length`, `Host`, `Authorization`, `Idempotency-Key`, and `Connection`
cannot be overridden by API-key configuration.

An Agent absent from the map remains unauthenticated for backward compatibility.
Invalid configuration or an unavailable referenced environment variable prevents
AgentMesh startup. Error messages identify the Agent but never include the secret
or the value of the configuration document.

## Abstraction

`runtime.RequestAuthenticator` receives the selected `domain.Agent` and the
outbound `http.Request`. `StaticAuthenticator` is the initial Bearer/API-key
implementation; `NoAuthentication` preserves legacy behavior. The HTTP Runtime
depends only on this interface, allowing a future Vault, cloud secret manager, or
mTLS implementation without changing the Engine or Agent Protocol body.

## Security boundaries

- environment access and rotation remain deployment responsibilities;
- credentials are loaded at startup; changing the environment requires restart;
- the health-check endpoint remains unauthenticated in this increment;
- inbound AgentMesh API authentication, RBAC, Agent-to-Agent caller proof, and
  mTLS are not implemented here;
- TLS should be required with `AGENTMESH_HTTP_REQUIRE_HTTPS=true` whenever a
  credential crosses an untrusted network;
- operators must avoid printing process environments or secret-manager values.
