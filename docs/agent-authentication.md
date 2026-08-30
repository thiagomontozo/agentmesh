# Agent request authentication

AgentMesh can authenticate Agent Protocol requests with either a Bearer token or
a custom API-key header. Authentication is selected by `agent_id` and applied at
the HTTP Runtime boundary after the protocol request is encoded.

Credentials are not fields of `Agent`, are not accepted or returned by the REST
API, are not persisted in Memory/PostgreSQL/Redis, and are not included in Agent
Protocol JSON, SSE, or structured errors. Credentials may come from the process
environment or an absolute mounted-secret file.

## Configuration

`AGENTMESH_AGENT_AUTH_CONFIG` is a JSON object keyed by Agent ID. Each entry
contains a type and exactly one non-secret reference:

```env
LEGAL_AGENT_TOKEN=replace-through-secret-manager
CODE_AGENT_KEY=replace-through-secret-manager
AGENTMESH_AGENT_AUTH_CONFIG={"agt_legal":{"type":"bearer","secret_env":"LEGAL_AGENT_TOKEN"},"agt_code":{"type":"api_key","secret_env":"CODE_AGENT_KEY","header":"X-Agent-Key"}}
```

A mounted file supports rotation without restart:

```env
AGENTMESH_AGENT_AUTH_CONFIG={"agt_legal":{"type":"bearer","secret_file":"/run/secrets/legal-agent-token"}}
```

`secret_file` must be absolute, is limited to 64 KiB, and is read for every
outbound request. A secret manager can atomically replace the mounted file and
the next attempt uses the new value. `secret_env` is also resolved through the
provider for every request; normal operating systems do not externally mutate a
running process environment, so mounted files are the practical rotation path.

Bearer authentication always emits:

```http
Authorization: Bearer <secret>
```

API-key authentication defaults to `X-API-Key`; a safe custom header can be
configured. Protocol-owned and hop-by-hop headers such as `Content-Type`,
`Content-Length`, `Host`, `Authorization`, `Idempotency-Key`, and `Connection`
cannot be overridden by API-key configuration.

An Agent absent from the map remains unauthenticated for backward compatibility.
Invalid configuration or an unavailable referenced secret prevents AgentMesh
startup. If a source later disappears, that request fails authentication without
reusing a cached value. Error messages identify the Agent but never include the secret
or the value of the configuration document.

## Abstraction

`runtime.RequestAuthenticator` receives the selected `domain.Agent` and the
outbound `http.Request`. `SecretProvider` resolves opaque references at request
time; `NewEnvironmentFileSecretProvider` implements environment and mounted-file
sources. A future Vault or cloud provider can implement the same interface
without changing the Engine, HTTP Runtime, or Agent Protocol body.

## Security boundaries

- source access, atomic file replacement, and rotation timing remain deployment responsibilities;
- the health-check endpoint remains unauthenticated in this increment;
- inbound AgentMesh API authentication and RBAC are separate from this outbound boundary;
- mTLS is not implemented;
- TLS should be required with `AGENTMESH_HTTP_REQUIRE_HTTPS=true` whenever a
  credential crosses an untrusted network;
- operators must avoid printing process environments or secret-manager values.
