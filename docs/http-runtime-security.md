# HTTP Runtime security

Remote Agent registration is a privileged network capability: AgentMesh sends a
POST request to the registered endpoint. The production HTTP Runtime applies a
destination policy at URL validation and again when opening each TCP connection.

## Defaults

- `http` and `https` are accepted; production can require HTTPS;
- private and loopback destinations are allowed for compatibility with internal
  Agents and local development;
- IPv4/IPv6 link-local destinations are blocked, including the common
  `169.254.169.254` cloud metadata address;
- URL credentials, query strings, fragments, redirects, and environment HTTP
  proxies are not accepted;
- DNS is resolved for every new connection and the selected address is checked
  immediately before dialing, reducing DNS-rebinding exposure;
- TLS uses version 1.2 or newer;
- automatic response decompression is disabled and encoded responses are
  rejected, preventing compressed responses from bypassing the body limit;
- request and response bodies are limited to 1 MiB by default;
- only controlled `Content-Type`, `Accept`, `Accept-Encoding`, and idempotency
  headers are emitted by this stage.

## Destination policy

| Variable | Default | Effect |
| --- | --- | --- |
| `AGENTMESH_HTTP_REQUIRE_HTTPS` | `false` | Reject non-TLS Agent endpoints |
| `AGENTMESH_HTTP_ALLOW_PRIVATE_NETWORKS` | `true` | Permit RFC private addresses |
| `AGENTMESH_HTTP_ALLOW_LOOPBACK` | `true` | Permit localhost/loopback addresses |
| `AGENTMESH_HTTP_ALLOW_LINK_LOCAL` | `false` | Permit link-local and metadata networks |
| `AGENTMESH_HTTP_ALLOWED_HOSTS` | empty | Comma-separated exact hosts or `*.example.com`; empty allows any hostname subject to IP policy |
| `AGENTMESH_HTTP_BLOCKED_CIDRS` | empty | Additional comma-separated IPv4/IPv6 CIDRs to reject |
| `AGENTMESH_HTTP_MAX_REQUEST_BYTES` | `1048576` | Maximum encoded protocol request size |
| `AGENTMESH_HTTP_MAX_RESPONSE_BYTES` | `1048576` | Maximum unencoded response size |

Explicit blocked CIDRs take precedence over the general private, loopback, and
link-local switches. A hostname allowlist does not bypass resolved-address
checks. Wildcards match subdomains only: `*.example.com` does not match the bare
`example.com` host.

Example production policy for internal Agents under one DNS suffix:

```env
AGENTMESH_HTTP_REQUIRE_HTTPS=true
AGENTMESH_HTTP_ALLOWED_HOSTS=*.agents.internal
AGENTMESH_HTTP_ALLOW_PRIVATE_NETWORKS=true
AGENTMESH_HTTP_ALLOW_LOOPBACK=false
AGENTMESH_HTTP_ALLOW_LINK_LOCAL=false
AGENTMESH_HTTP_BLOCKED_CIDRS=10.0.0.0/24
```

## Remaining boundaries

The Runtime policy is defense in depth, not a replacement for egress firewall,
service-mesh, or Kubernetes NetworkPolicy controls. Existing keep-alive
connections retain their already-validated destination. DNS answers can change
between new connections, but each new address is checked. TLS does not by itself
authenticate an Agent beyond normal server certificate validation; request
authentication is a separate protocol increment.
