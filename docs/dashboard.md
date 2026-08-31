# Operations dashboard

`dashboard/` is a self-hosted Next.js App Router application for operational
inspection; the Go API remains the system of record and control plane. It
shows registered Agents, recent Runs, Workflows, MCP approvals, status totals,
Run creation, approval decisions, and a live Run event drawer over SSE.

## Run locally

Node.js 20.9 or newer is required (Node 22 is used by CI and the container):

```bash
cd dashboard
npm ci
AGENTMESH_API_URL=http://127.0.0.1:8080 npm run dev
```

Or run the full stack and open `http://localhost:3000`:

```bash
docker compose up --build
```

The dashboard uses a same-origin Route Handler as a bounded backend-for-
frontend proxy. `AGENTMESH_API_URL` is server-only and must be an absolute HTTP
or HTTPS origin without credentials, query, or fragment. If inbound AgentMesh
authentication is enabled, set `AGENTMESH_API_TOKEN` only on the dashboard
server; it is inserted upstream and never sent to browser JavaScript.

The proxy strips hop-by-hop and client Authorization headers, rejects request
bodies above 1 MiB, disables redirects and caching, and streams upstream
responses without buffering. This allows `EventSource` to receive the existing
`GET /api/v1/runs/{id}/events` stream through the same origin. Reverse proxies
must also leave SSE buffering disabled.

The dashboard is an operations surface, not a new authorization boundary.
Deploy it behind the organization's normal access proxy and give its server
token only the roles needed by the displayed actions. Approval decisions need
an admin token; ordinary mutations need operator/admin; reads accept reader,
operator, or admin.

The implementation follows the official [Next.js installation requirements](https://nextjs.org/docs/app/getting-started/installation),
[Route Handler BFF guidance](https://nextjs.org/docs/app/guides/backend-for-frontend),
and [self-hosted streaming guidance](https://nextjs.org/docs/app/guides/self-hosting).
