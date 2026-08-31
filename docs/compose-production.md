# Production Docker Compose on premises

`compose.production.yml` is the hardened, single-host deployment path. It keeps
the development `compose.yml` unchanged and runs independent API and Worker
replicas against shared PostgreSQL, Redis, and NATS JetStream storage.

This is production-capable for one managed Docker host, but it is not host-level
high availability. Every replica and named volume still shares one machine. Use
the Helm chart when failure of the Docker host must not interrupt the service.

## Topology and security defaults

- Nginx is the only service with host ports, terminates TLS on `80/443`, and
  runs as the unprivileged UID/GID `101` on internal ports `8080/8443`.
- PostgreSQL, Redis, and NATS have no published ports and share an internal
  backend network with AgentMesh.
- API and Worker roles run independently and default to two replicas each.
- Dashboard and automation Bearer tokens are mandatory Docker secrets. The
  dashboard receives an admin identity because it exposes approval decisions;
  the separate automation identity has the operator role.
- Nginx requires TLS-protected HTTP Basic authentication for the dashboard and
  its BFF. Direct `/api/v1/*` automation continues to use AgentMesh Bearer/RBAC.
- Dependency passwords/tokens are generated as URL-safe Docker secrets and are
  not present in Compose interpolation, container arguments, or the Git tree.
- Containers have bounded CPU, memory, PIDs, and rotated JSON logs. Application
  containers use read-only filesystems, all capabilities are dropped, and
  privilege escalation is disabled.
- The edge, backend, and egress networks separate inbound traffic, durable
  dependencies, and outbound Agent calls.
- The backend image includes the standard CA bundle. A private corporate CA must
  be added to the image trust store during the internal image build.

## 1. Mirror immutable images

Build AgentMesh from the reviewed commit and push it to the internal registry:

```bash
VERSION=$(git rev-parse --short=12 HEAD)
REGISTRY=registry.intra.example/agentmesh

docker build --pull -t "$REGISTRY/backend:$VERSION" .
docker build --pull -t "$REGISTRY/dashboard:$VERSION" ./dashboard
docker push "$REGISTRY/backend:$VERSION"
docker push "$REGISTRY/dashboard:$VERSION"
```

Mirror pinned PostgreSQL 17, NATS 2.12, Redis 8.2, and Nginx 1.27.3 or newer
images into the same registry. Prefer image digests in `.env.production`; the
example tags are intentionally placeholders, not a supply-chain guarantee.

For an internal CA, derive the backend image before publishing it:

```dockerfile
FROM registry.intra.example/agentmesh/backend:REVIEWED_COMMIT
USER root
COPY corporate-root-ca.crt /usr/local/share/ca-certificates/corporate-root-ca.crt
RUN update-ca-certificates
USER agentmesh
```

## 2. Configure the environment

```bash
cp .env.production.example .env.production
chmod 600 .env.production
```

Edit every image reference and the externally visible bind address/ports. Keep
`AGENTMESH_NATS_ACK_WAIT` above `AGENTMESH_ATTEMPT_TIMEOUT`. Set
`AGENTMESH_HTTP_REQUIRE_HTTPS=false` only when remote Agents are deliberately
served as HTTP on a trusted network. Use `AGENTMESH_HTTP_ALLOWED_HOSTS` to bound
which Agent/LLM hostnames operators may register.

Do not put credential values in `.env.production`. The file is ignored by Git;
only `.env.production.example` is versioned. The root `.dockerignore` also
excludes all environment and secret paths from the backend build context.

## 3. Prepare secrets and TLS

Supply a certificate whose SAN contains the DNS name used by clients and its
unencrypted PEM private key. The preparation script preserves existing secrets
instead of silently rotating them:

```bash
./scripts/prepare-production-compose.sh \
  /secure/source/agentmesh-fullchain.pem \
  /secure/source/agentmesh-private-key.pem
```

It creates an owner-only mode-`0700` directory under `./secrets/agentmesh`,
which is ignored by Git. Individual files are readable because Compose mounts
them into deliberately non-root containers; the directory prevents other host
users from traversing to them. Back these secrets up through the organization's
secret manager. The passwords are hexadecimal so they are safe inside the
dependency connection URLs assembled only inside the AgentMesh containers.

## 4. Validate and start

```bash
docker compose \
  --env-file .env.production \
  -f compose.production.yml \
  config -q

docker compose \
  --env-file .env.production \
  -f compose.production.yml \
  pull

docker compose \
  --env-file .env.production \
  -f compose.production.yml \
  up -d --remove-orphans --wait
```

Inspect the deployment without printing its environment or secrets:

```bash
docker compose --env-file .env.production -f compose.production.yml ps
docker compose --env-file .env.production -f compose.production.yml logs --tail=100 gateway api worker
curl --fail --cacert /secure/source/corporate-root-ca.crt https://agentmesh.intra.example/readyz
```

The public surface is the TLS gateway. `/healthz` and `/readyz` remain public
for probes; `/api/v1/*` is protected by AgentMesh Bearer authentication;
the dashboard and its BFF require the generated Basic Auth username `agentmesh`;
`/metrics` and all dependency ports are intentionally not exposed. Read the
dashboard password from `./secrets/agentmesh/dashboard_basic_auth_password`.

## 5. Generate a result

Read the operator token locally without copying it into shell history:

```bash
read -r TOKEN < ./secrets/agentmesh/automation_api_token
API=https://agentmesh.intra.example
```

First prove the queue and persistence path with a deterministic demo Agent:

```bash
AGENT_ID=$(
  curl --fail --silent --show-error -X POST "$API/api/v1/agents" \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{"name":"Production smoke test"}' | jq -r .id
)

RUN_ID=$(
  curl --fail --silent --show-error -X POST "$API/api/v1/runs" \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: production-smoke-$(date +%s)" \
    -d "{\"agent_id\":\"$AGENT_ID\",\"input\":\"Validate the production queue.\"}" | jq -r .id
)

curl --no-buffer -H "Authorization: Bearer $TOKEN" \
  "$API/api/v1/runs/$RUN_ID/events"

curl --fail --silent --show-error \
  -H "Authorization: Bearer $TOKEN" \
  "$API/api/v1/runs/$RUN_ID" |
  jq '{id,status,output,error,attempt,duration_ms}'
```

The demo output verifies AgentMesh, not AI quality. Register an `llm/openai`
Agent pointing to an internal OpenAI-compatible endpoint, or a `remote/http`
Agent implementing Agent Protocol V1, to produce real model/domain results.
The generated `outbound_agent_token` is exposed to API/Workers only as
`OUTBOUND_AGENT_TOKEN`; after the Agent ID exists, set a non-secret mapping such
as `AGENTMESH_AGENT_AUTH_CONFIG={"agt_ID":{"type":"bearer","secret_env":"OUTBOUND_AGENT_TOKEN"}}`
and recreate API/Workers. Use a small Compose override with additional Docker
secrets when different Agents require independent credentials.

## Operations

Change `API_REPLICAS` or `WORKER_REPLICAS` in `.env.production` and rerun
`up -d` to scale processes on the host. Worker scaling remains CPU/memory and
operator driven; Compose does not provide the Kubernetes HPA.

Back up PostgreSQL regularly:

```bash
docker compose --env-file .env.production -f compose.production.yml \
  exec -T postgres pg_dump -U agentmesh -d agentmesh -Fc > agentmesh.dump
```

Also snapshot the NATS and Redis named volumes using the organization's volume
backup mechanism. Test restores; file copies of live volumes are not a backup
strategy.

Never run `docker compose down -v` against production: `-v` deletes the three
named data volumes. Use ordinary `stop`, `restart`, or `down` only after the
backup and maintenance procedure has been approved.

For an upgrade, publish new immutable images, update `.env.production`, run
`pull`, validate `config`, and run `up -d`. Compose does not guarantee a
zero-downtime rolling replacement, so use a maintenance window or the Kubernetes
deployment for strict availability requirements.

API token mappings are loaded at process startup. After deliberately rotating
`dashboard_api_token` or `automation_api_token`, recreate the affected services:

```bash
docker compose --env-file .env.production -f compose.production.yml \
  up -d --force-recreate api worker dashboard
```

Rotating PostgreSQL, Redis, or NATS credentials requires a coordinated
dependency credential change; replacing only the secret file will break the
connection. The preparation script therefore never overwrites existing files.

## Known boundaries

- Compose provides process redundancy, not redundancy against host/storage
  failure and not multi-host scheduling.
- PostgreSQL-to-AgentMesh, Redis, and NATS traffic is plaintext inside the
  isolated backend bridge. Use externally managed TLS-enabled dependencies when
  the host/network trust boundary requires transport encryption.
- Nginx TLS protects client ingress; mTLS between AgentMesh and Agents is not
  implemented.
- Remote effects remain cooperative: strict Agents must atomically honor
  `effect_idempotency_key`.
- The bundled dependencies need external monitoring, capacity planning,
  retention, backup, restore, and disaster-recovery procedures.
