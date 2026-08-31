# AgentMesh Helm chart

This chart deploys the existing Go binary as independent `api` and `worker`
roles, plus the optional Next.js dashboard. PostgreSQL, Redis, and NATS are
external durable dependencies and are deliberately not embedded as chart
subdependencies.

Create the runtime Secret before installing:

```bash
kubectl -n agentmesh create secret generic agentmesh-runtime \
  --from-literal=database-url='postgres://...' \
  --from-literal=nats-url='nats://...' \
  --from-literal=redis-url='redis://...' \
  --from-literal=api-auth-config='' \
  --from-literal=agent-auth-config='' \
  --from-literal=dashboard-api-token=''

helm upgrade --install agentmesh deploy/helm/agentmesh \
  --namespace agentmesh --create-namespace \
  --set image.tag=REPLACE_ME \
  --set dashboard.image.tag=REPLACE_ME
```

Use an External Secrets operator, CSI driver, Sealed Secret, or equivalent in
production instead of committing credentials. Set `secrets.existingSecret` to
its generated Secret. Inline secret values exist only for local automation and
are omitted whenever `existingSecret` is set.

API and Worker HPAs use `autoscaling/v2` CPU and memory utilization with a
five-minute scale-down stabilization window. Resource requests are mandatory
for utilization-based HPA operation; a cluster metrics-server is also required.
When HPA is enabled, Deployments omit `spec.replicas`, preventing Helm/GitOps
from fighting the autoscaler. PDBs and hostname topology spreading preserve a
minimum replica and reduce correlated disruption.

Workers expose the existing metrics-only listener on port `9090`; TCP probes
avoid needing an API listener or authentication token. API probes use public
`/healthz` for liveness and dependency-aware `/readyz` for readiness. Kubernetes
sends SIGTERM and allows 35 seconds for the application's bounded graceful
shutdown.

The chart does not claim exactly-once external effects. Horizontal safety still
depends on PostgreSQL fencing, Redis leases, NATS JetStream, idempotent Agent
effects, and correctly sized lease/timeout values. CPU HPA reacts to resource
pressure, not queue depth; custom JetStream backlog metrics can be added later
without changing the workload split.
