# Kubernetes, Helm, and GitOps

The chart at `deploy/helm/agentmesh` maps the existing process-role boundary to
separate Go API and Worker Deployments. It also deploys the optional dashboard,
ClusterIP services, startup/readiness/liveness probes, PodDisruptionBudgets,
topology spreading, and `autoscaling/v2` HPAs.

## Preconditions

- Kubernetes 1.27 or newer;
- Helm 3 or 4;
- externally managed PostgreSQL, Redis, and NATS JetStream;
- Metrics Server for resource-utilization HPA;
- immutable backend and dashboard images in a registry;
- a runtime Secret described in the chart README.

Validate and render before applying:

```bash
helm lint deploy/helm/agentmesh --strict
helm template agentmesh deploy/helm/agentmesh \
  --namespace agentmesh \
  --values deploy/helm/agentmesh/values-production.yaml > rendered.yaml
```

The default split starts at two API and two Worker replicas and permits HPA to
scale them independently. API replicas are stateless at the HTTP layer; Workers
share JetStream, PostgreSQL, Redis leases, fencing tokens, persisted events,
and cross-replica cancellation. This is real horizontal execution using the
distributed mode already covered by the multi-replica integration tests—not
goroutine-only concurrency.

HPA is intentionally based on bounded CPU/memory resource metrics. It does not
yet scale directly on JetStream backlog, Run age, or active lease count, so
operators must tune resource requests and min/max replicas from observed load.
Scale-down stabilization reduces churn; PDBs do not protect against voluntary
disruption when a cluster has insufficient capacity.

## GitOps example

`deploy/gitops/argocd/agentmesh.yaml` is an Argo CD `Application` that renders
the in-repository Helm chart, creates the namespace, uses server-side apply,
automatically prunes drift, self-heals, and retries with bounded backoff. Replace
both image placeholders with immutable tags or digests and provision the
referenced Secret before applying it:

```bash
kubectl apply -f deploy/gitops/argocd/agentmesh.yaml
```

Production automation should update the image field in Git through a reviewed
change. It should not issue an imperative rollout that bypasses the declared
state. Secret values remain outside Git; the example references only the Secret
name.

The manifests follow Kubernetes' stable
[`autoscaling/v2` HPA](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/),
the [Helm chart format and best practices](https://helm.sh/docs/topics/charts/),
and Argo CD's [automated sync](https://argo-cd.readthedocs.io/en/stable/user-guide/auto_sync/)
and [Helm source](https://argo-cd.readthedocs.io/en/stable/user-guide/helm/)
contracts.
