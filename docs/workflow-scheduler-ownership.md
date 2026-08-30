# Workflow scheduler ownership

Every API-capable replica can receive Workflow start/cancel requests and scan
persisted running Workflows. Reconciliation ownership is coordinated per
Workflow with the existing `coordination.Coordinator`:

```text
workflow:{workflow_id} -> renewable owner token with TTL
```

Only the replica that acquires the lease runs `Manager.reconcile`. It renews the
lease every third of `AGENTMESH_WORKFLOW_LEASE_TTL`. Losing ownership cancels the
reconciliation context before additional Steps are scheduled. Release occurs on
terminal completion, cancellation, graceful shutdown, or reconciliation error.

Each Manager scans persisted running Workflows at half the lease TTL, capped at
five seconds. If an API/scheduler process crashes without releasing ownership,
healthy replicas fail acquisition until the TTL expires and then one takes over.
Stable per-Step idempotency keys and optimistic Workflow versions remain the
second layer preventing duplicate Run/Step state.

`AGENTMESH_WORKFLOW_LEASE_TTL` defaults to `30s`. It should exceed expected Redis
latency and process pauses while remaining short enough for the desired scheduler
recovery time. It is separate from `AGENTMESH_LEASE_TTL`, which protects Run
execution.

## Guarantees and boundary

- at most one healthy lease owner reconciles a Workflow at a time;
- a replacement does not reconcile before abandoned ownership expires;
- ownership is renewed for long Workflows without a goroutine leak;
- recovery reuses existing Runs and Step idempotency identities;
- losing Redis coordination stops new scheduling rather than continuing blindly.

Workflow state does not currently carry a fencing token. A process paused longer
than the entire lease TTL could resume around an ownership change; optimistic
versions reject conflicting Workflow writes, and idempotent Step Run creation
prevents a second Run, but this is not a general fencing guarantee for future
non-idempotent scheduler side effects.
