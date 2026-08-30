# External effect idempotency

AgentMesh can retry an HTTP Agent after a timeout, network break, or `5xx` response. The first request may already have committed an irreversible effect even though AgentMesh did not receive its result. A per-attempt key cannot safely deduplicate the next attempt because that key changes on retry.

## Contract

Register an Agent that supports effect deduplication with:

```json
{
  "name": "payments-agent",
  "runtime": "remote",
  "protocol": "http",
  "endpoint": "http://payments-agent:9000",
  "effect_idempotency": "required"
}
```

AgentMesh sends both identities on every `POST /v1/runs`:

| Identity | Body | Header | Retry behavior |
| --- | --- | --- | --- |
| Attempt | `idempotency_key` | `Idempotency-Key` | Changes from `<run_id>:1` to `<run_id>:2`. |
| Effect | `effect_idempotency_key` | `Agent-Effect-Idempotency-Key` | Remains the Run ID for every attempt. |

The Agent must use the effect key as the unique key of its externally visible operation. It should atomically persist the completed outcome with the effect or use an equivalent transactional/outbox design. When the same effect key arrives again, it returns the stored outcome instead of repeating the effect.

For a strict Agent, the response must echo `effect_idempotency_key`. Missing or mismatched acknowledgement produces `effect_idempotency_not_acknowledged` and AgentMesh fails the attempt as a protocol error. The acknowledgement is checked for both successful and structured failed responses.

## Compatibility

The fields and header are additive to Agent Protocol V1. AgentMesh sends the stable key to every HTTP Agent so compatible legacy implementations may start using it without a registry update. A legacy Agent with empty `effect_idempotency` does not have to echo the key and retains its existing behavior.

## Guarantee boundary

AgentMesh guarantees stable key generation, reuse across retries, transport in body and header, and strict acknowledgement when configured. It cannot prove that another process used the key atomically. Therefore:

- do not mark an Agent `required` until its implementation deduplicates the underlying effect;
- retain deduplication records for at least the maximum Run/retry/recovery horizon;
- never derive an effect key from the attempt key;
- treat an effect acknowledgement as a protocol assertion by the Agent, not distributed exactly-once delivery.

`TestExternalEffectIsAppliedOnceAcrossRetry` exercises a reference Agent that commits an effect, returns `500`, receives a retry with a different attempt key and the same effect key, and produces exactly one stored effect.
