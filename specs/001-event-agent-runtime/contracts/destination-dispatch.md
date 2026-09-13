# Contract: Destination Dispatch (outbound)

**Direction**: outbound. **Every destination in this feature is a stub** (Constitution Principle V, FR-015). This file fixes the shape of the call so the real implementations slot in without reshaping the agent.

## Go interface

```go
// Dispatch delivers one envelope to one destination.
// Implementations MUST be safe for concurrent use and MUST NOT retry internally —
// the attempt budget, backoff, and per-attempt timeout belong to the caller.
type Destination interface {
    Name() string
    Dispatch(ctx context.Context, env envelope.Envelope) error
}
```

`ctx` already carries the per-attempt timeout (FR-017a). An implementation that ignores it, or that retries on its own, breaks the bounded-loss guarantee FR-017b measures.

## Wire shape (HTTP stubs)

`POST <destination URL>`

```http
Content-Type: application/json
Idempotency-Key: <event_id>
X-Conflux-Agent: <agent id>
X-Conflux-Attempt: <1-based attempt number>
```

```json
{
  "event_id": "…",
  "gateway_id": "…",
  "received_at": "2026-09-14T09:41:07Z",
  "retry_count": 0,
  "stream_name": "ingestion-events",
  "event_header": { },
  "payload": { }
}
```

`payload` and `event_header` are forwarded **byte-for-byte** as received. `event_header` is omitted entirely when absent — never sent as `null`.

`Idempotency-Key` carries `event_id` because redelivery is normal operation (FR-011); it is how a real destination absorbs a duplicate.

## Outcome mapping

| Response | Treated as | Retried? |
|---|---|---|
| 2xx | delivered | no |
| 408, 429, 5xx | transient failure | yes, within budget |
| 4xx other than 408/429 | permanent failure | **no** — drop immediately, retrying a rejected payload only burns the budget |
| timeout / connection error | transient failure | yes, within budget |

Budget exhausted, or a permanent failure, produces a `dropped` [DispatchOutcome](../data-model.md#dispatchoutcome) — logged in full at error level and counted. Drops feed the `dispatch_drop_rate` anomaly (FR-017b).

## Destinations in this feature

| Name | Config key | Stub behavior |
|---|---|---|
| `ltr` | `DEST_LTR_URL` | HTTP stub; the search-ranking service's real contract is not yet specified |
| `cep` | `DEST_CEP_URL` | HTTP stub; the internal CEP engine's real contract is not yet specified |
| `email` | `SMTP_ADDR`, `SUPPORT_EMAIL_TO` | Stub sink while `EMAIL_ENABLED=false`; carries anomaly notifications, not envelopes |

Tests assert on the calls a stub received (SC-010). No test reaches a real system.
