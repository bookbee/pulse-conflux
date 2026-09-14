# Contract: Ingestion Source (consumed)

**Direction**: inbound. This service is the consumer; `pulse-gateway` is the producer.
**Authority**: `../pulse-infra/docs/stack-contract.md`. Where this file disagrees with it, that file wins (Constitution Principle I). Verified against `pulse-gateway`'s `internal/queue/redis/producer.go` and `internal/model/enriched_payload.go` on 2026-09-14.

## What the gateway writes

| Logical source | Redis structure | Written with | Cap (local) |
|---|---|---|---|
| `ingestion-events` | Stream | `XADD` | `MAXLEN ~10000` (approximate) |
| `ingestion-signals` | Stream | `XADD` | `MAXLEN ~10000` (approximate) |
| `ingestion-logs` | List | Lua `RPUSH` + `EXPIRE` | 10000 entries, TTL 120s |

Names are configuration in this service (`REDIS_STREAM_EVENTS`, `REDIS_STREAM_SIGNALS`, `REDIS_LIST_LOGS`), never literals.

## Entry shape

**A stream entry has exactly one field, named `data`**, whose value is the JSON envelope — not one Redis field per envelope key. Confirmed in the producer: `Values: []string{"data", payloadJSON}`. A consumer that iterates fields generically will work by accident today and break on the first added field; read `data` by name.

List entries are the same JSON envelope as the element value.

Envelope fields, types, and optionality: see [data-model.md](../data-model.md#envelope).

## What this service may assume

- The envelope is identical across all three sources, so parsing is shared.
- `event_header` is present **only** for JWT-authenticated gateway requests. Absent is normal.
- `event_id` is unique per accepted request and is the idempotency key.

## What this service must NOT assume

- **That entries persist.** Streams trim to an approximate `MAXLEN` and also age-trim (`StreamRetentionSeconds` in the producer). The list's Lua script **drops writes outright** at its cap and returns `LOGS_LIST_FULL`. An entry can vanish before it is read.
- **That a consumer group exists.** The gateway only `XADD`s. This service issues `XGROUP CREATE <key> <group> $ MKSTREAM` and owns every pending entry and claim from then on (FR-009).
- **That falling behind slows the producer.** Neither destination applies backpressure. Lag means loss upstream, which is why lag is the health signal (FR-022).
- **That the list can be shared.** Reading a list is destructive; a second consumer competes for entries. This service is the sole consumer of `ingestion-logs`. Destructive reads also mean **no entry can be read twice** — there is no re-reading a period to recompute anything.
- **That the list's cap trims like a stream's.** It does not. The Lua script checks `LLEN` first and returns `LOGS_LIST_FULL` *instead of* pushing, so at 10k entries the **gateway's write is refused** — old entries are never evicted to make room. A slow consumer here causes upstream log loss directly.
- **That the 120s TTL expires entries.** It does not. `EXPIRE` is re-issued on the key on *every* push, so the TTL is sliding: the list survives as long as writes keep arriving, and the whole key — unread entries included — disappears after 120 seconds of silence.

## Acknowledgement

| Source kind | Ack | Nack |
|---|---|---|
| Stream | `XACK <key> <group> <id>` | Leave pending; reclaimed later by `XAUTOCLAIM` |
| List | No-op — the pop already consumed it | Nothing can restore it; record the loss |

## Failure modes to handle

- `BUSYGROUP` on group creation — benign, the group already exists.
- `NULL` lag from `XINFO GROUPS` — entries were trimmed from under the group; treat as approximate and anomalous (D11).
- Auth failure or unreachable Redis — back off; never spin hot.
- Write refusal under `maxmemory-policy noeviction` — the policy is load-bearing and must not be worked around.
