# Phase 1 Data Model: Event Agent Runtime

**Date**: 2026-09-14 | **Plan**: [plan.md](./plan.md)

No datastore exists in this feature (FR-013). Everything below is either in-memory runtime state or a wire shape; the only durable state is the consumer-group position Redis holds on this service's behalf.

---

## Envelope

The unit carried in every source entry. **Mirrored field-for-field from `pulse-gateway`'s `internal/model/enriched_payload.go`** — verified against source, not prose. Under Principle I this repo does not get to reshape it.

| Field | JSON | Go type | Required | Notes |
|---|---|---|---|---|
| Event ID | `event_id` | `string` | yes | The idempotency key for every dispatch (FR-011) |
| Gateway ID | `gateway_id` | `string` | yes | Which gateway instance accepted it; joins our logs to theirs (FR-024) |
| Received at | `received_at` | `time.Time` | yes | UTC RFC3339, stamped by the gateway at acceptance |
| Retry count | `retry_count` | `int` | yes | The gateway's own count; **not** this service's dispatch attempt count |
| Stream name | `stream_name` | `string` | no (`omitempty`) | The logical source the gateway wrote to |
| Event header | `event_header` | `json.RawMessage` | **no** | Present only for JWT-authenticated gateway requests, absent for API-key ones (FR-010) |
| Payload | `payload` | `json.RawMessage` | yes | Opaque to this service; never parsed, only forwarded |

Fields the gateway marks `json:"-"` (`destination_type`, `ttl`, error fields) never appear on the wire and have no representation here.

**Validation rules**

- `event_id`, `gateway_id`, `payload` must be present and non-empty; a violation makes the entry unprocessable (FR-012), not a crash.
- `event_header` absent is valid and must not be treated as an error. Any code path that requires it is a contract change under Principle I.
- `payload` and `event_header` are held as raw JSON and forwarded byte-for-byte. Re-marshalling risks key reordering and number-format drift, which would break a destination-side idempotency check.
- `received_at` may be skewed or in the future relative to local time; it is recorded, never used as a correctness input.

---

## Delivery

One envelope as handed up from the ingestion boundary, with its acknowledgement still attached. This is the only type agents see from below.

| Field | Type | Notes |
|---|---|---|
| Envelope | `envelope.Envelope` | Parsed, validated |
| Source | `SourceRef` | Logical name and kind (stream or list) |
| Position | `string` | Opaque above the boundary — a stream entry ID or a list marker; agents must not parse it |
| Raw | `[]byte` | The original bytes, kept for the unprocessable-entry record (FR-012) |
| Ack | `func() error` | `XACK` for a stream; a no-op for a list, whose pop already consumed the entry |
| Nack | `func() error` | Leaves a stream entry pending for later reclaim; for a list, records the loss — nothing can put it back |

**The Ack/Nack asymmetry is the whole reason this type exists.** A list delivery cannot be un-consumed, and that fact must not escape upward into agent code.

---

## Agent

A named, independently configured unit of work, built from `AGENT_<ID>_*` configuration at startup.

| Field | Type | Notes |
|---|---|---|
| ID | `string` | Unique; normalises to `[A-Z0-9_]` for key lookup (D9) |
| Mode | `stream` \| `periodic` | Exactly one trigger (FR-002) |
| Enabled | `bool` | Disabled agents are not constructed (FR-003) |
| Source | `SourceRef` | Required for `stream`; optional for `periodic` |
| Destinations | `[]Destination` | One or more; all stubbed in this feature |
| Interval | `time.Duration` | `periodic` only |
| Batch size / block timeout | `int` / `time.Duration` | `stream` only |

**State transitions**

```text
configured ──start──> running ──ctx cancel──> draining ──> stopped
                 │                              (finishes in-flight, acks only what completed)
                 ├──panic──> recovering ──backoff──> running
                 └──repeated failure──> degraded (still scheduled, anomaly raised)
```

`recovering` and `degraded` are per-agent and never propagate: another agent's state machine is unaffected (FR-004). `draining` must not acknowledge work that did not complete (FR-006).

---

## DispatchOutcome

The record of one agent handing one envelope to one destination.

| Field | Type | Notes |
|---|---|---|
| Event ID | `string` | Joins back to the envelope and the gateway |
| Agent ID / Destination | `string` / `string` | Who and where |
| Attempts | `int` | Bounded by the configured budget (FR-017) |
| Result | `delivered` \| `dropped` | No third state — a dispatch is resolved before the delivery is acked |
| Reason | `string` | Set when `dropped`: last error, or timeout |
| Duration | `time.Duration` | Total across attempts |

A `dropped` outcome is the **only** record that the event existed (FR-013 leaves no datastore), so it is emitted at error level with the full identity and counted in `conflux_dispatch_dropped_total`. It must never be reduced to a counter alone.

---

## LagReading

| Field | Type | Notes |
|---|---|---|
| Agent ID / Source | `string` / `SourceRef` | |
| Entries behind | `int64` | `XINFO GROUPS` lag, or `LLEN` for the list |
| Approximate | `bool` | True when the server reported `NULL` lag and the value was derived (D11) |
| Observed at | `time.Time` | |

`Approximate == true` means entries were trimmed from under the group — data was lost upstream. It propagates into `/readyz` and raises an anomaly rather than being smoothed away.

---

## Anomaly

| Field | Type | Notes |
|---|---|---|
| Type | `lag_threshold` \| `dispatch_drop_rate` \| `source_unavailable` \| `entries_trimmed` | FR-025 |
| Agent ID / Source | `string` | Scope |
| Started at / Last notified at | `time.Time` | Drives the rate limit (FR-026) |
| Active | `bool` | Clearing sends exactly one "resolved" notification |
| Detail | `string` | Observed value against threshold |

**State transitions**: `detected → notified → (still active, suppressed until cooldown) → cleared → resolved-notified`. The suppressed state is what keeps a persistent anomaly from generating a message per check (SC-007).

---

## LogSummary

| Field | Type | Notes |
|---|---|---|
| Period start / end | `time.Time` | Closed, deterministic bucket boundaries (FR-018a) |
| Entry count | `int64` | |
| Breakdown | `map[string]int64` | By gateway, by outcome |
| Generated at | `time.Time` | |

Identity is the period boundary pair, not the generation time. Recomputing a closed period yields a byte-identical summary, which is what makes a repeated run harmless without any remembered state (SC-008).

---

## SourceRef

| Field | Type | Notes |
|---|---|---|
| Name | `string` | From configuration; never a literal in source (Principle I) |
| Kind | `stream` \| `list` | Selects the consumption strategy behind the boundary |

The three stream/list names in the platform today (`ingestion-events`, `ingestion-signals`, `ingestion-logs`) appear only in `.env.example` and in `contracts/ingestion-source.md` — never compiled into the binary.
