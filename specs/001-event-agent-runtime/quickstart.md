# Quickstart: Event Agent Runtime

**Date**: 2026-09-14 | **Plan**: [plan.md](./plan.md)

Five scenarios that prove the feature end-to-end against the real stack. Each maps to a success criterion. Nothing here reaches an external system — every destination is a stub (SC-010).

## Prerequisites

- Go 1.25.x — `go version`
- Docker running — the stack needs it
- `redis-cli` (or use `docker exec pulse-redis redis-cli`)
- The local stack, **`full` profile** — Redis exists only there:

```bash
cd ../pulse-infra && make up && make health
```

`make reset` between profile switches; `lite` and `core`/`full` both bind host port 19092 and must not run together.

## Setup

```bash
cp .env.example .env          # .env is gitignored; defaults target the local stack
go build ./...
```

Minimum `.env` additions for this walkthrough (full list in [contracts/configuration.md](./contracts/configuration.md)):

```bash
CONFLUX_AGENTS=events_ltr
AGENT_EVENTS_LTR_MODE=stream
AGENT_EVENTS_LTR_SOURCE=ingestion-events
AGENT_EVENTS_LTR_DESTINATIONS=ltr
DEST_LTR_URL=http://localhost:9999/stub/ltr
```

Run a throwaway stub sink on 9999 in another terminal (any HTTP echo that returns 200 works), then:

```bash
go run ./cmd/conflux
```

Expect: structured JSON on stdout, the consumer group created on first start, `/livez` and `/readyz` answering on `:8090`.

---

## Scenario 1 — An agent drains a stream (US1, SC-002)

Seed three envelopes. Note the single field named `data` — that shape is the contract, not a convenience:

```bash
redis-cli -a pulse-local-not-a-secret XADD ingestion-events '*' data \
  '{"event_id":"evt-1","gateway_id":"gw-local","received_at":"2026-09-14T09:00:00Z","retry_count":0,"stream_name":"ingestion-events","payload":{"hello":"world"}}'
```

Repeat for `evt-2`, `evt-3`.

**Expect**: three `POST`s at the stub, each with `Idempotency-Key: evt-N` and the payload byte-identical to what was seeded; `conflux_events_processed_total` at 3; `XPENDING ingestion-events <group>` empty once acked.

**Also verify FR-010**: seed an envelope with **no** `event_header` (as above — API-key gateway requests omit it). It must dispatch normally, not error.

## Scenario 2 — Restart loses nothing (US1, SC-006)

Stop the service mid-stream (Ctrl-C while seeding), then restart it.

**Expect**: consumption resumes from the last acknowledged position; entries read but not acked before shutdown are redelivered and dispatched; no gap, no skipped `event_id`. Duplicates at the stub are expected and correct — that is FR-011, and the `Idempotency-Key` is how a real destination absorbs them.

## Scenario 3 — A failing destination drops instead of stalling (US3, SC-002, SC-011)

Point the stub at a sink that returns `500`, or stop it entirely. Keep seeding.

**Expect**:
- `DISPATCH_MAX_ATTEMPTS` attempts per event with backoff between them, each bounded by `DISPATCH_TIMEOUT`.
- Then one `error` line per event carrying `event_id`, `gateway_id`, destination, attempt count, and reason — this record is the only trace the event existed (FR-013 leaves no datastore).
- `conflux_dispatch_dropped_total` climbing; `conflux_agent_lag_entries` **staying flat** — the point of the bounded budget is that a dead destination never becomes the reason the service falls behind.
- Support notified once when the drop rate crosses `DROP_RATE_THRESHOLD`, not once per event (SC-007).

Then bring the stub back: dispatching resumes and one "resolved" notification follows.

## Scenario 4 — Lag is visible and readiness reflects it (US3, SC-004)

Pause the agent (SIGSTOP the process, or disable the agent and restart) and seed past `LAG_THRESHOLD_ENTRIES`.

**Expect**: `curl localhost:8090/readyz` returns `503` with `"status": "degraded"`, naming the agent, its source, and its lag; `/livez` still returns `200` — the process is alive, it is just behind. `conflux_agent_lag_entries` matches what `XINFO GROUPS ingestion-events` reports.

**The trimming case matters more than the threshold case.** Seed beyond the 10k local cap while the agent is stopped so entries are trimmed from under the group. `XINFO GROUPS` then reports `NULL` lag; expect `conflux_agent_lag_approximate` at 1, readiness degraded, and an `entries_trimmed` anomaly — a reading of zero there would invert the signal (D11).

## Scenario 5 — Housekeeping is non-destructive (US4, SC-012)

Snapshot the queue, run the reclaim and summary agents, compare:

```bash
redis-cli -a pulse-local-not-a-secret --scan > /tmp/keys-before.txt
redis-cli -a pulse-local-not-a-secret XLEN ingestion-events
# enable pending_reclaim + log_summary in CONFLUX_AGENTS, run one cycle
redis-cli -a pulse-local-not-a-secret --scan > /tmp/keys-after.txt
diff /tmp/keys-before.txt /tmp/keys-after.txt   # expect no difference
```

**Expect**: `XLEN` unchanged, every key outside this service's own consumer groups unchanged (FR-020a), entries left pending by a dead consumer reclaimed and processed, and a live consumer's in-flight entries untouched — that last one is what `PENDING_MIN_IDLE` protects.

To stage abandoned entries: read with `XREADGROUP` under a consumer name, do not ack, kill the reader, wait out `PENDING_MIN_IDLE`, then run the reclaim agent.

**Log drain and summaries**: with the log agent running, push entries into `ingestion-logs` under sustained volume and watch `LLEN` stay below `SUMMARY_MAX_LIST_DEPTH` — the drain is continuous, not on the summary interval, because the gateway's script **refuses** log writes at the 10k cap rather than trimming to make room (FR-018c). Stop the agent and keep pushing to see `LOGS_LIST_FULL` come back: that is upstream loss this service caused.

Expect one summary per closed `SUMMARY_PERIOD`, carrying its period boundaries as identity; re-sending one leaves the destination unchanged (FR-018a). Restart the service mid-period and confirm that period's summary is emitted with `partial` set — the in-memory counts before the restart are gone and no datastore holds them (FR-018b). Do **not** expect to verify a summary by recomputing it: the list is read destructively, so a period cannot be read twice.

---

## Tests

```bash
go test ./...                                    # unit, no infrastructure needed
go test -tags=integration ./test/integration/... # needs pulse-infra `full` running
```

The integration tag is deliberate: `go test ./...` must stay fast and green without Docker, while the tagged suite exercises the real quirks a fake Redis gets wrong — single-field entries, `NULL` group lag, the Lua-dropped log write (Principle V).

## Teardown

```bash
cd ../pulse-infra && make down     # or `make reset` to clear data as well
```
