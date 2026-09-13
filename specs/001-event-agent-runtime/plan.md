# Implementation Plan: Event Agent Runtime

**Branch**: none created — spec directory `001-event-agent-runtime`; work is on `main` | **Date**: 2026-09-14 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-event-agent-runtime/spec.md`

## Summary

Build a Go service that hosts several independently configured agents in one process. Each agent either listens continuously to a gateway-written Redis source or runs on a fixed interval; it receives a parsed envelope with no knowledge of where it came from, and hands the result to a destination that is stubbed in this feature (LTR, CEP, support email). Redis specifics — consumer groups, pending-entry reclaim, claims, the list's destructive read — stay behind one ingestion boundary. Dispatch uses a bounded retry budget and then records a drop, so an unavailable destination never becomes the reason the service falls behind. Lag is measured per agent and per source and drives both health reporting and the anomaly path.

## Technical Context

**Language/Version**: Go 1.25.1 (locally installed). The `go` directive will be `1.25`, deliberately not `pulse-gateway`'s `1.26.2` — see research.md.

**Primary Dependencies**: `github.com/redis/go-redis/v9` (same version family as the gateway), `github.com/prometheus/client_golang`, `github.com/joho/godotenv`, stdlib `log/slog` and `net/http`. No agent framework, no scheduler library, no ORM.

**Storage**: None. FR-013 makes this feature dispatch-only; the sole durable state is the consumer-group position held in Redis, which the service owns but does not treat as a datastore.

**Testing**: stdlib `testing` for unit tests co-located with packages; integration tests behind a `//go:build integration` tag in `test/integration/`, run against the `pulse-infra` `full` profile.

**Target Platform**: Linux container in deployment, macOS/arm64 for local development. One long-running process.

**Project Type**: Single Go service (backend worker). No UI, no public API surface beyond operational endpoints.

**Performance Goals**: Keep up with the gateway at local-stack volume without accumulating lag. SC-002's bar is 10,000 events through an intermittently failing destination with every event accounted for; the binding constraint is lag, not throughput.

**Constraints**: Redis stream caps are 10k locally with approximate trimming plus age-based trimming, so an agent that stalls loses data upstream irrecoverably (no datastore, FR-013). A destination must never be able to stall an agent: per-attempt timeout plus bounded attempts, then a recorded drop (FR-017/017a).

**Scale/Scope**: Three streams and one list in the platform contract; four agents in this feature (events→LTR, signals→CEP, log summarization, pending reclaim). Agent count is configuration, not code.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Evaluated against `.specify/memory/constitution.md` v1.0.0.

| Principle | Gate | Pre-research | Post-design |
|---|---|---|---|
| I. Cross-Repo Contract Fidelity | No source/key/topic/field name hardcoded; envelope mirrored from `stack-contract.md`; every new key lands in `.env.example` in the same change | PASS | PASS — envelope fields mirror `model.EnrichedPayload` field-for-field (data-model.md); all new keys enumerated in contracts/configuration.md |
| II. Source semantics behind the ingestion boundary | Envelope parsing shared; consumption, acks, group lifecycle, pending recovery, claims invisible to agents | PASS | PASS — `internal/ingest` is the only package importing `go-redis`; agents receive `envelope.Envelope` + an opaque `Ack()`/`Nack()` |
| III. Assume redelivery, prove idempotency | Group creation owned here; every dispatch idempotent on `event_id`; `event_header` optional | PASS | PASS — `XGROUP CREATE … MKSTREAM` on start; dispatch carries `event_id` as the destination-side idempotency key; `EventHeader` is `json.RawMessage` with `omitempty` semantics |
| IV. Lag is the health signal | Lag measured per agent and source; health reflects lag; every envelope log line carries `event_id` and `gateway_id` | PASS | PASS — `conflux_agent_lag_entries` gauge, `/readyz` fails on threshold breach, `slog` base attrs enforced at the boundary |
| V. Real stack in, stubs out | Integration tests against `pulse-infra`; destinations stubbed; nothing scaffolded that has no upstream producer | PASS | PASS — `//go:build integration` against the `full` profile; all three destinations are stub implementations; no Kafka package exists in the layout |

**Result**: no violations. Complexity Tracking section omitted — nothing to justify.

One gate deserves a note rather than a silent pass: Principle V forbids scaffolding behavior that does not exist upstream, and this plan creates no `internal/ingest/kafka` package, not even an empty one. The `Source` interface is what makes Kafka additive later; an unused package would be the speculative scaffold the principle prohibits.

## Project Structure

### Documentation (this feature)

```text
specs/001-event-agent-runtime/
├── plan.md              # This file
├── spec.md              # Feature specification
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── ingestion-source.md
│   ├── destination-dispatch.md
│   ├── operational-endpoints.md
│   └── configuration.md
├── checklists/
│   └── requirements.md
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

```text
cmd/
└── conflux/
    └── main.go                  # wiring only: config → registry → agents → signals

internal/
├── config/                      # env loading; every key mirrored in .env.example
├── envelope/                    # shared JSON envelope: parse, validate, identity
├── ingest/                      # THE ingestion boundary — nothing below leaks upward
│   ├── source.go                # Source, Delivery, Ack/Nack interfaces
│   └── redis/
│       ├── client.go            # connection, auth, db selection
│       ├── stream.go            # XGROUP CREATE MKSTREAM, XREADGROUP, XACK
│       ├── pending.go           # XPENDING, XAUTOCLAIM, consumer retirement
│       └── list.go              # destructive read of the logs list
├── agent/                       # runtime: registry, lifecycle, isolation, triggers
│   ├── registry.go              # named agents from config; enable/disable
│   ├── runner.go                # panic isolation, shutdown, no-overlap guard
│   ├── stream.go                # continuous trigger
│   └── periodic.go              # interval trigger
├── dispatch/                    # destinations + retry budget + recorded drops
│   ├── destination.go           # Destination interface
│   ├── retry.go                 # attempt budget, backoff, per-attempt timeout
│   └── stub/                    # ltr.go, cep.go, email.go — all stubbed here
├── housekeeping/                # summarize.go, reclaim.go, report.go
└── observability/               # slog setup, metrics, /livez /readyz /metrics

test/
└── integration/                 # //go:build integration — needs pulse-infra `full`
```

Unit tests are co-located as `*_test.go` beside the package they cover, per Go convention; only cross-package integration tests live under `test/`.

**Structure Decision**: Single Go module, standard `cmd/` + `internal/` layout. The directory boundary *is* the Principle II boundary: `internal/ingest` is the only package permitted to import `go-redis`, and an import-check test enforces it rather than leaving it to review. `internal/agent` depends on `internal/envelope` and `internal/dispatch` but never on `internal/ingest/redis`.

## Phase 0 — Research

See [research.md](./research.md). Ten decisions recorded, each with rationale and rejected alternatives. The language question was settled by the user in-session (Go); the remaining nine were resolved from platform evidence and the constitution.

## Phase 1 — Design & Contracts

- [data-model.md](./data-model.md) — the envelope mirrored from the gateway's `model.EnrichedPayload`, plus the runtime's own entities (Agent, Delivery, DispatchOutcome, LagReading, Anomaly, LogSummary) and their validation rules and state transitions.
- [contracts/ingestion-source.md](./contracts/ingestion-source.md) — the consumed contract: what the gateway writes and what this service may assume.
- [contracts/destination-dispatch.md](./contracts/destination-dispatch.md) — the outbound envelope shape and the `Destination` interface every stub implements.
- [contracts/operational-endpoints.md](./contracts/operational-endpoints.md) — `/livez`, `/readyz`, `/metrics` and the metric names.
- [contracts/configuration.md](./contracts/configuration.md) — every new environment key, its default, and what it contracts with.
- [quickstart.md](./quickstart.md) — bring up the stack, seed envelopes, watch an agent drain them, force a drop, prove the housekeeping agent is non-destructive.
