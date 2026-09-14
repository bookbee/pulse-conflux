# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build, test, lint

**Go 1.25** (`go.mod` declares `go 1.25`, deliberately not `pulse-gateway`'s `1.26.2` — matching it would make every local build download a toolchain; see `specs/001-event-agent-runtime/research.md` D2).

```bash
make build                      # go build -o bin/conflux ./cmd/conflux
make run                        # go run ./cmd/conflux  (reads .env)
make test                       # unit tests — MUST stay green with no Docker running
make test-integration           # needs pulse-infra `full`: (cd ../pulse-infra && make up)
make test-one NAME=TestFoo      # single test; add PKG=./internal/envelope/... to narrow
make lint                       # golangci-lint (skips with a notice if not installed)
make fmt                        # go fmt + go vet
```

Integration tests sit behind a `//go:build integration` tag so the default suite needs no
infrastructure. The tagged suite runs against the real local stack, never a fake Redis —
single-field stream entries, `NULL` group lag and the Lua-dropped log write are exactly what
a mock reproduces incorrectly.

The `.gitignore` is the stock GitHub Node template with a Go section appended. The Node half
is historical and is NOT evidence of a JS/TS surface.

## Layout

`cmd/conflux` wires; `internal/ingest` is the ingestion boundary and **the only package
permitted to import `go-redis`** — `test/integration/boundary_test.go` fails the build if that
is violated. `internal/agent` hosts the runtime, `internal/dispatch` the destinations and retry
policy, `internal/housekeeping` the periodic agents, `internal/observability` logging, metrics
and the operational endpoints.

## What this service is

pulse-conflux aggregates events from **two sources with different delivery semantics** (Redis and Kafka) into one pipeline, then persists and forwards them. Keep source-specific behavior — consumption, offset/ack handling, redelivery — isolated behind the ingestion boundary rather than leaking into persistence or forwarding. The JSON envelope is identical across every source and both Redis primitives, so parsing is shared; consumption and acknowledgement are not.

## Cross-repo contract

This repo is one of five in `../` (`pulse-gateway`, `pulse-ingestor`, `pulse-client`, `pulse-infra`). **The authority for every key name, topic, port, and Redis structure is `../pulse-infra/docs/stack-contract.md`** — read it before changing a name in `.env.example`, since anything on that page is a breaking change for four-plus repos. `../pulse-infra/docs/divergences.md` records what the local stack does *not* exercise.

Local stack: `cd ../pulse-infra && make up`. Redis only exists in the `full` profile. `make ps`, `make health`, `make logs`, `make reset` (required between profile switches — `lite` and `core`/`full` both bind host port 19092 and must not run together).

## Source semantics that shape the design

**Redis** (host `localhost:6379`, in-network `redis:6379`, auth + db 0 in `.env.example`):

- Two different primitives, not one. Events and signals are **streams** (`XADD`); logs are a **list** (Lua `RPUSH` + `EXPIRE`).
- Stream entries have **exactly one field, `data`**, holding the JSON envelope — not one Redis field per envelope key.
- The gateway only `XADD`s and never creates a consumer group. If this service uses `XREADGROUP` it owns `XGROUP CREATE` (with `MKSTREAM`), pending-entry handling, and claims.
- Envelope: `event_id`, `gateway_id`, `received_at` (UTC RFC3339), `retry_count`, `stream_name`, `payload`, and `event_header` — present **only** for JWT-authenticated gateway requests, so treat it as optional.

**Kafka** (host `localhost:19092,localhost:19093,localhost:19094`; in-network `kafka-N:9092`): topics `ingestion-events`, `ingestion-signals`, `ingestion-logs` — names deliberately mirror the Redis keys. 6 partitions each; auto-topic-creation is **off**, so a misspelled topic errors rather than silently staying empty. Consumer groups are the consumer's business here too.

Two things to know before writing the Kafka path: `.env.example` currently has **no Kafka keys** (add them there), and per `divergences.md` the gateway has **no Kafka producer at this commit** — nothing in the local stack feeds those topics except `pulse-ingestor`'s own tests and `pulse-client`.

**Backpressure: there is none.** Both Redis destinations shed data under lag — streams trim to an approximate `MAXLEN`, and the log list's Lua script drops the write outright once capped. Neither pushes back on the gateway, so falling behind means data loss upstream, not a slower producer. **Consumer lag is the metric that matters.** Local caps are 10k (vs 100k in the gateway's example config) so the lossy path shows up in development; the absolute numbers mean nothing for capacity planning.

## Conventions

- Add every new configuration key to `.env.example` with a comment on what it contracts with (`.env` and `.env.*` are gitignored).
- External forwarding targets are out of local scope: stub them and assert on the calls rather than reaching real systems from a laptop (`FORWARD_ENABLED=false`).
- Local values in `.env.example` are not secrets, but they target the `pulse-infra` stack only — in-network consumers use the container names, not `localhost`.

## Spec-driven workflow

`.specify/` holds a spec-kit setup (v1.0.6) with the `speckit-*` skills installed in `.claude/skills/`. `.specify/memory/constitution.md` is still the **unfilled template** — run `/speckit-constitution` to populate it before leaning on `/speckit-plan` or `/speckit-analyze` output.
