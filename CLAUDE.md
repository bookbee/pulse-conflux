# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Scaffold only. Tracked files are `README.md` and `.gitignore`; `.env.example`, `CLAUDE.md` and `.specify/` are untracked. There is **no source, no package manifest, and therefore no build/test/lint command yet** — do not invent one. When the first manifest lands, replace this section with the real build/test/lint invocations, including how to run a single test.

The `.gitignore` is the stock GitHub Node template. It signals JS/TS but is NOT evidence of a chosen runtime, framework, or package manager — treat those as undecided.

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
