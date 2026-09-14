# Phase 0 Research: Event Agent Runtime

**Date**: 2026-09-14 | **Plan**: [plan.md](./plan.md)

Thirteen decisions. Evidence came from the sibling repos (`pulse-gateway` source and `go.mod`, `pulse-ingestor`, `pulse-client`), `../pulse-infra/docs/stack-contract.md` and `divergences.md`, and the toolchains actually installed on this machine.

---

## D1 — Language and runtime: Go

**Decision**: Go, targeting the locally installed 1.25.1.

**Rationale**: Settled by the user in session. The reasons that made it the recommendation: goroutines make FR-004 (one agent's failure never stalls another) a language primitive rather than a construction; `go-redis` covers the consumer-group surface FR-009 demands (`XGROUP CREATE … MKSTREAM`, `XREADGROUP`, `XPENDING`, `XAUTOCLAIM`) as first-class API; the envelope can be mirrored field-for-field from `pulse-gateway`'s `internal/model/enriched_payload.go` instead of being retyped from prose; and the toolchain is already on this machine, so integration tests against the local stack need no container indirection.

**Alternatives considered**: *TypeScript/Node 20* — what this repo's stock Node `.gitignore` hints at, and `ioredis` is capable, but the single event loop turns agent isolation and CPU-bound log summarization into `worker_threads` work. *Python 3.11* — quickest to write, `redis-py` supports groups, same event-loop and GIL caveats. *Rust* — would match `pulse-ingestor`'s decision of 2026-09-13 and give two of five repos one stack, but there is no Rust toolchain on this machine, so every build and test would run in a container, against a Principle V workflow that wants the local stack close at hand.

**Consequence**: `CLAUDE.md` and the constitution both currently record the runtime as UNDECIDED. Both must be updated in the change that lands `go.mod` — the constitution's Technology Constraints section requires it explicitly.

---

## D2 — `go` directive: 1.25, not the gateway's 1.26.2

**Decision**: `go 1.25` in `go.mod`. No `toolchain` line.

**Rationale**: The installed toolchain is 1.25.1. Declaring `go 1.26.2` like `pulse-gateway` would make every local build download a toolchain, and `pulse-gateway`'s own `CLAUDE.md` already records friction from that pin (`-race` needing an explicit toolchain when the base install is older). Nothing in this feature needs a 1.26 language feature. Go's compatibility promise means a 1.25 module builds fine under a newer toolchain in CI or in a container.

**Alternatives considered**: Match 1.26.2 for platform uniformity — rejected: uniformity of a version number is not worth making the default local loop depend on a download. Omit the directive — not valid for a module that wants reproducible builds.

---

## D3 — Redis client: `github.com/redis/go-redis/v9`

**Decision**: `go-redis/v9`, tracking the same major/minor family as the gateway (`v9.19.0`).

**Rationale**: The gateway already depends on it, so the producer and consumer sides of the same queue share one client's semantics — notably how `XAdd`'s `Values: []string{"data", payloadJSON}` round-trips, which is the single-field entry shape the stack contract fixes. It covers the full group API this feature needs, is context-aware throughout, and handles auth plus DB selection from the existing `REDIS_*` keys.

**Alternatives considered**: *rueidis* — faster and client-side-caching capable, but the caching is irrelevant to a streaming consumer and it would introduce a second Redis dialect into the platform. *radix* — smaller, but thinner group support, which is exactly the part this service owns.

---

## D4 — One `Source` interface as the ingestion boundary

**Decision**: `internal/ingest` defines `Source`, `Delivery`, and acknowledgement; `internal/ingest/redis` is the only implementation and the only package in the module permitted to import `go-redis`. Agents consume `envelope.Envelope` plus an opaque `Ack()`/`Nack()`.

**Rationale**: Constitution Principle II in a directory. It also makes the Kafka feature additive: a second `Source` implementation, no change above the line. The enforcement is a test that walks the import graph and fails if `go-redis` appears outside `internal/ingest/...`, because a boundary defended only by code review erodes.

**Alternatives considered**: A generic `Queue` abstraction covering both produce and consume — rejected, this service never produces. Passing raw `redis.XMessage` to agents with a "please don't depend on it" comment — rejected outright; that is the leak the principle names.

---

## D5 — Two consumption modes behind that boundary

**Decision**: Streams (`ingestion-events`, `ingestion-signals`) are read with `XREADGROUP` under a consumer group this service creates; the logs list (`ingestion-logs`) is read destructively with a blocking pop. Both produce the same `Delivery` type upward.

**Rationale**: The two Redis primitives are not interchangeable and the contract says so: streams are non-destructive and support groups, so several agents can each see every event independently (US2 scenario 1); a list read removes the entry, so exactly one consumer can have it. Ack on a stream delivery is `XACK`; ack on a list delivery is a no-op because the pop already consumed it — and that asymmetry is precisely what must not escape upward.

**Alternatives considered**: Reading the list non-destructively with `LRANGE` plus an index — rejected: no atomic claim, two instances would double-process, and the gateway's Lua script trims the list under you. Treating the list as a stream — impossible, different primitive.

---

## D6 — Pending recovery via `XAUTOCLAIM`

**Decision**: The reclaim housekeeping agent uses `XAUTOCLAIM` with a configured minimum idle time, and retires consumer names that hold no pending entries and have been idle beyond a threshold.

**Rationale**: `XAUTOCLAIM` (Redis 6.2+; the stack runs 7.4.11) does the scan-and-claim in one atomic server-side step and returns a cursor, so reclaim is paginated and bounded. The minimum-idle-time argument is what protects a live consumer's in-flight work — US4 scenario 1 and FR-019 both turn on it.

**Alternatives considered**: `XPENDING` to list then `XCLAIM` per entry — two round-trips per entry and a race between them. Never reclaiming and relying on restarts — a crashed consumer's entries would sit pending forever, which is the failure this agent exists for.

---

## D7 — Scheduling: `time.Ticker` with a no-overlap guard

**Decision**: Periodic agents run on a `time.Ticker` at a configured `time.Duration`, with a guard that skips a tick when the previous run is still in flight and counts the skip.

**Rationale**: FR-002 says "configured interval", not "cron expression". A ticker plus a guard is about thirty lines against a dependency that brings a parser, a timezone model, and a scheduler goroutine. FR-005's no-overlap rule is a `sync/atomic` flag, and counting skips makes an overrunning agent visible instead of silent.

**Alternatives considered**: `robfig/cron` — correct choice the day someone needs "03:00 on weekdays"; adding it now is speculative. Sleeping in a loop — drifts.

---

## D8 — Retry: hand-rolled exponential backoff with jitter

**Decision**: A small `internal/dispatch/retry.go`: bounded attempts, exponential backoff with full jitter between them, a per-attempt context timeout, then a recorded drop.

**Rationale**: FR-017/017a/017b describe the whole policy, and it is a loop, a duration, and a random number. The per-attempt timeout is the part that matters most — it is what makes a hanging destination consume its budget rather than pin an agent forever — and that is a `context.WithTimeout`, not a library feature. Jitter prevents two agents that fail together from retrying in lockstep.

**Alternatives considered**: `cenkalti/backoff` — well-made, but it is a dependency for control flow the team must understand precisely anyway, since bounded loss depends on it.

---

## D9 — Configuration: environment only, prefix-keyed per agent

**Decision**: All configuration from environment variables, loaded with `github.com/joho/godotenv` (as the gateway does). `CONFLUX_AGENTS` lists agent ids; each agent's settings are read from `AGENT_<ID>_*` keys. No YAML, no agent manifest file.

**Rationale**: The constitution makes `.env.example` the configuration contract and requires every new key to land there with a comment naming what it contracts with. One surface keeps that enforceable; a second file format would create a second contract with no such rule attached. `godotenv` matches the gateway so local runs behave the same way in both repos.

**Alternatives considered**: A YAML agent manifest — more expressive for nested per-agent destinations, and worth revisiting when agents outgrow flat keys, but it splits the contract. A flag-based CLI — wrong shape for a containerised long-running worker.

**Consequence**: agent ids are uppercased into key names, so they must be `[A-Z0-9_]` after normalisation. `contracts/configuration.md` states the rule.

---

## D10 — Observability: `log/slog` plus `prometheus/client_golang`

**Decision**: Structured JSON logging through stdlib `log/slog`; metrics via `prometheus/client_golang`; `/livez`, `/readyz`, `/metrics` served by stdlib `net/http` on a dedicated port.

**Rationale**: `slog` is stdlib and already the gateway's choice (`slog.Error("redis stream key required", "component", "redis")`), so log shapes match across the platform. FR-024 requires `event_id` and `gateway_id` on every envelope log line; the ingestion boundary attaches them as base attributes to a per-delivery logger, which is the only way to make "every line" true rather than aspirational. Prometheus matches the gateway's `/metrics` surface, so one scrape config covers both.

**Alternatives considered**: OpenTelemetry — the right answer when the platform adopts tracing; adopting it here alone would make conflux the only repo with a collector dependency. `zap`/`zerolog` — faster, but a third logging dialect in a five-repo platform. Fiber for the endpoints — the gateway uses it for a real API; three operational handlers do not need a framework.

---

## D11 — Lag measurement: `XINFO GROUPS`, plus an explicit trim check

**Decision**: Per-stream lag comes from the `lag` field of `XINFO GROUPS`. Separately, and on every reading, compare the group's `last-delivered-id` against the stream's first surviving entry: if the group's position is older, entries were trimmed away before it read them, and the reading is marked approximate. List lag is `LLEN`.

**Corrected 2026-09-14 against Redis 7.4 on the local stack.** This decision originally said NULL lag was the trim signal. Testing showed that is wrong in both directions, and the correction matters because the original design would have reported a healthy-looking number over real data loss:

- **`MAXLEN` trimming does NOT make lag NULL.** Redis keeps computing it. Trimming a 5-entry stream to 2 with a group still at `0-0` reports `lag 2` — a confident number that silently omits the 3 entries destroyed unread.
- **What does make lag NULL is `XDEL` tombstones**, which the gateway never creates but an operator might. That path is still handled, and still falls back to `XLEN` marked approximate.
- **The condition the spec actually cares about** — "the stream is trimmed while an agent is behind" — is invisible in `lag` and needs the position comparison above.

**Rationale**: FR-022 makes lag first-class, and the server computes the ordinary case well. The trim case is not an edge case here: local caps are 10k with approximate *and* age-based trimming, so a lagging group WILL have entries trimmed from under it. A reading that trusts `lag` alone there would report "slightly behind" during active data loss, which is worse than no signal at all — it actively misleads. The approximate flag propagates into `/readyz` and raises an `entries_trimmed` anomaly.

**Alternatives considered**: Tracking last-processed ID and diffing against `XLEN` ourselves — duplicates server state and drifts. Time-based lag from `received_at` — useful and cheap to add later as a second gauge, but it measures a different thing and depends on clock agreement between gateway and consumer.

## D12 — Agent isolation: goroutine per agent, panic recovery in the runner

**Decision**: Each agent runs in its own goroutine supervised by `agent.Runner`, which recovers panics, records them against the agent's name, applies a restart backoff, and leaves every other agent untouched. A shared `context.Context` handles shutdown.

**Rationale**: FR-004 and US2 scenario 3. An unrecovered panic in one goroutine takes the whole process down, which would violate the requirement in the most direct way possible; recovery at the runner is the boundary where the agent's identity is still known, so the failure can be attributed (FR-004 says "attribute the failure to the named agent").

**Alternatives considered**: One process per agent — genuinely better isolation, and the configuration model deliberately does not prevent it later, but it multiplies deployment units on day one. Letting panics crash the process under a supervisor — restarts every other agent for one agent's bug, and loses in-flight acks.

---

## D13 — Testing: co-located unit tests, build-tagged integration against `pulse-infra`

**Decision**: Unit tests as `*_test.go` beside their package. Integration tests in `test/integration/` behind `//go:build integration`, run against the `pulse-infra` `full` profile with `go test -tags=integration ./test/integration/...`. Destination stubs are `httptest` servers asserted on.

**Rationale**: Constitution Principle V requires the real stack and forbids mocks that "reproduce the sources' quirks incorrectly" — single-field entries, Lua-dropped log writes, and `NULL` group lag are precisely what a fake Redis gets wrong. The build tag keeps `go test ./...` fast and green without infrastructure, which is what a pre-commit loop needs.

**Alternatives considered**: `testcontainers-go` — real Redis, but its own container, not the pulse-infra stack the principle names, and it would not carry the deliberately low local caps that make lossy behavior visible. `miniredis` — an in-process fake; rejected for exactly the quirks above.
