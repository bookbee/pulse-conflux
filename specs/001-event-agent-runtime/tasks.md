---

description: "Task list template for feature implementation"
---

# Tasks: Event Agent Runtime

**Input**: Design documents from `/specs/001-event-agent-runtime/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Test tasks ARE included. They are not optional here — Constitution Principle V requires integration tests against the `pulse-infra` local stack, the Development Workflow section requires each new ingestion path to arrive with one, and SC-010 requires every dispatch in the suite to land on an asserted stub.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

Single Go module at repository root: `cmd/conflux/`, `internal/`, `test/integration/`. Unit tests are co-located as `*_test.go` beside their package (Go convention); only cross-package integration tests live under `test/`. Structure per [plan.md](./plan.md#project-structure).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Turn the scaffold into a Go module. This phase is also where the repo's "runtime undecided" documentation stops being true.

- [ ] T001 Create `go.mod` at repository root with module path `in.dmart.pulse.conflux` and directive `go 1.25` (NOT `1.26.2` — see research.md D2; no `toolchain` line)
- [ ] T002 Add pinned dependencies to `go.mod`: `github.com/redis/go-redis/v9 v9.19.0`, `github.com/prometheus/client_golang`, `github.com/joho/godotenv` — matching pulse-gateway's versions per research.md D3/D10
- [ ] T003 Create the directory skeleton from plan.md: `cmd/conflux/`, `internal/{config,envelope,ingest,agent,dispatch,housekeeping,observability}/`, `test/integration/`
- [ ] T004 [P] Extend `.gitignore` with Go artifacts (compiled binaries, `vendor/`, coverage output) — the file is currently the stock Node template, and the constitution's Technology Constraints require `.gitignore` additions in the same change as a new language surface
- [ ] T005 [P] Create `Makefile` with `build`, `run`, `test`, `test-integration`, `lint`, `fmt` targets; `test-integration` must pass `-tags=integration` and state the `pulse-infra` `full` profile prerequisite
- [ ] T006 [P] Create `.golangci.yml` enabling at minimum `govet`, `errcheck`, `staticcheck`, `ineffassign`, `misspell`
- [ ] T007 Replace the "Status" section of `CLAUDE.md` with the real build/lint/test commands including how to run a single test (`go test -run TestName ./internal/...`), and update `.specify/memory/constitution.md`'s Technology & Configuration Constraints section where it says the runtime is UNDECIDED — research.md D1 records this as required in the same change as the manifest

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The ingestion boundary, the envelope, config, and the agent runtime skeleton. Every user story depends on all of it.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T008 [P] Implement the `Envelope` struct in `internal/envelope/envelope.go`, mirroring pulse-gateway's `internal/model/enriched_payload.go` field-for-field: `event_id`, `gateway_id`, `received_at`, `retry_count`, `stream_name` (`omitempty`), `event_header` (`json.RawMessage`, `omitempty`), `payload` (`json.RawMessage`). Do NOT represent the gateway's `json:"-"` fields
- [ ] T009 Implement envelope parsing and validation in `internal/envelope/parse.go`: "`event_id`, `gateway_id`, `payload` must be present and non-empty"; "`event_header` absent is valid and must not be treated as an error"; hold `payload` and `event_header` as raw JSON and forward byte-for-byte — "Re-marshalling risks key reordering and number-format drift, which would break a destination-side idempotency check"
- [ ] T010 [P] Write unit tests in `internal/envelope/parse_test.go` covering: valid envelope, missing `event_id`, missing `payload`, absent `event_header` (must pass), malformed JSON, and byte-for-byte payload preservation
- [ ] T011 Define `Source`, `Delivery`, and acknowledgement in `internal/ingest/source.go` per data-model.md: fields `Envelope`, `Source (SourceRef)`, `Position (string, opaque above the boundary)`, `Raw ([]byte)`, `Ack func() error`, `Nack func() error`. Document that "A list delivery cannot be un-consumed, and that fact must not escape upward into agent code"
- [ ] T012 [P] Define `SourceRef` in `internal/ingest/source.go` with `Name string` (from configuration, never a literal) and `Kind` of `stream` or `list`
- [ ] T013 Implement the Redis client in `internal/ingest/redis/client.go`: connection, auth, DB selection from existing `REDIS_ADDR`/`REDIS_PASSWORD`/`REDIS_DB` keys, context-aware, with connect backoff that never spins hot
- [ ] T014 [P] Implement config loading in `internal/config/config.go` using `godotenv`: runtime keys (`HTTP_ADDR`, `SHUTDOWN_GRACE`) and the agent registry keys (`CONFLUX_AGENTS`, `AGENT_<ID>_*`) per contracts/configuration.md. Agent ids uppercase with non-alphanumerics to `_`; ids must be unique after normalisation
- [ ] T015 [P] Implement config validation in `internal/config/validate.go`: "A missing required key for an **enabled** agent is a startup failure naming the key. A missing key for a disabled agent is ignored"
- [ ] T016 [P] Write unit tests in `internal/config/config_test.go` for id normalisation, missing-required-key failure naming the key, and the disabled-agent exemption
- [ ] T017 Add every new key from contracts/configuration.md to `.env.example` with a comment naming what it contracts with (Constitution Principle I requires this in the same change as the key)
- [ ] T018 [P] Configure structured logging in `internal/observability/logging.go`: `log/slog` JSON handler, level from `LOG_LEVEL`, standard attributes `agent`, `source`, `component`
- [ ] T019 [P] Create the Prometheus registry and metric definitions in `internal/observability/metrics.go` for all 14 metrics in contracts/operational-endpoints.md
- [ ] T020 Implement the agent registry in `internal/agent/registry.go`: build named agents from config, skip disabled ones, reject duplicate normalised ids
- [ ] T021 Implement `agent.Runner` in `internal/agent/runner.go`: one goroutine per agent, panic recovery that records against the agent's name and increments `conflux_agent_panics_total`, restart backoff, and the `configured → running → draining → stopped` lifecycle from data-model.md. Draining "must not acknowledge work that did not complete"
- [ ] T022 Define the `Destination` interface in `internal/dispatch/destination.go` exactly as contracts/destination-dispatch.md specifies: `Name() string`, `Dispatch(ctx, env) error`; document that implementations "MUST be safe for concurrent use and MUST NOT retry internally"
- [ ] T023 Implement `cmd/conflux/main.go`: wiring only — config → logging → metrics → registry → runner → signal handling with `SHUTDOWN_GRACE` graceful drain
- [ ] T024 Write the import-boundary test in `test/integration/boundary_test.go` (no build tag — it must run in the default suite): walk the module's import graph and fail if `github.com/redis/go-redis` is imported anywhere outside `internal/ingest/...`. This enforces Constitution Principle II mechanically rather than by review (research.md D4)

**Checkpoint**: Foundation ready — the service starts, serves nothing, processes nothing, and the boundary test passes

---

## Phase 3: User Story 1 - Stream-listening agent dispatches events to a destination (Priority: P1) 🎯 MVP

**Goal**: One agent listens to a stream continuously, parses the envelope, and dispatches to a stubbed destination with a bounded retry budget.

**Independent Test**: Bring up the local stack, seed three envelopes into `ingestion-events`, and confirm three dispatches arrived at the stub with envelope fields intact, the group auto-created, and the stream position advanced past all three.

### Tests for User Story 1 ⚠️

> **Write these FIRST and confirm they fail before implementing**

- [ ] T025 [P] [US1] Contract test in `internal/dispatch/destination_test.go`: an `httptest` stub asserts `Idempotency-Key: <event_id>`, `X-Conflux-Agent`, `X-Conflux-Attempt`, byte-for-byte `payload`, and that `event_header` is "omitted entirely when absent — never sent as `null`"
- [ ] T026 [P] [US1] Unit test in `internal/dispatch/retry_test.go`: the HTTP outcome mapping from contracts/destination-dispatch.md — 2xx delivered; 408/429/5xx retried within budget; "4xx other than 408/429" dropped immediately without retry; timeout retried
- [ ] T027 [P] [US1] Integration test in `test/integration/stream_dispatch_test.go` (`//go:build integration`): quickstart Scenario 1 — seed three envelopes, assert three stub calls, `conflux_events_processed_total` at 3, and `XPENDING` empty after ack
- [ ] T028 [P] [US1] Integration test in `test/integration/stream_dispatch_test.go`: seed an envelope with NO `event_header` (an API-key gateway request) and assert it dispatches normally rather than erroring (FR-010)
- [ ] T029 [P] [US1] Integration test in `test/integration/restart_resume_test.go`: quickstart Scenario 2 — stop mid-stream, restart, assert resumption from the last acknowledged position with no skipped `event_id` and that redelivery is absorbed idempotently

### Implementation for User Story 1

- [ ] T030 [US1] Implement stream consumption in `internal/ingest/redis/stream.go`: `XGROUP CREATE <key> <group> $ MKSTREAM` on start treating `BUSYGROUP` as benign, then `XREADGROUP` with `COUNT`/`BLOCK` from `AGENT_<ID>_BATCH_SIZE` and `AGENT_<ID>_BLOCK_TIMEOUT`
- [ ] T031 [US1] Read the entry's `data` field **by name** in `internal/ingest/redis/stream.go` — "A stream entry has exactly one field, named `data`" and "a consumer that iterates fields generically will work by accident today and break on the first added field"
- [ ] T032 [US1] Implement stream `Ack` as `XACK <key> <group> <id>` and `Nack` as leave-pending in `internal/ingest/redis/stream.go`
- [ ] T033 [US1] Implement the continuous trigger in `internal/agent/stream.go`: loop reading from the `Source`, hand each `Delivery` to the agent's dispatch chain, ack only on resolution
- [ ] T034 [US1] Implement the retry policy in `internal/dispatch/retry.go`: `DISPATCH_MAX_ATTEMPTS` attempts, exponential backoff with full jitter between `DISPATCH_BACKOFF_INITIAL` and `DISPATCH_BACKOFF_MAX`, and a per-attempt `context.WithTimeout` of `DISPATCH_TIMEOUT` — "per attempt, not total"
- [ ] T035 [US1] Implement the HTTP outcome mapping in `internal/dispatch/retry.go` per contracts/destination-dispatch.md, including the permanent-failure short-circuit ("retrying a rejected payload only burns the budget")
- [ ] T036 [P] [US1] Implement the LTR stub destination in `internal/dispatch/stub/ltr.go` posting to `DEST_LTR_URL` with the headers and body from contracts/destination-dispatch.md
- [ ] T037 [US1] Implement `DispatchOutcome` recording in `internal/dispatch/outcome.go`: `delivered` or `dropped` with no third state, plus `Attempts`, `Reason`, `Duration` per data-model.md
- [ ] T038 [US1] Emit a `dropped` outcome at error level with full identity in `internal/dispatch/outcome.go` — "this record is the only trace the event existed" and "must never be reduced to a counter alone" — and increment `conflux_dispatch_dropped_total`
- [ ] T039 [US1] Attach `event_id` and `gateway_id` as base slog attributes at the ingestion boundary in `internal/ingest/redis/stream.go`, so FR-024's "every line" is enforced structurally rather than per call site
- [ ] T040 [US1] Handle unprocessable entries in `internal/agent/stream.go`: do not block remaining entries, record the entry in full with its source and position, increment `conflux_events_unprocessable_total` — "Since no datastore exists (FR-013), that record is the only trace — it MUST NOT be reduced to a counter"
- [ ] T041 [US1] Wire `conflux_events_processed_total` in `internal/agent/stream.go`, and `conflux_dispatch_attempts_total` plus `conflux_dispatch_duration_seconds` in `internal/dispatch/retry.go`

**Checkpoint**: MVP. One agent drains a stream into a stub, survives a restart, and drops rather than stalls when the stub fails.

---

## Phase 4: User Story 2 - Multiple agents run side by side on different triggers (Priority: P2)

**Goal**: Several agents with independent positions and trigger modes, where one agent's failure is invisible to the others.

**Independent Test**: Run one stream agent and one periodic agent; force the stream agent to fail on every event and confirm the periodic agent keeps running on schedule at its own position.

### Tests for User Story 2 ⚠️

- [ ] T042 [P] [US2] Unit test in `internal/agent/periodic_test.go`: a run that overruns its next due time does not start a second overlapping run, and the skip increments `conflux_agent_runs_total{outcome="skipped_overlap"}`
- [ ] T043 [P] [US2] Unit test in `internal/agent/runner_test.go`: a panicking agent is recovered, marked `degraded`, and every other agent continues — with the failure attributed to the agent's name
- [ ] T044 [P] [US2] Integration test in `test/integration/multi_agent_test.go` (`//go:build integration`): two stream agents on the same stream each see every event and neither consumes the other's
- [ ] T045 [P] [US2] Integration test in `test/integration/multi_agent_test.go`: an agent disabled via `AGENT_<ID>_ENABLED=false` does not run and the others are unaffected

### Implementation for User Story 2

- [ ] T046 [US2] Implement the periodic trigger in `internal/agent/periodic.go`: `time.Ticker` at `AGENT_<ID>_INTERVAL` with a `sync/atomic` in-flight guard (research.md D7); count skipped ticks rather than queueing them
- [ ] T047 [US2] Derive a per-agent consumer group name in `internal/ingest/redis/stream.go` from `REDIS_CONSUMER_GROUP` plus the agent id, so two agents on one stream each get the full event set independently
- [ ] T048 [US2] Set the per-agent consumer name from `REDIS_CONSUMER_NAME` plus the agent id in `internal/ingest/redis/stream.go`, so pending entries are attributable to a specific agent instance
- [ ] T049 [US2] Add restart backoff and the `degraded` state transition to `internal/agent/runner.go` per the data-model.md state diagram; `recovering` and `degraded` must never propagate to another agent
- [ ] T050 [US2] Extend `internal/agent/registry.go` to construct both trigger modes from `AGENT_<ID>_MODE` and reject an agent declaring neither or both
- [ ] T051 [P] [US2] Implement the CEP stub destination in `internal/dispatch/stub/cep.go` posting to `DEST_CEP_URL`

**Checkpoint**: Multiple agents run independently; failure isolation is proven by test, not by assertion.

---

## Phase 5: User Story 3 - Operators see lag and support is told about anomalies (Priority: P3)

**Goal**: Lag is measured per agent and per source, drives readiness, and raises rate-limited support notifications.

**Independent Test**: Pause an agent while seeding past the threshold; confirm reported lag grows, `/readyz` degrades while `/livez` stays healthy, and support gets one notification per anomaly window rather than one per event.

### Tests for User Story 3 ⚠️

- [ ] T052 [P] [US3] Unit test in `internal/observability/anomaly_test.go`: the `detected → notified → suppressed → cleared → resolved-notified` transitions, `ANOMALY_NOTIFY_COOLDOWN` suppression, the `ANOMALY_MAX_PER_HOUR` ceiling, and exactly one resolved notification
- [ ] T053 [P] [US3] Integration test in `test/integration/lag_health_test.go` (`//go:build integration`): quickstart Scenario 4 — lag over `LAG_THRESHOLD_ENTRIES` returns `503` from `/readyz` naming the agent and reason, while `/livez` still returns `200`
- [ ] T054 [P] [US3] Integration test in `test/integration/lag_health_test.go`: seed past the 10k local cap with the agent stopped so entries are trimmed from under the group; assert `XINFO GROUPS` `NULL` lag becomes `conflux_agent_lag_approximate=1`, readiness degrades, and an `entries_trimmed` anomaly is raised — "a reading of zero there would invert the signal"

### Implementation for User Story 3

- [ ] T055 [US3] Implement lag collection in `internal/ingest/redis/lag.go`: `XINFO GROUPS` `lag` field, falling back to `XLEN` minus `entries-read` when the server reports `NULL`, marking the reading `Approximate` (research.md D11); list lag is `LLEN`
- [ ] T056 [US3] Populate `conflux_agent_lag_entries` and `conflux_agent_lag_approximate` from `LagReading` in `internal/observability/metrics.go`
- [ ] T057 [P] [US3] Implement `/livez` in `internal/observability/http.go`: `200 OK` body `ok`, never consulting Redis or lag — "a liveness probe that fails on a dependency outage causes a restart loop that fixes nothing"
- [ ] T058 [US3] Implement `/readyz` in `internal/observability/http.go` returning the JSON shape in contracts/operational-endpoints.md, with `503` and `"status": "degraded"` when Redis is unreachable, any agent exceeds its lag threshold, any agent is `degraded`, or any lag reading is approximate
- [ ] T059 [P] [US3] Serve `/metrics` in `internal/observability/http.go` on `HTTP_ADDR` via `promhttp`
- [ ] T060 [US3] Implement the anomaly detector in `internal/observability/anomaly.go` for `lag_threshold`, `dispatch_drop_rate`, `source_unavailable`, and `entries_trimmed`, running every `ANOMALY_CHECK_INTERVAL` to meet SC-007's five-minute detection bound
- [ ] T061 [US3] Implement the notification rate limiter in `internal/observability/anomaly.go` using `ANOMALY_NOTIFY_COOLDOWN` and `ANOMALY_MAX_PER_HOUR`, with exactly one notification when a condition clears
- [ ] T062 [P] [US3] Implement the email stub in `internal/dispatch/stub/email.go`: a sink while `EMAIL_ENABLED=false`, addressing `SUPPORT_EMAIL_TO` from `SUPPORT_EMAIL_FROM` via `SMTP_ADDR`, carrying anomaly notifications rather than envelopes
- [ ] T063 [US3] Wire `conflux_anomalies_active` and `conflux_notifications_sent_total` (including suppressed-by-cooldown) in `internal/observability/anomaly.go`

**Checkpoint**: An operator can tell within a minute whether the service is keeping up, and support hears about it exactly once per window.

---

## Phase 6: User Story 4 - Housekeeping agents keep the queue and logs manageable (Priority: P4)

**Goal**: A continuous agent drains the log list and emits one summary per closed period; scheduled agents reclaim abandoned pending entries — all of it touching nothing outside this service's own consumer groups.

**Independent Test**: Two halves, each testable alone. (1) Seed the log list under sustained write volume and confirm the drain keeps its depth below `SUMMARY_MAX_LIST_DEPTH` while one summary is emitted per closed period. (2) Leave entries pending under a dead consumer name and confirm they are reclaimed while a live consumer's in-flight entries and every other key are untouched.

### Tests for User Story 4 ⚠️

- [ ] T064 [P] [US4] Unit test in `internal/housekeeping/summarize_test.go`: exactly one summary is emitted per closed period, its identity is the period boundary pair so a re-send is absorbed rather than double-counted (FR-018a), and a period spanning a simulated restart is emitted with `Partial` set (FR-018b). Do NOT test recomputation — destructive list reads make a second pass over a period impossible
- [ ] T065 [P] [US4] Integration test in `test/integration/housekeeping_test.go` (`//go:build integration`): quickstart Scenario 5 — stage pending entries under a killed consumer, wait out `PENDING_MIN_IDLE`, run reclaim, assert they are processed and a live consumer's in-flight entries are untouched
- [ ] T066 [P] [US4] Integration test in `test/integration/housekeeping_test.go`: snapshot the keyspace before and after a full housekeeping cycle and assert no difference and unchanged `XLEN` (SC-012, FR-020a)
- [ ] T082 [P] [US4] Integration test in `test/integration/list_consume_test.go` (`//go:build integration`): the log-list ingestion path end to end — push entries with the gateway's Lua semantics, assert continuous draining keeps `LLEN` below `SUMMARY_MAX_LIST_DEPTH`, that a `LOGS_LIST_FULL` refusal is observable when the drain is stopped, and that one summary is emitted per closed period. The constitution requires each new ingestion path to arrive with an integration test against the local stack

### Implementation for User Story 4

- [ ] T067 [US4] Implement destructive list consumption in `internal/ingest/redis/list.go`: blocking pop from `REDIS_LIST_LOGS`, with `Ack` as a no-op "because the pop already consumed it" and `Nack` recording the loss since "nothing can restore it". The agent runs in **continuous** mode (US1's trigger, T033), never on the summary interval — FR-018c: "letting the list fill makes this service the cause of upstream log loss"
- [ ] T068 [US4] Implement pending reclaim in `internal/ingest/redis/pending.go` using `XAUTOCLAIM` with `PENDING_MIN_IDLE` and `PENDING_RECLAIM_BATCH`, following the returned cursor so reclaim is paginated and bounded (research.md D6)
- [ ] T069 [US4] Implement consumer retirement in `internal/ingest/redis/pending.go`: retire only consumers holding no pending entries and idle beyond `CONSUMER_RETIRE_IDLE`
- [ ] T070 [US4] Implement the reclaim agent in `internal/housekeeping/reclaim.go` wiring T068/T069 to the periodic trigger; increment `conflux_entries_reclaimed_total` and populate `conflux_pending_entries`
- [ ] T071 [US4] Implement log summarization in `internal/housekeeping/summarize.go`: fold drained entries into in-memory counters keyed by clock-aligned `SUMMARY_PERIOD` bucket, emitting one `LogSummary` (`Entry count`, `Breakdown` by gateway and outcome, `Partial`) to `SUMMARY_DESTINATION` and structured output when a bucket closes
- [ ] T080 [US4] Set `Partial` on any summary whose period spanned a process start in `internal/housekeeping/summarize.go` — a restart loses that period's in-memory accumulation and FR-018b requires the undercount be declared rather than presented as complete
- [ ] T072 [US4] Implement read-only queue reporting in `internal/housekeeping/report.go`: entry counts, configured caps, pending counts, consumer liveness — reporting only
- [ ] T081 [US4] Raise a `log_list_depth` anomaly in `internal/observability/anomaly.go` when `LLEN` exceeds `SUMMARY_MAX_LIST_DEPTH`, with the detail naming the 10k cap at which the gateway's writes are refused — the drain falling behind is upstream data loss, not a local slowdown
- [ ] T073 [US4] Add a guard test in `internal/housekeeping/guard_test.go` asserting no code path issues a delete, trim, expire, or rename outside this service's own consumer groups (FR-020a); "Stream trimming and the log list's expiry belong to the gateway"

**Checkpoint**: All four stories functional and independently testable.

---

## Phase 7: Polish & Cross-Cutting Concerns

- [ ] T074 [P] Add a `Dockerfile` building a static binary on `golang:1.25-alpine`, for parity with how pulse-gateway is containerised in the local stack
- [ ] T075 [P] Expand `README.md` beyond its two lines: what the service does, how to run it against `pulse-infra`, and a pointer to `specs/001-event-agent-runtime/quickstart.md`
- [ ] T076 Reconcile `.env.example` against contracts/configuration.md — every key present, every comment naming what it contracts with, defaults matching the local stack
- [ ] T077 Run the full quickstart.md walkthrough end-to-end against a fresh `make up` and record any step that does not behave as written
- [ ] T078 [P] Verify `CLAUDE.md` and `.specify/memory/constitution.md` are consistent with what was actually built (the constitution's Governance section requires `CLAUDE.md` to be kept consistent with it)
- [ ] T079 Confirm SC-002 at scale in `test/integration/scale_test.go` (`//go:build integration`): 10,000 events through an intermittently failing destination, with every event accounted for as delivered or recorded-dropped

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **User Stories (Phase 3-6)**: All depend on Foundational completion; then parallel if staffed, or sequential P1 → P2 → P3 → P4
- **Polish (Phase 7)**: Depends on all desired user stories

### User Story Dependencies

- **US1 (P1)**: Foundational only. No dependency on other stories — this is the MVP
- **US2 (P2)**: Foundational only. Reuses US1's dispatch chain but is independently testable with a periodic agent that dispatches nothing
- **US3 (P3)**: Foundational only. Needs a running agent to report lag *for*; use US1's, or a stub agent
- **US4 (P4)**: Foundational, plus **both** triggers — US1's continuous trigger (T033) for the log drain and US2's periodic trigger (T046) for reclaim and for closing summary buckets. This is the one genuine cross-story dependency, and it widened when summarization moved to continuous drain

### Within Each User Story

- Tests first, failing, before implementation
- Boundary/ingest layer before agent layer before dispatch wiring
- Story complete and checkpointed before moving to the next priority

### Parallel Opportunities

- Setup: T004, T005, T006 together after T003
- Foundational: T008, T012, T014, T015, T018, T019 together (different files, no shared state)
- US1: all four test tasks T025-T029 together; T036 alongside T034/T035
- US2: T042-T045 together; T051 anytime
- US3: T052-T054 together; T057, T059, T062 together
- US4: T064-T066 together
- Across stories: once Phase 2 checkpoints, US1/US2/US3 can proceed on separate branches; US4 waits on T046

---

## Parallel Example: User Story 1

```bash
# All US1 tests first — they must fail before implementation starts:
Task: "Contract test for destination wire shape in internal/dispatch/destination_test.go"
Task: "Unit test for HTTP outcome mapping in internal/dispatch/retry_test.go"
Task: "Integration test for stream drain in test/integration/stream_dispatch_test.go"
Task: "Integration test for restart resumption in test/integration/restart_resume_test.go"

# Then the independent implementation files:
Task: "Implement LTR stub in internal/dispatch/stub/ltr.go"
Task: "Implement retry policy in internal/dispatch/retry.go"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Phase 1 Setup — the module exists and `CLAUDE.md` stops claiming the runtime is undecided
2. Phase 2 Foundational — CRITICAL, blocks everything, and the boundary test (T024) locks Principle II in before there is code to leak
3. Phase 3 US1 — one agent, one stream, one stub destination
4. **STOP and VALIDATE**: quickstart Scenarios 1-3 against the live stack
5. That is a working pipeline: events reach a destination, restarts lose nothing, a dead destination drops rather than stalls

### Incremental Delivery

1. Setup + Foundational → foundation ready
2. + US1 → **MVP**, demonstrable end-to-end
3. + US2 → multiple agents, failure isolation proven
4. + US3 → operable: lag visible, support notified
5. + US4 → maintainable: logs summarized, pending entries reclaimed

Each increment is independently testable and adds value without breaking the previous one.

### Parallel Team Strategy

After Phase 2 checkpoints: Developer A takes US1, B takes US3 (against a stub agent), C takes US2 then US4. US4's dependency on T046 makes C's ordering fixed; A and B are free.

---

## Notes

- [P] tasks = different files, no dependencies
- Task ids are stable identifiers, not a strict execution sequence: T080-T082 were added during `/speckit-analyze` remediation and belong to Phase 6 where they appear, not after T079
- Every task cites the contract, research decision, or data-model rule it implements — follow the link rather than re-deciding at implementation time
- Verify tests fail before implementing
- Commit after each task or logical group; the constitution requires any change touching a cross-repo name to cite `stack-contract.md` and name the affected sibling repos
- `go test ./...` must stay green without Docker; `-tags=integration` is where the stack is required
