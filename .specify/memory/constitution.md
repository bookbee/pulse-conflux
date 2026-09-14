<!--
SYNC IMPACT REPORT (scratch — remove before committing the amended constitution)

Version change: [CONSTITUTION_VERSION] (unfilled template) → 1.0.0
Bump rationale: initial ratification. The file previously held only core-template
placeholders and governed nothing; this is the first version with binding content.

Modified principles (placeholder → concrete):
  [PRINCIPLE_1_NAME] → I. Cross-Repo Contract Fidelity (NON-NEGOTIABLE)
  [PRINCIPLE_2_NAME] → II. Source Semantics Stay Behind the Ingestion Boundary
  [PRINCIPLE_3_NAME] → III. Assume Redelivery, Prove Idempotency
  [PRINCIPLE_4_NAME] → IV. Lag Is the Health Signal
  [PRINCIPLE_5_NAME] → V. Real Stack In, Stubs Out

Added sections:
  [SECTION_2_NAME]   → Technology & Configuration Constraints
  [SECTION_3_NAME]   → Development Workflow & Quality Gates

Removed sections: none.

Follow-up TODOs: none. No placeholder tokens retained.

Consistency notes:
  - Principle I defers to ../pulse-infra/docs/stack-contract.md as the naming
    authority; that file is external and may change independently of this one.
  - Principle V's "no build/test command yet" reality means Section 3 gates are
    written to activate when the first manifest lands, not to block before it.
-->

# pulse-conflux Constitution

## Core Principles

### I. Cross-Repo Contract Fidelity (NON-NEGOTIABLE)

Every key name, topic, port, Redis structure, and envelope field this service reads is a
contract shared with `pulse-gateway`, `pulse-ingestor`, `pulse-client`, and `pulse-infra`.
`../pulse-infra/docs/stack-contract.md` is the sole authority for those names; where this
repo disagrees with it, this repo is wrong. Renaming or reshaping anything on that page
MUST be treated as a breaking change across five repos, agreed there first, and only then
reflected here. Endpoints, credentials, topic names, and limits MUST be read from
configuration — never hardcoded in source or fixtures. Every new configuration key MUST be
added to `.env.example` in the same change, with a comment naming what it contracts with.

Rationale: this service is a consumer sitting between four other repos. A silent local
rename does not fail loudly; it reads an empty topic or the wrong key and looks healthy.

### II. Source Semantics Stay Behind the Ingestion Boundary

Redis and Kafka deliver differently, and Redis itself uses two primitives: events and
signals are streams (`XADD`, entries carrying exactly one field named `data`), logs are a
list (Lua `RPUSH` + `EXPIRE`). The JSON envelope is identical across all three, so envelope
parsing MUST be shared code. Consumption, offset and acknowledgement handling, consumer
group lifecycle, pending-entry recovery, and claim logic MUST NOT be shared and MUST NOT
leak past the ingestion boundary into persistence or forwarding. Downstream stages MUST
receive a parsed envelope with no knowledge of which source produced it.

Rationale: one pipeline over sources with incompatible delivery semantics only stays
maintainable if the differences are quarantined at the point they enter.

### III. Assume Redelivery, Prove Idempotency

The gateway only `XADD`s and never creates a consumer group; if this service uses
`XREADGROUP` it owns `XGROUP CREATE` (with `MKSTREAM`), pending-entry handling, and claims.
Kafka consumer groups are likewise this service's responsibility. Because acknowledgement
is ours, redelivery is normal operation, not an error path: every persist and forward
operation MUST be idempotent, keyed on `event_id`. Envelope fields that are conditionally
present — `event_header`, which exists only for JWT-authenticated gateway requests and is
absent for API-key ones — MUST be handled as optional; treating one as required MUST NOT be
introduced without a contract change under Principle I.

Rationale: an at-least-once source with consumer-owned acknowledgement guarantees duplicate
delivery. Idempotency is the only thing that makes that safe.

### IV. Lag Is the Health Signal

Neither source pushes back. Redis streams trim to an approximate `MAXLEN` and the log
list's Lua script drops writes outright once capped, so falling behind destroys data
upstream rather than slowing the producer. Consumer lag per source MUST therefore be
measured and exposed as a first-class metric, and health/readiness MUST reflect it rather
than reporting only process liveness. Structured logging is REQUIRED, and every log line
about an envelope MUST carry `event_id` and `gateway_id` so records join back to gateway
logs. Local `MAXLEN` caps (10k, against 100k in the gateway's own example config) are
deliberately low to surface loss during development; those absolute numbers MUST NOT be
used for capacity planning.

Rationale: with no backpressure and lossy destinations, a healthy-looking service that is
behind is actively losing data. Lag is the only place that becomes visible.

### V. Real Stack In, Stubs Out

Integration tests MUST run against the `pulse-infra` local stack (`cd ../pulse-infra &&
make up`; Redis exists only in the `full` profile, and `make reset` is required between
profile switches). External forwarding targets MUST be stubbed with assertions on the
outgoing calls (`FORWARD_ENABLED=false`) — a test MUST NOT reach a real external system
from a developer machine or CI. Behavior that does not exist upstream MUST NOT be
scaffolded speculatively: per `../pulse-infra/docs/divergences.md` the gateway has no Kafka
producer at this commit, so Kafka-path tests MUST state how the topic gets fed rather than
assuming the gateway fills it.

Rationale: the sources' quirks — single-field stream entries, Lua-dropped log writes,
disabled auto-topic-creation — are exactly what a mock reproduces incorrectly.

## Technology & Configuration Constraints

- The runtime is **Go** (decided 2026-09-14 with feature 001; `go.mod` declares `go 1.25`).
  Dependencies MUST stay minimal and MUST match `pulse-gateway`'s choices where an
  equivalent exists, so producer and consumer share one dialect: `go-redis/v9`,
  `prometheus/client_golang`, `godotenv`, and stdlib `log/slog` and `net/http`.
- Build, lint, test, and single-test commands MUST be recorded in `CLAUDE.md` and MUST stay
  accurate as they change. `go test ./...` MUST pass with no infrastructure running;
  anything needing the local stack MUST sit behind the `integration` build tag.
- `.env` and `.env.*` are gitignored and MUST stay untracked. `.env.example` ships
  placeholder values only. Credentials and secrets MUST NOT appear in source, fixtures, or
  test data.
- Values in `.env.example` target the `pulse-infra` stack only. In-network consumers MUST
  use container names (`redis:6379`, `kafka-N:9092`), not `localhost`.
- Adding a source, sink, or language surface MUST include the corresponding `.gitignore`
  additions in the same change, so generated artifacts never get committed.

## Development Workflow & Quality Gates

- Work is spec-driven through `.specify/`: `/speckit-specify` → `/speckit-plan` →
  `/speckit-tasks` → `/speckit-implement`, with `/speckit-analyze` before implementation on
  any change that crosses the ingestion boundary or touches a cross-repo name.
- Any change to a name, topic, key, or envelope field MUST cite `stack-contract.md` in its
  description, and MUST state which sibling repos are affected.
- Every change that adds or renames a configuration key MUST update `.env.example` in the
  same commit; a review MUST reject the change otherwise.
- Once a manifest lands, the full test suite MUST pass before merge, and each new ingestion
  path MUST arrive with an integration test against the local stack.
- Reviews MUST verify compliance with these principles explicitly. Deviations MUST be
  justified in writing in the pull request, naming the principle and why the simpler
  compliant option does not work.

## Governance

This constitution supersedes other practices in this repository. Where it conflicts with
`CLAUDE.md` or a README, this file wins and the other document MUST be corrected; where it
conflicts with `../pulse-infra/docs/stack-contract.md` on a cross-repo name, the stack
contract wins (Principle I).

Amendments MUST be made through `/speckit-constitution`, MUST record the rationale in the
amendment's Sync Impact Report, and MUST be reviewed like any other change. Versioning is
semantic: MAJOR for removing or redefining a principle in a backward-incompatible way,
MINOR for adding a principle or materially expanding guidance, PATCH for clarifications and
wording that change no obligation. The version line below MUST be updated in the same
change as the content it describes.

Compliance is reviewed at every pull request and again whenever a sibling repo publishes a
contract change. `CLAUDE.md` remains the runtime development guidance for agents working in
this repo and MUST be kept consistent with this constitution.

**Version**: 1.1.0 | **Ratified**: 2026-09-13 | **Last Amended**: 2026-09-14
