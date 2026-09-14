# Feature Specification: Event Agent Runtime

**Feature Branch**: none created — spec directory `001-event-agent-runtime`; work is on `main`

**Created**: 2026-09-13

**Status**: Draft — clarifications resolved 2026-09-13 (Q1 dispatch-only, Q2 bounded retry then recorded drop, Q3 own-consumer-group upkeep only); ready for planning

**Input**: User description: "This project works as an agent, reading data from Redis ingested by the pulse-gateway and invoking external interfaces (dummy for now). This project will have multiple agents that run periodically or listen to the Redis streams and act accordingly, such as passing the processed information to an external search engine like LTR or invoking the internal CEP engine. These agents will also perform housekeeping tasks like summarizing event logs, maintaining Redis, and occasionally sending emails to support for any anomalies. Please review the scope as mentioned and update the specs to start the development work."

## User Scenarios & Testing *(mandatory)*

The actors in this feature are not end users. They are:

- **Platform operator** — runs and configures pulse-conflux, adds agents, watches whether it is keeping up.
- **Support engineer** — receives anomaly notifications and acts on them.
- **Downstream consumer** — the search-ranking service (LTR) and the internal CEP engine, which receive what agents dispatch.

### User Story 1 - Stream-listening agent dispatches events to a destination (Priority: P1)

A platform operator configures an agent that continuously listens to an ingestion stream. As the gateway writes events, the agent picks them up, reads the common envelope, and hands the result to a configured destination. The destination is a stub in this feature, so the agent's correctness is judged by what it attempted to send, not by a live external system.

**Why this priority**: This is the whole point of the service and the smallest slice that delivers value. Without it there is no pipeline; with it alone, events reach a destination and the service is useful. It also forces the ingestion boundary, envelope parsing, and acknowledgement handling into existence, which every later story builds on.

**Independent Test**: Bring up the local stack, write envelopes to a stream, and confirm the agent dispatched each one exactly once to the stub, with the envelope fields intact and the stream position advanced.

**Acceptance Scenarios**:

1. **Given** the local stack is running and an agent is configured for the events stream, **When** the gateway writes three envelopes, **Then** the agent dispatches three payloads to the configured destination and advances its position past all three.
2. **Given** an agent whose consumer group does not yet exist, **When** the agent starts, **Then** it creates the group and begins consuming without manual setup.
3. **Given** an envelope produced by an API-key gateway request (no `event_header`), **When** the agent processes it, **Then** it dispatches successfully and does not treat the missing header as an error.
4. **Given** an event the agent already dispatched, **When** the same event is redelivered, **Then** the destination's end state is unchanged.
5. **Given** the agent is stopped mid-stream and restarted, **When** it resumes, **Then** it continues from its last acknowledged position with no event skipped.

---

### User Story 2 - Multiple agents run side by side on different triggers (Priority: P2)

A platform operator runs several agents at once: some listening to streams continuously, others waking on a schedule. Each agent has its own destination and its own position in the data. One agent failing or being slow does not stop the others.

**Why this priority**: The service is explicitly plural — "multiple agents". Until more than one can run independently, every new destination means another deployment. This story also introduces scheduled execution, which the housekeeping stories depend on.

**Independent Test**: Configure one stream agent and one scheduled agent, run both, then force the stream agent to fail repeatedly and confirm the scheduled agent keeps running on time and at its own position.

**Acceptance Scenarios**:

1. **Given** two stream agents subscribed to the same stream, **When** events arrive, **Then** each agent sees every event independently, and neither consumes the other's.
2. **Given** a scheduled agent with a configured interval, **When** the interval elapses, **Then** the agent runs, and a run that overruns its next due time does not start a second overlapping run.
3. **Given** one agent raising errors on every event, **When** it fails, **Then** every other agent continues processing and the failure is attributed to the named agent.
4. **Given** an agent disabled in configuration, **When** the service starts, **Then** that agent does not run and the others are unaffected.

---

### User Story 3 - Operators see lag and support is told about anomalies (Priority: P3)

A platform operator can see, per agent and per source, how far behind processing is. When the service falls behind, a destination keeps failing, or another anomaly appears, a support engineer receives an email — enough of them to notice, not enough to drown in.

**Why this priority**: Both sources are lossy under lag and neither pushes back on the gateway, so falling behind destroys data upstream rather than slowing the producer. An operator who cannot see lag cannot tell a healthy service from one that is silently losing data. The email path is the user-visible half of the same concern.

**Independent Test**: Pause an agent while the gateway keeps writing, then confirm the reported lag grows, the health signal stops reporting healthy, and support receives exactly one notification per anomaly window rather than one per event.

**Acceptance Scenarios**:

1. **Given** an agent is behind, **When** an operator inspects the service's health, **Then** lag is reported per agent and per source, and health reflects lag rather than only whether the process is alive.
2. **Given** lag crosses its configured threshold, **When** the condition persists, **Then** support receives one notification naming the agent, source, and observed lag.
3. **Given** the same anomaly continues for an extended period, **When** further checks run, **Then** notifications are rate-limited rather than repeated per check.
4. **Given** the anomaly clears, **When** the next check runs, **Then** support is told it has cleared.
5. **Given** any log line about an envelope, **When** it is emitted, **Then** it carries the event's identifier and originating gateway so records join back to gateway logs.

---

### User Story 4 - Housekeeping agents keep the queue and logs manageable (Priority: P4)

A continuously running agent drains the raw event-log entries and folds them into a compact summary emitted once per period, while scheduled agents perform routine upkeep of the queue this service owns — reclaiming entries left pending by a consumer that died, and retiring consumers that no longer exist.

Draining is not optional and not merely a summarization concern: the log list refuses new writes once it reaches its cap, so a consumer that falls behind makes the **gateway** start losing logs.

**Why this priority**: This is maintenance, not delivery: valuable, but the pipeline works without it and the earlier stories must exist first. Pending-entry reclaim matters only once agents have been running long enough to crash mid-batch. The log-drain half carries more urgency than its priority suggests — while it is unbuilt, nothing is consuming the log list at all.

**Independent Test**: Leave entries pending under a consumer name, run the housekeeping agent, and confirm the entries are reclaimed and processed while no other consumer's in-flight work is disturbed.

**Acceptance Scenarios**:

1. **Given** entries left pending by a consumer that is gone, **When** the housekeeping agent runs, **Then** those entries are reclaimed and processed, and entries still in flight elsewhere are left alone.
2. **Given** the log agent is running, **When** raw log entries arrive, **Then** they are drained continuously and the list stays well below the cap at which the gateway's writes would be refused.
3. **Given** a summary period closes, **When** the agent emits, **Then** it produces one summary stating the period it covers, and re-emitting that same summary leaves the destination's end state unchanged — identity is the period, so a repeat neither double-counts nor adds a second distinct summary.
4. **Given** the service restarts mid-period, **When** the period closes, **Then** the summary for that period is emitted marked partial, because the entries consumed before the restart are gone and no datastore holds them.
5. **Given** a housekeeping action would remove data another repo in the platform depends on, **When** the agent runs, **Then** it does not perform that action.

---

### Edge Cases

- An entry's payload is not valid JSON, or is valid JSON that does not match the envelope shape — the agent must not stop the stream over one bad entry, and the entry must be recoverable for inspection rather than silently discarded.
- `event_header` is absent (API-key gateway requests) — this is normal, not an error.
- The stream is trimmed while an agent is behind, so entries the agent had not yet read are gone — the agent must not report success for data it never saw, and this is an anomaly worth telling support about.
- The log list hits its cap and the gateway's script drops writes outright — nothing arrives to consume, and the absence itself is the signal.
- A consumer group does not exist when an agent starts, or an agent starts against a stream that has never been written to.
- Two instances of the same agent run at once (a restart overlapping a shutdown) — neither may double-dispatch in a way the destination cannot absorb.
- The queue is unreachable, refuses writes, or rejects credentials — agents must not spin hot against it.
- A destination hangs rather than failing outright.
- The same `event_id` appears twice from the source.
- `received_at` is in the future or badly skewed relative to local time.
- A scheduled agent's run takes longer than its interval.
- Shutdown arrives mid-batch, with entries read but not acknowledged.

## Requirements *(mandatory)*

### Functional Requirements

**Agent runtime**

- **FR-001**: The system MUST host multiple independently configured agents in one service.
- **FR-002**: Each agent MUST declare one trigger mode: continuous listening on a source, or periodic execution on a configured interval.
- **FR-003**: Each agent MUST be independently enabled or disabled through configuration, without code changes to other agents.
- **FR-004**: A failure in one agent MUST NOT stop, stall, or corrupt any other agent; the runtime MUST attribute the failure to the named agent.
- **FR-005**: A periodic agent MUST NOT start a new run while its previous run is still in progress.
- **FR-006**: The system MUST shut down without acknowledging work it did not complete, so that unfinished entries are redelivered rather than lost.

**Ingestion and delivery**

- **FR-007**: Agents MUST consume from the gateway-written sources named in the cross-repo stack contract, reading the JSON envelope carried in each entry.
- **FR-008**: Envelope parsing MUST be shared across all sources and entry structures; consumption, acknowledgement, consumer-group lifecycle, pending-entry recovery, and claim logic MUST remain behind the ingestion boundary and MUST NOT be visible to agents' processing or dispatch logic.
- **FR-009**: The system MUST own its consumer groups end to end — creating them when absent, tracking pending entries, and claiming abandoned ones — because nothing upstream creates them.
- **FR-010**: The system MUST treat `event_header` as optional, present only for JWT-authenticated gateway requests.
- **FR-011**: Redelivery MUST be treated as normal operation; every dispatch and every persisted effect MUST be idempotent, keyed on the envelope's event identifier.
- **FR-012**: An entry that cannot be parsed or processed MUST NOT block progress on the remaining entries, and MUST be recorded in full in structured output together with its source and position, so it can be inspected and replayed by hand while it remains in the source. Since no datastore exists (FR-013), that record is the only trace — it MUST NOT be reduced to a counter.
- **FR-013**: Ingestion MUST be dispatch-only in this feature: no datastore is introduced, and no envelope is durably stored. Any state an agent needs between runs MUST be derivable from the source itself, from the service's own consumer-group position, or from configuration. Durable persistence of events — which the repository README anticipates — is deferred to a later feature, and nothing in this one may foreclose it.

**Dispatch destinations**

- **FR-014**: Destinations MUST be configurable per agent, with endpoints and credentials read from configuration and never hardcoded in source or fixtures.
- **FR-015**: All external destinations — the search-ranking (LTR) service, the internal CEP engine, and the support email path — MUST be stubbed in this feature, with tests asserting on the outgoing call rather than reaching a real system.
- **FR-016**: Adding a destination MUST NOT require changing the ingestion boundary or any unrelated agent.
- **FR-017**: When a destination is unavailable or failing, the system MUST retry the dispatch up to a configured attempt budget with backoff between attempts, and on exhausting that budget MUST drop the event and record the drop with its event identifier, the destination, the attempt count, and the failure reason. A drop MUST NOT stall the agent or hold up later events, and the dropped event MUST be identifiable well enough to be re-sent by hand while it remains in the source.
- **FR-017a**: Every destination MUST have a configured per-attempt timeout, so a destination that hangs consumes the attempt budget rather than blocking the agent indefinitely.
- **FR-017b**: Drops MUST be counted per agent and per destination, and a drop rate crossing its configured threshold MUST raise an anomaly (FR-025). Loss is accepted here by design, so it MUST be visible rather than silent.

**Housekeeping agents**

- **FR-018**: A continuously running agent MUST drain raw event-log entries and fold them into a compact summary, emitting one summary per closed period to a configured destination (stubbed in this feature) and to structured output, stating which period it covers.
- **FR-018c**: That agent MUST drain continuously rather than on the summary interval, keeping the log list below the cap at which the gateway's Lua script refuses writes. A summary cadence MUST NOT determine the drain cadence: letting the list fill makes this service the cause of upstream log loss.
- **FR-018a**: A summary's identity MUST be its closed period boundary pair, so re-emitting the same summary is absorbed by the destination rather than double-counted. Recomputation is **not** available as a dedup mechanism: the log list is read destructively, so entries are gone once consumed and no second pass over a period is possible.
- **FR-018b**: Because aggregation is held in memory and no datastore exists (FR-013), a restart mid-period loses that period's accumulated counts. The agent MUST still emit the period's summary and MUST mark it partial rather than presenting an undercount as complete.
- **FR-019**: A periodic agent MUST reclaim entries left pending by consumers that are no longer active, and MUST NOT disturb entries currently in flight for a live consumer.
- **FR-020**: Queue upkeep MUST be limited to two things: read-only reporting on the queue this service consumes (entry counts, configured caps, pending-entry counts, consumer liveness), and upkeep of the consumer groups this service itself owns — reclaiming pending entries and retiring consumer names that are no longer active.
- **FR-020a**: The system MUST NOT delete, trim, expire, rename, or otherwise mutate any queue data outside its own consumer groups. Stream trimming and the log list's expiry belong to the gateway; the eviction policy is deliberately set so writes fail loudly rather than keys vanishing under a consumer, and this service MUST NOT work around that.
- **FR-021**: No housekeeping action MUST alter a name, key, structure, or limit that the cross-repo stack contract governs.

**Observability and alerting**

- **FR-022**: The system MUST measure and expose consumer lag per agent and per source as a first-class signal.
- **FR-023**: Health and readiness reporting MUST reflect lag, not merely that the process is alive.
- **FR-024**: Every log line concerning an envelope MUST carry the event identifier and the originating gateway identifier, and logging MUST be structured.
- **FR-025**: The system MUST detect anomalies — at minimum sustained lag beyond a configured threshold, repeated dispatch failure to a destination, and source unavailability — and notify support by email.
- **FR-026**: Anomaly notifications MUST be rate-limited per anomaly type so a persistent condition produces a bounded number of messages, and MUST be followed by a notification when the condition clears.
- **FR-027**: Support recipients, thresholds, and rate limits MUST be configurable.

**Configuration**

- **FR-028**: Every new configuration key MUST be added to the example configuration file in the same change, with a comment naming what it contracts with.
- **FR-029**: Source names, destination endpoints, credentials, intervals, and thresholds MUST come from configuration; none may be hardcoded.

### Key Entities

- **Envelope** — the unit of data carried in every source entry, identical in shape across all of them. Identified by an event identifier, attributed to a gateway, timestamped at receipt, carrying a retry count, its source name, its payload, and an optional header present only for JWT-authenticated requests.
- **Agent** — a named, independently configurable unit of work with one trigger mode (continuous or periodic), one source or schedule, one or more destinations, and its own position in the data.
- **Agent run** — a single execution of a periodic agent, or a single processing pass of a streaming agent: what it covered, how long it took, what it dispatched, what failed.
- **Destination** — an external or internal interface an agent hands results to (search-ranking service, CEP engine, support email), stubbed in this feature.
- **Lag reading** — how far behind an agent is on a source at a point in time; the primary health signal.
- **Anomaly** — a detected condition worth a human's attention, with a type, the agent and source it concerns, when it started, whether it is still active, and when support was last told.
- **Log summary** — the compact form produced from raw log entries, covering a stated period.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An operator can add a new agent and put it into service through configuration alone, with no change to any existing agent's behavior and no interruption to agents already running.
- **SC-002**: Every event an agent reads is either dispatched to its destination at least once, or — after its retry budget is exhausted — recorded as dropped with its identifier and failure reason. Across a test run of at least 10,000 events with an intermittently failing destination, every event is accounted for as one or the other, and none disappears without a record.
- **SC-003**: Replaying an already-processed batch leaves every destination in the same end state as the first run — duplicate delivery produces no duplicate effect.
- **SC-004**: An operator can tell within one minute, from the service's own health reporting, whether processing is keeping up, and by how much it is behind, per agent and per source.
- **SC-005**: With one agent failing continuously, every other agent maintains its normal processing rate and its own position for the duration of the failure.
- **SC-006**: After an unclean stop, processing resumes from the last acknowledged position with no event skipped and no gap in coverage.
- **SC-007**: A persistent anomaly produces its first support notification within five minutes of onset and no more than a configured maximum per hour thereafter, followed by exactly one message when it clears.
- **SC-008**: Under sustained local-stack log volume, the log list never approaches the cap at which the gateway's writes are refused, and exactly one summary is emitted per closed period. A summary re-sent to the destination leaves its end state unchanged, and a summary whose period spanned a restart is marked partial.
- **SC-009**: Every record the service emits about an event can be joined to the originating gateway's own records using the identifiers the record carries.
- **SC-010**: No test reaches a real external system: every dispatch in the suite lands on a stub whose calls are asserted.
- **SC-011**: With a destination failing for a sustained period, the agent's lag stays within its normal operating range — dropped events do not accumulate into a backlog — and the resulting drop rate reaches support as a single rate-limited anomaly rather than one message per event.
- **SC-012**: Running the full housekeeping agent against the local stack leaves every queue entry, cap, and key outside this service's own consumer groups byte-for-byte unchanged.

## Assumptions

- **Redis only for this feature.** The service's charter covers both Redis and Kafka, but the gateway has no Kafka producer at this commit, so nothing would feed a Kafka path locally. Kafka ingestion is deliberately out of scope here and expected as a later feature; the ingestion boundary is designed so it can be added without touching agents.
- **Sources and envelope are fixed contracts**, defined by the platform's stack contract and not redefinable by this feature: two streams whose entries carry a single field holding the JSON envelope, and one list written with an expiry.
- **This service is the sole consumer of the log list.** Reading from a list is destructive, so a second consumer would compete for entries.
- **Destinations are stubs.** LTR, the CEP engine, and outbound email are interfaces with no live implementation in this feature; their real contracts are expected to be specified separately.
- **Integration testing runs against the local platform stack**, whose queue exists only in its full profile.
- **One deployable unit** hosts all agents for now; splitting agents across processes is a deployment concern, not a scope change, and the configuration model should not prevent it.
- **Anomaly notification is email**, to a configured support address, with no incident-management integration in this feature.
- **Local queue caps are deliberately low** so lossy behavior appears during development; those absolute numbers carry no capacity-planning meaning.
- **Dispatch-only, no datastore (Q1).** Feature 001 introduces no persistence layer. The consequence to plan around: the source is the only copy, and it is lossy under lag, so an event dropped after a failed dispatch is genuinely gone once the stream trims past it. That is accepted here and is the reason FR-017b makes drops measurable.
- **Bounded retry, then a recorded drop (Q2).** Liveness is preferred over completeness at the destination: an unavailable LTR or CEP engine must never become the reason this service falls behind and causes loss upstream. This applies uniformly to all destinations in this feature; a per-destination policy can be introduced later without reshaping the dispatch path.
- **Housekeeping touches only what this service owns (Q3).** Read-only reporting plus this service's own consumer-group upkeep. Anything that would mutate shared queue data requires agreement in `pulse-infra` first under Constitution Principle I, and is out of scope here.
- **No user-facing interface.** There is no UI and no end-user surface; operators interact through configuration and the service's own health and log output.
