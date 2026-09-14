# pulse-conflux

A data aggregator service that reads events from Redis and Kafka to persist and forward
as per the integration needs.

## What it does today

pulse-conflux hosts **several independently configured agents in one process**. Each one
either listens continuously to a gateway-written source or runs on a fixed interval; it
receives a parsed envelope with no knowledge of where it came from, and hands the result
to a destination.

Feature 001 (`specs/001-event-agent-runtime/`) delivers the Redis half:

- **Stream agents** consume `ingestion-events` and `ingestion-signals` through consumer
  groups this service creates and owns, dispatching to a search-ranking service (LTR) or
  the internal CEP engine.
- **A log-drain agent** continuously drains `ingestion-logs` and emits one summary per
  closed period.
- **Housekeeping agents** reclaim entries abandoned by consumers that died, retire stale
  consumer names, and report read-only facts about the queue.
- **Lag is the health signal**: measured per agent and per source, it drives `/readyz` and
  raises rate-limited notifications to support.

**Every external destination is stubbed.** LTR, the CEP engine, and outbound email have no
live contract yet; what is fixed is the shape of the call.

**Kafka ingestion is not built yet.** The gateway has no Kafka producer at this commit, so
nothing would feed it locally. The `Source` interface is what makes it additive.

## Running it

Needs the local platform stack — Redis lives only in its `full` profile:

```bash
cd ../pulse-infra && make up && make health
cd ../pulse-conflux
cp .env.example .env      # defaults target the local stack
make run
```

Then `curl localhost:8090/readyz` and `curl localhost:8090/metrics`.

See [`specs/001-event-agent-runtime/quickstart.md`](specs/001-event-agent-runtime/quickstart.md)
for five scenarios that exercise it end to end — draining a stream, surviving a restart,
dropping rather than stalling on a dead destination, watching lag degrade readiness, and
proving housekeeping touches nothing it does not own.

## Building and testing

```bash
make build              # binary at bin/conflux
make test               # unit tests; green with no Docker running
make test-integration   # needs pulse-infra `full` up
make lint               # golangci-lint
```

`go test ./...` must never require infrastructure. Anything that needs the real stack sits
behind the `integration` build tag, and it runs against **real Redis** rather than a fake —
single-field stream entries, sliding list TTLs, and a cap that refuses writes instead of
trimming are exactly what a mock reproduces incorrectly.

## Two things that will bite you

**Falling behind is upstream data loss, not a local slowdown.** Neither source applies
backpressure. Streams trim to an approximate `MAXLEN`, and the log list's script **refuses**
the gateway's write once full rather than evicting to make room. Consumer lag is the metric
that matters.

**There is no datastore.** Feature 001 is dispatch-only. An event dropped after a failing
destination exhausts its retry budget is genuinely gone — which is why every drop is logged
in full with its identity and counted, never reduced to a metric.

## Cross-repo contract

Key names, topics, ports, and Redis structures are shared with `pulse-gateway`,
`pulse-ingestor`, `pulse-client` and `pulse-infra`. The authority is
[`../pulse-infra/docs/stack-contract.md`](../pulse-infra/docs/stack-contract.md); where this
repo disagrees with it, this repo is wrong.
