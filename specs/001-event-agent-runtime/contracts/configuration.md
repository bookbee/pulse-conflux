# Contract: Configuration

Every key below is **new** and MUST be added to `.env.example` in the change that introduces it, with a comment naming what it contracts with (Constitution Principle I and Section 2). Existing `REDIS_*`, `FORWARD_*`, and `LOG_LEVEL` keys stay as they are.

Agent ids in `CONFLUX_AGENTS` are uppercased and non-alphanumerics become `_` to form key names, so `events-ltr` reads `AGENT_EVENTS_LTR_*`. Ids must be unique after normalisation.

## Runtime

| Key | Default | Contracts with |
|---|---|---|
| `HTTP_ADDR` | `:8090` | The deployment's scrape and probe configuration |
| `SHUTDOWN_GRACE` | `15s` | How long draining agents may finish in-flight work (FR-006) |

## Agent registry

| Key | Default | Contracts with |
|---|---|---|
| `CONFLUX_AGENTS` | *(empty)* | Comma-separated agent ids; empty means the service starts, serves probes, and processes nothing |
| `AGENT_<ID>_ENABLED` | `true` | FR-003 |
| `AGENT_<ID>_MODE` | *(required)* | `stream` or `periodic` (FR-002) |
| `AGENT_<ID>_SOURCE` | *(required for `stream`)* | A Redis key name from the stack contract |
| `AGENT_<ID>_SOURCE_KIND` | `stream` | `stream` or `list` — selects the consumption strategy (D5) |
| `AGENT_<ID>_DESTINATIONS` | *(required)* | Comma-separated destination names |
| `AGENT_<ID>_INTERVAL` | *(required for `periodic`)* | Go duration, e.g. `5m` |
| `AGENT_<ID>_BATCH_SIZE` | `100` | `XREADGROUP COUNT` |
| `AGENT_<ID>_BLOCK_TIMEOUT` | `5s` | `XREADGROUP BLOCK` |

## Dispatch

| Key | Default | Contracts with |
|---|---|---|
| `DISPATCH_MAX_ATTEMPTS` | `3` | FR-017 — the bounded budget after which an event is dropped |
| `DISPATCH_TIMEOUT` | `10s` | FR-017a — per attempt, not total |
| `DISPATCH_BACKOFF_INITIAL` | `200ms` | D8 |
| `DISPATCH_BACKOFF_MAX` | `5s` | D8 |
| `DEST_LTR_URL` | `http://localhost:9999/stub/ltr` | The search-ranking stub |
| `DEST_CEP_URL` | `http://localhost:9999/stub/cep` | The CEP engine stub |

## Housekeeping

| Key | Default | Contracts with |
|---|---|---|
| `PENDING_MIN_IDLE` | `5m` | `XAUTOCLAIM` minimum idle — protects a live consumer's in-flight entries (FR-019) |
| `PENDING_RECLAIM_BATCH` | `100` | `XAUTOCLAIM COUNT` |
| `CONSUMER_RETIRE_IDLE` | `24h` | How long a consumer with no pending entries may be idle before retirement |
| `SUMMARY_PERIOD` | `1h` | Closed bucket size; determines summary identity (FR-018a). This is the **emit** cadence only — the log agent drains continuously regardless (FR-018c) |
| `SUMMARY_DESTINATION` | *(required if the summary agent is enabled)* | Where summaries are emitted |
| `SUMMARY_MAX_LIST_DEPTH` | `2000` | Drain target for the log list: exceeding it raises an anomaly, because the gateway refuses log writes at the list's 10k cap (FR-018c) |

## Anomalies and notification

| Key | Default | Contracts with |
|---|---|---|
| `LAG_THRESHOLD_ENTRIES` | `1000` | `/readyz` degradation and the `lag_threshold` anomaly (FR-025). Note local caps are 10k |
| `DROP_RATE_THRESHOLD` | `0.01` | Fraction of dispatches dropped over the check window (FR-017b) |
| `ANOMALY_CHECK_INTERVAL` | `1m` | SC-007's five-minute detection bound |
| `ANOMALY_NOTIFY_COOLDOWN` | `30m` | FR-026 rate limit per anomaly type |
| `ANOMALY_MAX_PER_HOUR` | `4` | Hard ceiling per type |
| `EMAIL_ENABLED` | `false` | Stubbed locally (Principle V) |
| `SMTP_ADDR` | `localhost:1025` | |
| `SUPPORT_EMAIL_TO` | *(required if `EMAIL_ENABLED=true`)* | FR-027 |
| `SUPPORT_EMAIL_FROM` | `pulse-conflux@localhost` | |

## Rules

- No endpoint, credential, source name, interval, or threshold may be hardcoded (FR-029, Principle I).
- Defaults target the `pulse-infra` local stack. In-network consumers use container names (`redis:6379`), not `localhost`.
- A missing required key for an **enabled** agent is a startup failure naming the key. A missing key for a disabled agent is ignored — disabling must not require filling in configuration you do not use.
