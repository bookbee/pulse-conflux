# Contract: Operational Endpoints

Served by stdlib `net/http` on `HTTP_ADDR` (default `:8090`). No authentication — the port is not published outside the deployment network. This is the only HTTP surface this service exposes; it is not an API.

## `GET /livez`

Is the process alive. `200 OK` with body `ok` whenever the process can serve. Never consults Redis or lag — a liveness probe that fails on a dependency outage causes a restart loop that fixes nothing.

## `GET /readyz`

Is the service *keeping up*. This is where FR-023 lands: readiness reflects lag, not merely liveness.

`200 OK` when every enabled agent is within its lag threshold and Redis is reachable:

```json
{
  "status": "ready",
  "redis": "ok",
  "agents": [
    {"id": "events_ltr", "mode": "stream", "source": "ingestion-events",
     "state": "running", "lag_entries": 12, "lag_approximate": false}
  ]
}
```

`503 Service Unavailable` with the same shape and `"status": "degraded"` when any of: Redis unreachable; an agent's lag exceeds `LAG_THRESHOLD_ENTRIES`; an agent is `degraded`; or a lag reading is approximate — which means entries were trimmed from under the group and data was lost upstream (D11).

A degraded readiness must name the agent and the reason. An operator reading this should know within one minute whether the service is behind and by how much (SC-004).

## `GET /metrics`

Prometheus text format, matching `pulse-gateway`'s surface so one scrape config covers both.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `conflux_agent_lag_entries` | gauge | `agent`, `source` | Entries behind (FR-022) |
| `conflux_agent_lag_approximate` | gauge | `agent`, `source` | 1 when derived because the server reported `NULL` lag |
| `conflux_events_processed_total` | counter | `agent`, `source` | Envelopes successfully processed |
| `conflux_events_unprocessable_total` | counter | `agent`, `source`, `reason` | Parse/validation failures (FR-012) |
| `conflux_dispatch_attempts_total` | counter | `agent`, `destination`, `outcome` | `outcome` = `delivered`\|`transient`\|`permanent` |
| `conflux_dispatch_dropped_total` | counter | `agent`, `destination`, `reason` | Budget exhausted or permanent failure (FR-017b) |
| `conflux_dispatch_duration_seconds` | histogram | `agent`, `destination` | Across all attempts |
| `conflux_agent_runs_total` | counter | `agent`, `outcome` | Periodic agents; `outcome` includes `skipped_overlap` (FR-005) |
| `conflux_agent_run_duration_seconds` | histogram | `agent` | Periodic agents |
| `conflux_agent_panics_total` | counter | `agent` | Recovered panics (FR-004) |
| `conflux_pending_entries` | gauge | `agent`, `source` | Pending entry count from `XPENDING` |
| `conflux_entries_reclaimed_total` | counter | `agent`, `source` | Reclaimed by `XAUTOCLAIM` (FR-019) |
| `conflux_anomalies_active` | gauge | `type` | Currently active anomalies |
| `conflux_notifications_sent_total` | counter | `type`, `result` | Support emails, including suppressed-by-cooldown |

## Logging contract

JSON via `log/slog`. **Every line concerning an envelope carries `event_id` and `gateway_id`** (FR-024) — attached as base attributes at the ingestion boundary, not by each call site, because "every line" is otherwise unenforceable.

Standard attributes: `agent`, `source`, `component`. Level: `error` for drops and unprocessable entries (they are the only record those events existed), `warn` for anomalies raised and overlap skips, `info` for lifecycle.
