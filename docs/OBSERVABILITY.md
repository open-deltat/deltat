# deltat observability: the operator's page

deltat emits three signals: Prometheus metrics on an opt-in `/metrics` endpoint, structured logs
via `tracing`, and a slow-query log. All three are configured with env vars only; there is no
config file. This page lists every metric that exists, what question each answers, the error
taxonomy on the wire, and a starter set of alerts. A ready-made Grafana dashboard lives at
[`../grafana/deltat.json`](../grafana/deltat.json).

---

## 1. Turning it on

### Metrics

Metrics are off by default. Set a port and the binary serves Prometheus text format on it:

```bash
DELTAT_METRICS_PORT=9187 deltat
curl -s localhost:9187/metrics | grep deltat_
```

### Logs

Logs go to stdout. Two env vars control them:

| Env var | Default | Effect |
|---|---|---|
| `DELTAT_LOG_FORMAT` | `text` | `json` (case-insensitive) switches to newline-delimited JSON for log collectors. Any other value means human-readable text, so a typo degrades to the default instead of killing startup |
| `RUST_LOG` | `info` | Level filter in `tracing` `EnvFilter` syntax, e.g. `deltat=debug`. Unset or unparseable falls back to `info` |

Panics in connection tasks are routed through the log stream as `tracing` errors before the
default panic hook runs, so a JSON collector sees them instead of losing them to raw stderr.

### Slow-query log

`DELTAT_SLOW_QUERY_MS` is the local equivalent of PostgreSQL's `log_min_duration_statement`:
statements taking at least that many milliseconds are logged at `warn` and counted in
`deltat_slow_queries_total`. `0` (the default) disables it. The log line carries the command
label, tenant, and elapsed time, never the statement text: query text can contain customer
identifiers.

```bash
DELTAT_METRICS_PORT=9187 DELTAT_LOG_FORMAT=json DELTAT_SLOW_QUERY_MS=50 deltat
```

---

## 2. The metrics

One exporter note before the tables: histograms are rendered as Prometheus **summaries**, not
bucketed histograms. Each histogram below appears on the wire as `<name>{quantile="..."}` series
(quantiles 0, 0.5, 0.9, 0.95, 0.99, 0.999, 1) plus `<name>_sum` and `<name>_count`. That means
p99 is read directly from `{quantile="0.99"}`, there are no `_bucket` series, and quantiles
cannot be re-aggregated across label sets; use `max()` when you need one number across commands
or tenants.

### Query path (RED)

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `deltat_queries_total` | counter | `command`, `status` (`ok`/`error`), `kind`, `tenant` | Request rate and error rate, split by statement type. `kind` is `none` on success, an engine error kind (§3) on engine failure, `other` for non-engine failures, so expected contention is separable from real faults |
| `deltat_query_duration_seconds` | histogram | `command`, `tenant` | Statement latency, parse included. Which command is slow, and for whom |
| `deltat_slow_queries_total` | counter | `command` | How often the `DELTAT_SLOW_QUERY_MS` threshold was crossed, without reading logs |
| `deltat_parse_errors_total` | counter | none | Statements rejected before execution (unparseable SQL, oversized statements). These never reach `deltat_queries_total`, so a broken client generating garbage is only visible here |

The `tenant` label is the sanitized tenant name engines are keyed on, so cardinality is bounded
by the tenant cap, not by raw database-name aliases.

### Errors

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `deltat_engine_errors_total` | counter | `kind` | Engine errors by taxonomy kind (§3). The one to watch: is the error mix healthy contention (`conflict`) or something real (`wal`)? |

### Booking domain

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `deltat_holds_placed_total` | counter | none | Holds accepted by the engine |
| `deltat_holds_committed_total` | counter | none | Holds converted into bookings via `commit_hold` |
| `deltat_holds_released_total` | counter | none | Holds released explicitly by a client |
| `deltat_holds_expired_total` | counter | none | Holds reaped after expiry. Placed minus committed minus released minus expired is the abandonment rate: how often a client takes a slot out of circulation and never comes back |
| `deltat_bookings_created_total` | counter | none | Bookings confirmed (direct inserts, batches, and committed holds) |
| `deltat_bookings_deleted_total` | counter | none | Bookings cancelled |
| `deltat_gc_intervals_collected_total` | counter | none | Past intervals removed by GC. Zero forever means retention is not working and memory grows |

### WAL

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `deltat_wal_flush_duration_seconds` | histogram | none | Group-commit flush latency: the floor under every write's latency |
| `deltat_wal_flush_batch_size` | histogram | none | Events per flush. Rising batch sizes mean the disk is falling behind the write rate |
| `deltat_wal_compaction_duration_seconds` | histogram | none | Compaction runs inline in the writer task, so this duration is latency every write on that tenant queues behind |
| `deltat_wal_errors_total` | counter | `kind` (`append`/`flush`) | Disk-level write failures. Any nonzero rate is an incident |
| `deltat_wal_poisoned` | gauge | `tenant` | 1 while a tenant's WAL is poisoned (every append fails), 0 otherwise. A poisoned tenant rejects all writes until operator intervention |

### Connections and tenants

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `deltat_connections_active` | gauge | none | Current TCP connections, against the `DELTAT_MAX_CONNECTIONS` cap |
| `deltat_connections_total` | counter | none | Connections accepted since start |
| `deltat_connections_rejected_total` | counter | none | Connections turned away at the cap. Nonzero means clients are being refused |
| `deltat_connections_closed_total` | counter | `reason` (`normal`/`error`) | Connection churn and how connections end. `error` covers startup/TLS failures; idle and max-age closes count as `normal` |
| `deltat_connection_duration_seconds` | histogram | none | Connection lifetime. A collapsing median means clients are reconnecting in a loop |
| `deltat_auth_failures_total` | counter | none | Startup failures, bad passwords included. A spike is credential stuffing |
| `deltat_tenants_active` | gauge | none | Loaded engines (one per tenant), against the tenant cap |

### Notifications

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `deltat_notifications_lagged_total` | counter | none | LISTEN notifications a subscriber missed because its channel lagged. A silent drop here means a client's view of a resource is stale with no error anywhere, so nonzero deserves attention |

---

## 3. Error taxonomy

Every engine error carries a stable `kind` label (bounded: one per variant) and crosses the wire
with a real SQLSTATE instead of a catch-all code. The split that matters is **retryable
contention against everything else**:

**SQLSTATE `40001` (serialization_failure) means the caller lost a race and should retry.** It
covers exactly `conflict` (two clients raced for one span) and `capacity_exceeded` (the slot
filled first). PostgreSQL drivers already treat 40001 as "try again"; the caller should pick
another span or retry, not surface a failure. A steady rate of 40001 on a busy resource is the
system working as designed.

**Everything else must not be retried blindly.** Retrying a `23505` or a `42704` returns the
same answer forever; retrying a `58030` hammers a broken disk.

| SQLSTATE | Kind(s) | Meaning | Retry? |
|---|---|---|---|
| `40001` | `conflict`, `capacity_exceeded` | Lost a race for the span, or capacity filled first | Yes: pick another span or retry |
| `23505` | `already_exists` | Duplicate id | No |
| `42704` | `not_found` | Referenced resource/rule/hold/booking does not exist | No |
| `23514` | `not_covered_by_parent`, `closed_by_schedule` | Allocation violates the parent's coverage or a blocking rule | No |
| `23503` | `cycle_detected`, `has_children` | Resource-tree structure violation | No |
| `54000` | `limit_exceeded` | A hard cap was hit (resources, intervals, query window, statement length) | No |
| `58030` | `wal` | WAL append or flush failed: a server fault, not a client mistake | No: page an operator |

Wire-level rejections outside the engine taxonomy: `42601` for unparseable SQL, `22007` for an
inverted time range, `08006` for tenant resolution failures.

---

## 4. What to alert on

Starter expressions, tuned for the summary-style histograms described in §2.

**Non-contention error rate above 1%.** Excludes 40001-class kinds, which are steady-state:

```promql
sum(rate(deltat_queries_total{status="error", kind!~"conflict|capacity_exceeded"}[5m]))
  / sum(rate(deltat_queries_total[5m])) > 0.01
```

**Any WAL trouble.** Both of these are page-worthy, not ticket-worthy:

```promql
rate(deltat_wal_errors_total[5m]) > 0
```

```promql
max(deltat_wal_poisoned) > 0
```

**p99 statement latency.** Quantiles cannot be aggregated, so take the worst series:

```promql
max(deltat_query_duration_seconds{quantile="0.99"}) > 0.25
```

**Clients being refused at the connection cap:**

```promql
rate(deltat_connections_rejected_total[5m]) > 0
```

**Credential stuffing or a misconfigured client:**

```promql
rate(deltat_auth_failures_total[5m]) > 1
```

**Subscribers silently missing notifications:**

```promql
rate(deltat_notifications_lagged_total[5m]) > 0
```

**Hold abandonment ratio** (worth a dashboard panel more than an alert; a rising ratio means
clients hold slots they never commit):

```promql
1 - (
  rate(deltat_holds_committed_total[1h]) + rate(deltat_holds_released_total[1h])
) / rate(deltat_holds_placed_total[1h])
```

---

## 5. Grafana dashboard

[`../grafana/deltat.json`](../grafana/deltat.json) is an importable dashboard covering query rate
and p99 latency, error rate by kind, the hold funnel (placed, committed, released, expired), WAL
flush and compaction timing, and connection churn. Import it via Dashboards > New > Import,
upload the JSON, and pick your Prometheus datasource when prompted.
