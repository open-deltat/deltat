# Changelog

All notable changes to deltat are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/) once the wire/storage format is frozen for 1.0.

## [Unreleased]

### Security
- Availability read path no longer overflows `i64` on untrusted query bounds (saturating width plus
  inverted-window guards on the single- and multi-resource paths).
- Extended-query `$N` substitution no longer overflows `usize` on a long digit run; a shared checked
  `parse_param_index` is used by both the substitution and the parameter count.
- WAL replay rejects an implausible length prefix before allocating, and rejects records whose span
  is inverted on load.
- Negative `min_available` is rejected at the SQL boundary; integer parsing uses `checked_neg`.
- The shared password is redacted from `DeltaTAuthSource`'s `Debug`.

### Fixed
- A torn or corrupt record at the WAL tail no longer poisons the log: `Wal::open` truncates back
  to the last good record boundary, so writes acknowledged after a crash survive later replays
  instead of silently vanishing behind the tear. Corruption in the middle of the log (a bad record
  followed by valid data) is now a hard error instead of a silent stop. A failed flush recovers the
  tail (or poisons the WAL so appends keep failing) rather than appending acknowledged records
  behind partial bytes.
- `delete_resource` no longer panics on a TOCTOU unwrap and now reclaims its notify channel.
- WAL compaction awaits a read lock instead of `try_read().expect()`, so a mid-mutation resource can
  no longer panic the compactor or be dropped from the rewritten WAL.
- GC cutoff subtracts saturating; a negative `DELTAT_GC_RETENTION_MS` is clamped at parse time.
- Corrected a CI-skipped test that failed whenever run; the full suite now passes with no skips.

### Changed
- Errors now cross the wire with real SQLSTATEs instead of a catch-all `P0001`. Retryable
  contention (a lost race for a span, or capacity filling first) reports `40001`, the code
  PostgreSQL drivers already treat as "retry"; client mistakes and server faults report their own
  codes, so a caller can branch on the class of failure instead of parsing message text.
- `schema_for_sql` derives the Describe schema from the parsed SQL AST instead of scanning the text.
- Removed the orphaned duplicate TypeScript client and the unused `VERSION` file.
- Added crate metadata; README architecture, env, and demo tables corrected.

### Added
- Property and fuzz tests for the availability read path and the SQL/parameter boundary, a stateful
  capacity property, multi-resource sweep and corrupt-store tests, and end-to-end pgwire tests for
  the hardened paths. CI now also runs the release profile.
- The `/metrics` endpoint (`DELTAT_METRICS_PORT`) now tells an operator what the server is doing:
  per-command query rates and latencies with error kinds split out, connection churn and auth
  failures, WAL flush/compaction timing and a per-tenant poisoned-WAL gauge, the hold funnel
  (placed, committed, released, expired), booking and GC counters, and dropped LISTEN
  notifications. Documented metric by metric in `docs/OBSERVABILITY.md`, with a ready-made
  Grafana dashboard in `grafana/deltat.json`.
- Machine-readable logs: `DELTAT_LOG_FORMAT=json` switches output to newline-delimited JSON for
  log collectors, and panics in connection tasks are routed through the log stream instead of
  dying silently on stderr.
- A slow-query log: statements at or over `DELTAT_SLOW_QUERY_MS` are logged at `warn` (command
  and tenant, never the statement text) and counted, so a latency regression names its culprit.
