# Changelog

All notable changes to deltat are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/) once the wire/storage format is frozen for 1.0.

## [Unreleased]

### Added
- **A refusal now says when, not just no.** When a hold or booking is refused because the span is
  taken, the resource is at capacity, or the time is outside open hours, the same error carries up
  to three spans of the same duration that were free at that instant, in the standard PostgreSQL
  `DETAIL` field as JSON, with a sentence in `HINT`. `psql` prints it as an ordinary `DETAIL:` line
  and every driver already surfaces the field, so this is not a protocol extension. An agent that
  loses a race can counter-offer in the same turn instead of spending a round trip asking what it
  should have asked for, which on a live phone call is the difference between a pause and silence.

  **Nothing is reserved.** An offered span is free for anyone else too, and the payload says
  `"reserved": false` rather than leaving a reader to infer it. Taking one means placing a hold like
  any other caller. Handing out a commitment nobody asked for would be worse than the refusal it
  replaces.

  Offers are computed under the write guard that produced the refusal, at the same instant, so an
  offer can never be the span that was just refused, and every offered span is one the write path
  accepts (asserted by feeding each one straight back in). A run is offered only if it fits the
  request plus the resource's `buffer_after`, because a booking's effective footprint includes its
  turnaround tail. `DELTAT_COUNTER_OFFER=0` disables the feature; a refusal with nothing to say
  emits no `DETAIL` at all, so its absence is a legal state rather than a signal.

  Not covered: batch inserts carry no offers, because N alternatives on a capacity-1 resource all
  collapse onto the same free run and substituting two of them is a guaranteed fresh conflict. The
  honest shape there is a re-plan, which is a different feature.

### Security
- **A conflict no longer names the allocation that won.** `conflict with allocation: <ulid>` became
  `span is already allocated`. The previous message handed a losing caller the identifier of an
  allocation it has no other way to see, so cheap failing `INSERT`s enumerated every allocation on a
  resource the caller cannot read, and `cancel_booking` takes an id and nothing else, so what could
  be enumerated could be cancelled. Audit 2026-08-26, ship-blocker 2, and half of the chain in
  ship-blocker 4. Scoping cancellation to a principal is the other half and is not in this release.
- `ClosedBySchedule` and `NotCoveredByParent` stop rendering `{:?}` over a `Vec<Span>` into the
  error message, which leaked internal struct formatting to any client. They report counts; the
  spans themselves belong in `DETAIL` where a client can parse them.

### Fixed
- **`WHERE` clauses on `bookings` and `holds` reads are honoured instead of silently dropped.**
  `SELECT * FROM bookings WHERE resource_id = 'X' AND start >= 1000 AND "end" <= 2000` used to
  return *every* booking on the resource, with a success code and no warning: the parser collected
  the `resource_id` and ignored the rest of the clause. Span predicates (`start` and `"end"` with
  `<`, `<=`, `>`, `>=`, `=`) now filter as written, with the literal meaning SQL gives them, so
  `start >= A AND "end" <= B` is containment rather than a re-interpreted overlap window.
  Anyone connecting a plain Postgres client, which the README invites, was affected; the
  TypeScript SDK was not, because it windowed client-side precisely to work around this.
- **On `bookings`, `holds` and `rules`, a predicate the parser cannot honour is now an error rather
  than a no-op.** Filtering on a column the engine does not index (`label`, `expires_at`) or with
  an operator it cannot apply returns `unsupported` instead of quietly answering a different
  question. Wrong rows under a success code is the worst failure mode a database has, and the
  silent catch-alls on those three reads are gone. `rules` had the same bug as `bookings`:
  `WHERE resource_id = 'X' AND start >= 99999` returned every rule on the resource.

  **Now covered too** (#37, below): `availability`, and the clauses no read honoured.

- **`availability` refuses a filter it cannot honour, and repeated bounds intersect.** Three
  separate paths used to discard part of the clause and answer anyway. The sharpest was
  `min_duration > 500`: a recognised column with an operator the walk skipped, so the caller was
  handed slots too short to book. `label = 'x'`, `BETWEEN`, `NOT (...)`, `IN` on anything but
  `resource_id`, and `IS NULL` were all dropped the same way, and a parenthesised clause such as
  `WHERE (resource_id = 'X' AND start >= 1000)` vanished entirely, because `Expr::Nested` had no arm.

  Repeated bounds were resolved last-write-wins rather than intersected, so
  `start >= 1500 AND start >= 1000` yielded `start = 1000`. That is **wider** than asked for, which
  on an availability read means offering time the caller explicitly excluded. `AND` means both
  bounds hold; they now intersect regardless of the order they appear in.

  `min_available` and `min_duration` are query parameters wearing a predicate's clothes rather than
  filters on returned columns, so the accepted set is written out explicitly instead of inferred.

- **A read refuses `ORDER BY`, `LIMIT`, `OFFSET`, a column list, `DISTINCT`, `GROUP BY`, `HAVING`,
  `JOIN`, CTEs and row locks** instead of parsing and discarding them. `LIMIT 200` returned every
  row, `ORDER BY start DESC` returned rows in store order, and `SELECT start` returned all columns.

  Refusing is the reversible direction: a caller who needs `LIMIT` can be given it later without
  breaking anyone, while a caller that has silently been receiving unlimited rows cannot be
  un-broken once it depends on them. Nothing in this repo or the SDK sends any of these, and GUI
  data browsers cannot connect regardless, since there is no `pg_catalog` to introspect.

### Security
- **`UPDATE` refuses a column the table does not have, instead of reporting a write that never
  happened.** `UPDATE resources SET capcity = 5 WHERE id = 'X'` (a typo) replied `UPDATE 1`, changed
  no field, and still appended a no-op `ResourceUpdated` record to the WAL. The same held for
  `rules` and `holds`. A write that reports success and does nothing is worse than a wrong read,
  because the caller has no reason to look again. Refusals name the assignable columns, since the
  overwhelmingly likely cause is a misspelling.

- **Reusing an entity id no longer strands an interval.** `INSERT INTO holds`, `INSERT INTO
  bookings` and `INSERT INTO rules` now reject an id that is already in use, anywhere in the
  tenant, with SQLSTATE `23505`. Previously nothing caught a reuse: the conflict check skips
  expired holds and never runs at all when the new span does not overlap, and the interval store
  did not deduplicate. A client retrying with the same id, which is ordinary behaviour after a
  timeout, could therefore create a second interval carrying that id. Removing one copy unmapped
  the id, leaving the other unreachable by release, commit, cancel or the reaper: it kept
  occupying a slot against the per-resource interval cap and came back on every replay. If you
  were relying on a re-`INSERT` silently succeeding, mint a fresh id instead.
- **The same rejection now covers `commit_hold` and multi-row `INSERT INTO bookings`,** which the
  first pass missed. The guard was wired into three of the five write paths, so the identical
  stranding was still reachable through `UPDATE holds SET booking_id = $1 WHERE id = $2` (the SDK's
  own commit path, and the one an agent uses most) and through a batch insert. A batch is also
  checked against itself: two rows sharing one id are rejected, which no amount of inspecting
  existing state can catch because neither row exists yet. Rejection happens before the WAL append,
  so a refused commit leaves the hold live and a retry with a fresh booking id still has something
  to commit.

### Changed
- **Breaking, in the honest direction:** a read whose `WHERE` clause previously "worked" by having
  part of itself discarded now fails with `unsupported`. Any caller relying on that silence was
  already receiving rows that did not match what it asked for.
- The WAL format version is read back and an older log is migrated in full on open, rather than
  the version being parsed and discarded (#33). A mixed-version log, which an older binary would
  have truncated at the tail and thereby dropped an acknowledged booking, can no longer exist.

## [0.3.0] - 2026-08-26

The observability release, plus the two things that make a published container safe to upgrade
into: a versioned storage format and an automated image pipeline.

### Added
- The WAL carries a format version. Records are bincode and carry no schema of their own, so a
  future format change would previously have been read as garbage; a binary now refuses to open a
  log written by a newer deltat and says so. Logs written before this release have no header, are
  still read exactly as they are, and gain one the first time they are compacted.
- Official container images at `ghcr.io/open-deltat/deltat`, built for `linux/amd64` and
  `linux/arm64` and published automatically when a `v*` tag is pushed. `docker-compose.yml` now
  runs the published image against a named volume, so upgrading is `docker compose pull` and your
  data stays put; `docker-compose.build.yml` is the overlay for building from a checkout instead.
- `RELEASING.md` documents the release runbook, including what to do when the storage format
  changes.
- The `/metrics` endpoint (`DELTAT_METRICS_PORT`) now reports what the server is actually doing:
  per-command query rates and latencies labelled by tenant with error kinds split out, connection
  churn and close reasons, WAL flush and compaction timing, a per-tenant poisoned-WAL gauge, the
  hold funnel (placed, committed, released, expired), booking and GC counters, rejected statements,
  and dropped LISTEN notifications. Documented metric by metric in `docs/OBSERVABILITY.md`, with an
  importable Grafana dashboard in `grafana/deltat.json`.
- Machine-readable logs: `DELTAT_LOG_FORMAT=json` switches output to newline-delimited JSON for log
  collectors, `RUST_LOG` filtering is honored and documented, and a panic in a connection task is
  routed through the log stream instead of dying on stderr outside it.
- A slow-query log: statements at or over `DELTAT_SLOW_QUERY_MS` are logged at `warn` with the
  command and tenant, never the statement text, and counted, so a latency regression names its
  culprit. This is the `log_min_duration_statement` equivalent Postgres operators expect.

### Changed
- Errors cross the wire with real SQLSTATEs instead of a catch-all `P0001`. Retryable contention (a
  lost race for a span, or capacity filling first) reports `40001`, the code PostgreSQL drivers
  already treat as "retry"; client mistakes and server faults report their own codes, so a caller
  can branch on the class of failure instead of parsing message text.

All of it stays off by default and is switchable independently: no `DELTAT_METRICS_PORT` means no
endpoint and no per-query recording at all, and logs stay human-readable unless asked otherwise.

## [0.2.0] - 2026-08-26

The security and durability release. A full-repo audit produced 24 confirmed findings across
deltat and its SDK; all 17 deltat findings are closed here, each with a regression test written
red first.

### Added
- `commit_hold` is reachable over the wire as `UPDATE holds SET booking_id = $1 [, label = $2]
  WHERE id = $3`, replying `UPDATE 1`. Converting a hold into a booking is now one atomic
  statement, closing the race where a competing booker could steal the span between a release
  and an insert. Documented in `docs/FORMAT.md`.
- `DELTAT_MAX_HOLD_TTL_MS` (default 1 hour) caps hold lifetime; `place_hold` clamps a requested
  `expires_at` to the server clock plus this ceiling, so a skewed or hostile client clock can no
  longer park a hold the reaper never releases.
- `DELTAT_TENANT_PASSWORDS` takes comma-separated `tenant:password` pairs. A tenant with an entry
  accepts only its own password; `DELTAT_PASSWORD` covers the rest. Malformed or duplicate entries
  fail startup rather than silently weakening auth.
- Integration coverage at the protocol seams: the extended query path (Parse/Bind/Describe/Execute,
  the surface Bun SQL and postgres.js use), wrong-password rejection, and database-name to tenant
  isolation, each driven through a real client socket.
- Property and fuzz tests for the availability read path and the SQL parameter boundary, a stateful
  capacity property, multi-resource sweep and corrupt-store tests, and end-to-end pgwire tests for
  the hardened paths. CI also runs the release profile.

### Security
- An unset `DELTAT_PASSWORD` now generates a random 160-bit password printed once at startup
  instead of defaulting to the known string `deltat` on `0.0.0.0` without TLS. `docker-compose`
  refuses to start without an explicit password. `SECURITY.md` documents the trust model.
- Password comparison is constant time. The previous handler short-circuited on byte equality,
  leaking length and prefix-match timing for the credential that is the whole security boundary.
- `$N` parameter indices are capped at 65535 (the Bind-message ceiling). An 18-byte statement such
  as `SELECT $9999999999` previously sized an allocation from the raw index and aborted the whole
  multi-tenant process.
- The availability read path no longer overflows `i64` on untrusted query bounds, `$N`
  substitution no longer overflows `usize` on a long digit run, WAL replay rejects an implausible
  length prefix before allocating, negative `min_available` is rejected at the SQL boundary, and
  the shared password is redacted from `DeltaTAuthSource`'s `Debug`.

### Fixed
- A torn or corrupt record at the WAL tail no longer poisons the log: `Wal::open` truncates back
  to the last good record boundary, so writes acknowledged after a crash survive later replays
  instead of vanishing behind the tear. Corruption in the middle of the log is now a hard error
  rather than a silent stop.
- Events acknowledged during the compaction window are carried into the compacted file. The writer
  opens a recording window on `CompactBegin` and appends recorded events to the snapshot before the
  rename, so the swap stays atomic and an acknowledged write is no longer erased by a stale
  snapshot.
- The parent directory is fsynced after the compaction rename. Without it a power loss could
  resurrect the pre-compaction inode and lose every record acknowledged since the swap.
- The write path honors rules (T-03, recorded as AVAIL-16). Admission previously weighed
  allocations only, so a booking at an instant the read path reports closed or blocked was accepted
  and durably committed. `place_hold`, `confirm_booking`, and every batch member now check the
  effective open windows, rejecting with the new `ClosedBySchedule` error.
- The non-blocking OVERRIDE is window-independent. The own-versus-inherited base decision keyed on
  whether a rule overlapped the query window, so the same instant read open in a narrow query and
  closed in a wide one.
- Hierarchy DDL is serialized under a tenant topology mutex. A concurrent create and delete of the
  parent could durably commit a child whose every availability query errors `NotFound`. Replay
  sweeps pre-existing WALs for orphans and detaches them to roots.
- `list_resources` awaits read locks instead of skipping rows on `try_read` failure, which under
  ordinary write load returned an incomplete list with a success status.
- GC retains a booking until its `buffer_after` tail passes the cutoff, so a slot rejected at 15:00
  is no longer accepted at 15:01 after a reaper cycle.
- `check_batch_capacity` no longer panics at capacity `u32::MAX`, and capacity `0` is rejected at
  create and update instead of silently behaving as `1` (documented under MODEL-03).
- `INSERT INTO bookings` resolves every field by the declared column list. Reordering `id` and
  `resource_id` previously swapped them silently, landing a booking on the wrong resource with
  success reported.
- Multi-row `INSERT INTO holds` is rejected rather than truncated to the first row and reported as
  success.
- `delete_resource` no longer panics on a TOCTOU unwrap and reclaims its notify channel; WAL
  compaction awaits a read lock rather than `try_read().expect()`; the GC cutoff subtracts
  saturating and a negative `DELTAT_GC_RETENTION_MS` is clamped at parse time.

### Changed
- `schema_for_sql` derives the Describe schema from the parsed SQL AST instead of scanning text.
- The 6466-line `src/engine/tests.rs` is split into banner-aligned submodules under
  `src/engine/tests/` with a shared helpers module. Pure mechanical move: 433 lib tests before and
  after, identical leaf inventories.
- Test helpers namespace their temp directories per process, so parallel `cargo test` runs no
  longer delete and replay each other's WAL files.
- Removed the orphaned duplicate TypeScript client and the unused `VERSION` file; added crate
  metadata and corrected the README architecture, env, and demo tables.
