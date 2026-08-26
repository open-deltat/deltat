# Changelog

All notable changes to deltat are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/) once the wire/storage format is frozen for 1.0.

## [Unreleased]

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
