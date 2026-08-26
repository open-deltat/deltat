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
- `schema_for_sql` derives the Describe schema from the parsed SQL AST instead of scanning the text.
- Removed the orphaned duplicate TypeScript client and the unused `VERSION` file.
- Added crate metadata; README architecture, env, and demo tables corrected.

### Added
- Property and fuzz tests for the availability read path and the SQL/parameter boundary, a stateful
  capacity property, multi-resource sweep and corrupt-store tests, and end-to-end pgwire tests for
  the hardened paths. CI now also runs the release profile.
