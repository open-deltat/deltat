# OSS-Hardening Session — Handoff

> Overnight pass to fix the security and OSS-readiness findings from `REVIEW-2026-06-22.md`, to a
> public-domain quality bar (spec-aligned, precise naming, minimal comments, no AI tells, TDD).
> All work is isolated in dedicated worktrees on `fix/oss-hardening`; your active checkouts were not
> touched (deltat `feat/clock-seam` clean; tap `feat/demo-v2` WIP intact).

## Branches

| Repo | Worktree | Branch | Based on |
|---|---|---|---|
| deltat | `../deltat-fixes` | `fix/oss-hardening` | `feat/clock-seam` @ 05086f48 |
| tap | `../tap-fixes` | `fix/oss-hardening` | `feat/demo-v2` @ d1c7bde |

Branched off the feature tips (not `main`) because that is where the reviewed code lives. Retarget
the merge wherever you prefer.

## deltat — what changed (1 commit)

Verified: `326` lib tests (dev + release profile) + `17` integration tests pass, `clippy -D warnings`
clean, ambient-time gate passes.

Security / reachable panics (errors as values, PRIN-08):
- **Availability read path no longer panics on i64 overflow** from untrusted query bounds
  (`engine/queries.rs`): saturating width check + an inverted/empty guard on both the single- and
  multi-resource paths. This was a new blocker (same class as the write-path DoS already fixed).
- **WAL compaction no longer panics under contention** (`engine/mutations.rs`): it snapshotted each
  resource via `try_read().expect()`; a resource mid-mutation made that panic and skipping it would
  drop the resource from the rewritten WAL. Now it awaits a read lock, snapshots, and emits
  ancestors-first by tree depth.
- `delete_resource` resolves through `Option` (no TOCTOU unwrap) and reclaims its notify channel.
- `gc_past_intervals` cutoff uses `saturating_sub` (operator-set retention is unbounded).
- Reject negative `min_available` at the SQL boundary; engine also short-circuits a threshold above
  the resource count.

Cleanup for public release:
- Deleted the orphaned duplicate TypeScript client (`client/`) and the unreferenced `VERSION` file.
- Redacted the shared password from `DeltaTAuthSource`'s `Debug`.
- Removed stale `#[allow(dead_code)]` on the now-wired notify channel API.
- `schema_for_sql` derives the Describe schema from the parsed SQL AST instead of scanning the text.
- `Cargo.toml` package metadata; README env/architecture/demos accuracy; corrected `REQUIREMENTS`
  TEST-12 (proptest is present); CI now runs the suite under the release profile too.

Tests (red→green, with a mutation check on the headline overflow fix): regressions for the overflow
and underflow panics, locked-resource compaction, notify reclamation, and a same-resource
capacity-1 booking race that admits exactly one.

## tap — what changed (1 commit)

Verified: SDK builds, `18` SDK tests pass, calendar + demo typecheck clean.

Calendar (security, on files you were not editing):
- **Production fail-fast** when `CAL_SECRET` / `CAL_PASS` / `DELTAT_PASSWORD` are unset or still the
  in-repo dev default (the published HMAC secret allowed a forgeable session cookie). Checked at
  request time so a build without runtime env still succeeds.
- Constant-time compare for the session signature and the login password.
- `cancelBooking` verifies the booking belongs to the calendar resource before cancelling.

SDK / demo (cleanup):
- Dropped the stale `ScheduleSet` / `ScheduleRemoved` `DeltaTEvent` variants (zero consumers; the
  kernel no longer emits them).
- Single source for the visitor cookie name + TTL (was duplicated).
- README notes pgwire is transitional.

## Deferred (with reasons) — your call in the morning

- **tap: the dead `Schedules` class removal (full H3).** The committed `calendar/app/actions/setup.ts`
  still calls `dt.schedules.set/get`, and the demo seeds use the mask/time helpers, so removing the
  class now would break the build. Your in-progress `setup.ts` appears to be migrating off it; once
  that lands, deleting the class + helpers + `Schedule` type + `schedules.test.ts` is a clean
  follow-up (the stale event variants are already gone).
- **tap: `calendar/server.ts` hardening (B1), `createPublicBooking` reconciliation (H4),
  `setup.ts` dual-write (M7), `schedule-store.ts` validation (L7).** All are your active uncommitted
  WIP — left untouched on purpose.
- **tap: collapse the 13 demo route shims (M10), `demo/lib/store.ts` duplicated state (L9), scattered
  `!` in demo seed code (L10).** Structural/cosmetic in the demo; deferred because they want a
  browser run to verify, not just a typecheck.
- **deltat: `label → external_ref` (M2/GAP-02), monotonic-clock split (M3/GAP-11), `CommitHold`
  atomicity (M4/AVAIL-07), blocking-rule conflict decision (M5/T-03).** Spec-tracked design changes,
  explicitly out of the cleanup scope; the spec already records them.

## Adversarial self-review (before push)

Both diffs were reviewed by an independent multi-agent pass. It caught one real regression, now
fixed and locked with a test, plus minor items, all addressed:
- **Regression (fixed):** the `schema_for_sql` AST rewrite matched table names case-sensitively,
  while the execution path lowercases identifiers, so a Describe of `BOOKINGS` returned empty column
  metadata. Now case-folded, with a `schema_for_select_is_case_insensitive` test.
- Corrected the compact_wal comment (replay tolerates any order; the depth-sort matches the live
  create order rather than being required to avoid orphan rejection).
- Strengthened the GC saturation test to also assert a normal retention still collects.
- Clamped a negative `DELTAT_GC_RETENTION_MS` at parse time.
- Reverted a dedup of duplicate resource ids in multi-availability: counting a twice-listed id
  twice is intentional and tested (`multi_avail_duplicate_resource_id`).

## How to review

```
git -C ../deltat-fixes show 700d953f        # deltat diff
git -C ../tap-fixes   show 4c3317d          # tap diff
```

## Rounds 2-4 — audit-driven hardening + ratings

After round 1, an independent multi-agent audit graded the work and was asked to drive every
dimension to at least A (ideally S). It found real bugs round 1 missed, which rounds 2-4 fixed, then
re-graded (HEAD-pinned to the worktrees).

**Final ratings (every dimension is now at least A; 3 reached S):**

| Area | Dimension | round 1 | final |
|---|---|---|---|
| deltat | Correctness | A | **S** |
| deltat | Security / untrusted-input | A | **S** |
| deltat | Concurrency-safety | A | A |
| deltat | Test-rigor | A | **S** |
| deltat | Code-quality | A | A |
| tap | Security | A | A |
| tap | Correctness | A | A |
| tap | Test-rigor | B | **A** |
| tap | Code-quality | A | A |
| tap | Publishability | B | **A** |

**Real bugs the deeper audit surfaced and rounds 2-4 closed:**
- `substitute_params` ($N extended-query substitution) overflowed `usize` on a long digit run
  (`$99999999999999999999`) and panicked the connection task. Fixed via a shared checked
  `parse_param_index`; both it and `count_params` now use it.
- WAL replay allocated from an untrusted u32 length prefix before the CRC check (~4 GiB from a
  corrupt prefix) and bincode admitted inverted spans. Now length-guarded before allocation and
  span-validated on load (`Event::spans_valid`).
- `parse_i64_expr` negated with unchecked `-` (latent i64::MIN footgun); now `checked_neg`.
- tap login compared the username with a short-circuiting `||` that skipped the constant-time
  password compare on a wrong username (timing leak); both compares are now unconditional.
- A CI-skipped test (`interval_limit_hold`) used an out-of-range expiry and **failed whenever run**;
  fixed, so the full suite now passes with zero skips.

**Test depth added:** no-panic fuzz of the availability read path and the `parse_sql` / `count_params`
boundary; a stateful capacity property (INV-01 through the command path); a multi-resource sweep
fuzz; a corrupt-store over-deep-hierarchy test; e2e pgwire tests for the fixed queries; unit tests
for the WAL guards, `parse_param_index`, `constantTimeEqual`, `assertProductionSecrets`, and
`credentialsMatch` (with a spy locking the timing fix). deltat: **342 lib + 19 integration, clippy
clean, full suite no skips**. tap: **SDK 18 + calendar 7, both apps typecheck clean**.

**Mutation testing (deltat conflict.rs + queries.rs):** the honest final `cargo-mutants` run tested
**103 mutants: 82 caught, 5 survivors, 16 unviable** (my earlier partial "19/0" was misleading and is
corrected here). The restructure removed 2 sites (the dead default) and the new corrupt-store test
killed the depth-increment mutant. The 5 survivors are: 2 equivalent (the `compute_multi_availability`
short-circuit guard at queries.rs:143 is a pure optimization — output is identical without it), 2
immaterial depth-threshold boundary mutants (queries.rs:34, the defensive backstop's exact cutoff
does not matter), and 1 narrow zero-width-segment guard (queries.rs:173) the fuzz did not happen to
trigger — a small, known, reachable-later gap.

**What still caps S (honest):**
- deltat Concurrency-safety (A): correct by lock construction but no loom/adversarial-interleave
  harness yet. Reachable later.
- deltat Code-quality (A): the `Span::new` (panicking) vs `try_new` (fallible) split remains
  (spec item TIME-05, a deliberate kernel migration). Reachable later.
- tap Security / Correctness / Code-quality / Publishability (A, not S): capped by your active WIP —
  the dead `Schedules` class/type is still exported and `calendar/server.ts` is your untracked WIP.
  Once you finish that removal, those can reach S.
- tap Test-rigor (A): an end-to-end login-action-boundary test would push it to S.

## How to verify locally

```
# deltat
cd ../deltat-fixes && cargo test --lib -- --skip create_resource_too_many --skip interval_limit \
  && cargo clippy --all-targets -- -D warnings && cargo test --test listen_notify
# tap
cd ../tap-fixes && bun install && (cd packages/client && bun run build && bun test) \
  && (cd calendar && bunx tsc --noEmit) && (cd demo && bunx tsc --noEmit)
```
