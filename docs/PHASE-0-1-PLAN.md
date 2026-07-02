# deltat v2: Phase 0 & 1 executable build plan

> Companion to [`../V2-DESIGN.md`](../V2-DESIGN.md). This is the *code-grounded* plan: exact files,
> verified facts, ordered PRs, CI gates. Produced by agents reading the real source, where a design
> assumption was wrong, the verified number is used and the correction is noted.
>
> **Goal of these two phases:** make deltat *testable for real* (Phase 0), then fix the two correctness
> bugs and prove it with a *seed, not a hand-written ordering* (Phase 1). This is the foundation that
> stops v2 repeating v1's "passed tests but felt off."

---

## Fact corrections from the source audit (use these, not the design's estimates)

- **The "~77 nondeterminism sites" was a large overcount.** Verified production sites:
  - **3** `SystemTime::now()` (one in `conflict.rs:6-11 now_ms()`, two in `reaper.rs:13,35`).
  - **4** `now_ms()` callers (`mutations.rs:149,182,229`, `queries.rs:104`).
  - **0** server-side `Ulid::new()`: all **589** `Ulid::new()` calls are inside `#[cfg(test)]`; production IDs are **client-supplied** via `parse_ulid`.
  - **0** direct RNG in `src/` (only transitive via `ulid`).
  - `Instant::now()` appears 3× but is **latency-metrics only** and never feeds engine state; leave it.
  - **The determinism seam is ~7 surgical edits, not 77.** State this in the PR so the work isn't padded (and so nobody "fixes" 77 sites by wrongly touching test code).
- **`Command` enum is 20 engine variants** (`sql.rs:11-113`), not serde-derived today (`#[derive(Debug, PartialEq)]`). `Event` and all sub-types *are* serde.
- **The bug is real and exact:** `confirm_booking` (`mutations.rs:162-186`) takes no `hold_id`, runs a fresh `check_no_conflict`, inserts a `Booking`. There is **no `CommitHold` anywhere** (grep: zero hits).
- **The one "concurrency" test** (`tests.rs:3699`) spawns 20 tasks each on a *different* resource → structurally cannot see same-slot contention. `Cargo.toml` has **zero** property/sim/mutation deps.

---

## Decisions locked by the audit (contradictions resolved)

| Decision | Resolution | Why |
|---|---|---|
| Where does `Clock` live? | **`src/clock.rs` at crate root** (not `src/engine/clock.rs`) | `lib.rs` uses flat `pub mod`; `reaper.rs` (not under `engine/`) consumes `Clock` too. |
| Wire `IdGen`/`Rng` now? | **No: declare the `Clock` trait only.** | Production mints **zero** server IDs and uses no RNG. Wiring them is the speculative generality the design forbids. Add `IdGen` when a phase has a real server-minting caller (Phase 1 idempotency uses the *client* id; Phase 5 nonces). |
| Sim framework? | **In-process seeded poll-order executor**, not `madsim`/`turmoil`. | Single-node contention needs actor interleaving, not host+network modelling. Defer `turmoil` to Phase 5 federation. |
| `CommitHold` idempotency key? | **Key on `booking_id` alone**; any pre-existing `Booking` with that id → `Ok` echo. | The sub-plan's `existing.span == hold.span` sketch is circular (hold span is gone on retry). |
| Hold-capability auth (Open Q1) | **Recommended: possession of the `Ulid` `hold_id` authorizes commit**, safe *only* while every connection is tenant-authenticated and `hold_id`s aren't cross-tenant enumerable. **Write the capability-token upgrade trigger into `V2-DESIGN.md` §9 Q1 now.** | Becomes a slot-hijack vector the moment a `hold_id` crosses a trust boundary (Phase 1.5 payments, Phase 5 federation). **Needs your sign-off before Phase 1 step 12.** |

---

## The #1 risk: FALSE GREEN (this is why Phase 0 exists)

If the multi-actor sim `await`s each op to completion **without a yield between `release_hold` and
`confirm_booking`**, the per-resource `RwLock` serializes every interleaving into sequential order and
the TOCTOU bug is **invisible**, exactly how v1 shipped 178 green tests over a live double-book. The
scheduler **must** switch actors at the `await` boundary and assert `never_double_book` **globally**
after every committed event. **Prove it by RED-ing on the current code (step 9) before writing any fix.**

Two more traps:
- **Oracle contamination:** `src/spec/invariants.rs` must import **only `crate::model` + `std`**, never
  `engine::availability`/`conflict`. The instant someone "DRYs it up" by calling the engine sweep, the
  checker re-encodes the engine's bug and judges it correct. A **CI grep-gate** failing on any
  `availability|conflict` import in `src/spec/` is the only real defense.
- **Lint blind spot:** clippy `disallowed-methods` is path-based and **misses the fully-qualified
  `std::time::SystemTime::now()`** form `reaper.rs:13,35` use. The **grep-gate is mandatory**, not
  belt-and-suspenders.
- **`tokio::time` ≠ `Clock`:** the reaper's intervals/sleeps are *scheduled* time, a distinct seam from
  business `now`. The in-process executor sidesteps this for Phase 0; document it so nobody tries to
  hide `tokio::time` behind `Clock` or gets a false-green sim where the reaper fires on wall time.

---

## The seam (PR1 keystone)

```rust
// src/clock.rs: the ONLY file allowed to call the banned primitives.
#![allow(clippy::disallowed_methods)]
use crate::model::Ms;

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> Ms;            // business `now`: feeds conflict checks + hold expiry
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> Ms {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as Ms
    }
}

// test-only
use std::sync::atomic::{AtomicI64, Ordering};
pub struct TestClock(AtomicI64);
impl TestClock {
    pub fn new(t: Ms) -> Self { Self(AtomicI64::new(t)) }
    pub fn advance(&self, by: Ms) { self.0.fetch_add(by, Ordering::SeqCst); }
    pub fn set(&self, t: Ms) { self.0.store(t, Ordering::SeqCst); }
}
impl Clock for TestClock { fn now_ms(&self) -> Ms { self.0.load(Ordering::SeqCst) } }
```

```rust
// src/engine/mod.rs: constructor injection through the only Engine ctor
pub struct Engine {
    pub(super) store: InMemoryStore,
    pub(super) wal_tx: mpsc::Sender<WalCommand>,
    pub notify: Arc<NotifyHub>,
    pub(super) clock: Arc<dyn crate::clock::Clock>,   // NEW: the seam
}
```

`SystemClock` defaults in `main.rs`; threads through `Engine::new` and `TenantManager::new`/
`get_or_create`. **No call-site or behaviour change in PR1**: all 178 tests stay green because nothing
reads the field yet.

---

## The executable spec (PR2)

```rust
// src/spec/invariants.rs: HAND-WRITTEN oracle. Imports ONLY crate::model + std. NEVER engine fns.
use crate::model::{Ms, Span, IntervalKind, ResourceState};

/// Effective span of an active allocation: end extended by buffer_after. Hand-derived, NOT from availability.rs.
fn effective_active(rs: &ResourceState, now: Ms) -> Vec<Span> {
    let buf = rs.buffer_after.unwrap_or(0);
    rs.intervals.iter().filter_map(|i| match &i.kind {
        IntervalKind::Booking { .. } => Some(Span { start: i.span.start, end: i.span.end + buf }),
        IntervalKind::Hold { expires_at } if *expires_at > now =>
            Some(Span { start: i.span.start, end: i.span.end + buf }),
        _ => None,
    }).collect()
}

/// ∀ instant: count(active overlapping) ≤ capacity. Dumb O(n²) point-sweep on purpose; no temptation to
/// call the clever engine sweep.
pub fn never_double_book(rs: &ResourceState, now: Ms) -> Result<(), Violation> { /* ... */ }
```

Plus the **dumb reference model** (`tests/common/reference.rs`: `HashMap<Ulid, RefResource>` with O(n)
admission scan) run **alongside** the real engine on identical `Cmd` sequences, asserting accept/reject
parity + state equality + `assert_all_invariants` **after every op**. The `Cmd` enum **includes
`CommitHold` now** so the harness exists *before* the Phase-1 fix and can RED on a naive implementation.

Compare on `mem::discriminant` (`EngineError` has no `PartialEq`); run harnesses as **integration tests
under `tests/`** to inherit production limits (`#[cfg(test)]` caps `MAX_INTERVALS=200`).

> **Multi-resource correction (do not skip: this is exactly v1's blind spot one level up).** A
> single-resource `HashMap<Ulid, RefResource>` reference **cannot** judge hierarchy, capacity-across-a-
> section, or cross-resource atomicity: the very things the stadium / plane / zero-sum cases need. So:
> - **Reference is a whole-tenant `RefWorld`** (`BTreeMap`s for `resources`/`parent`/`children`/
>   `entity_to_resource`, deterministic for byte-comparison). `RefWorld::apply` implements
>   `batch_confirm` *and* `commit_hold` as **atomic transactions** (validate the whole batch against a
>   clone, mutate only if all pass), mirroring the engine's lock-all/validate-all/commit-all.
> - **`Cmd` enum is multi-resource + hierarchy-aware**: `CreateResource{parent_id?}`, `AddRule`,
>   `BatchConfirm{bookings: Vec<(…, resource_from_LIVE_set, …)>}`, `CommitHold{hold_id, booking_id}`.
>   Generators draw resource ids from the **live set**, target the **same span across same and different
>   resources**, build small 1-3 level trees so inheritance fires, and mix conflicting+free slots so
>   rejection fires.
> - **Global/multi-resource invariants** (hand-written, model+std only; extend the grep-gate to also
>   forbid `queries`/`engine` imports): `never_over_capacity(all, now)` over **every** resource;
>   `batch_atomicity` (Err ⇒ *every* resource byte-identical incl. indexes; Ok ⇒ exactly the batch
>   applied); `zero_sum_hold_commit` + `both_or_neither` (a `CommitHold` conserves count and lands the
>   booking on exactly its resource; coupled resources both-or-neither); `hierarchy_inheritance` (a
>   model-side ancestor-walking `ref_availability` equals `compute_availability`); `no_side_effects_on_
>   rejection` for *single* ops too; `multi_avail_parity`. Compare the **full** per-resource interval set
>   for *all* resources, not just the op's target, so stray sibling/ancestor mutations are caught.
> - **PR3 sim adds three scenarios beyond the cap-1 slot**, each RED-first with a captured seed:
>   (1) N actors racing the **same multi-resource batch** (yield *between the two lock acquisitions* and
>   between validate and commit: assert no half-applied batch); (2) a **hold→commit zero-sum race**;
>   (3) a **capacity-across-section race** (cap `K−1`, with a `buffer_after>0` variant). Same
>   STOP-if-cannot-RED guard: if a multi-resource scenario won't go red even with a deliberately broken
>   batch commit, the scheduler isn't yielding *inside* `batch_confirm_bookings`.
> - Add the missing `batch_confirm_bookings` atomicity unit tests; today's 4 batch tests are all
>   single-resource or short-circuit before locking.

---

## The Phase-1 fix

```rust
// src/model.rs: new self-describing Event variant (NEVER reuse a discriminant, 100yr-format rule)
Event::HoldCommitted { hold_id: Ulid, booking_id: Ulid, resource_id: Ulid, span: Span, label: Option<String> }
```

```rust
// src/engine/store.rs: apply_event arm: the atomic transfer, one funnel for replay + live
Event::HoldCommitted { hold_id, booking_id, resource_id, span, label } => {
    rs.remove_interval(*hold_id);
    self.unmap_entity(hold_id);
    rs.insert_interval(Interval { id: *booking_id, span: *span,
        kind: IntervalKind::Booking { label: label.clone() } });
    self.map_entity(*booking_id, *resource_id);
}
```

```rust
// src/engine/conflict.rs: exclude the hold being committed (else at cap 1 it self-conflicts)
pub(crate) fn check_no_conflict(rs: &ResourceState, span: &Span, now: Ms, exclude: Option<Ulid>) -> Result<(), EngineError>
// 3 legacy callers pass None. For cap>1, apply `exclude` in collect_active_allocs_with_buffer BEFORE the
// alloc is pushed, or the saturated-span math still counts the hold.
```

```rust
// src/engine/mutations.rs: Bug-2 fix lives IN the clock seam: authority assigns expiry, client cannot
pub async fn place_hold(&self, id: Ulid, resource_id: Ulid, span: Span, ttl: Option<Ms>) -> Result<Ms, EngineError> {
    let now = self.clock.now_ms();
    let expires_at = now + ttl.unwrap_or(DEFAULT_HOLD_TTL_MS).clamp(MIN_TTL, MAX_TTL); // returned opaque to client
    // ...same clock that set expiry is used in check_no_conflict → no skew
}
// commit_hold(hold_id, booking_id, label): one write lock; resolve resource via get_resource_for_entity;
// idempotency: pre-existing Booking with booking_id → Ok echo; find Hold else NotFound;
// check_no_conflict excluding hold_id; emit HoldCommitted; do NOT re-check MAX_INTERVALS (net count unchanged).
```

`Event::HoldPlaced` **shape is unchanged** → WAL on-disk format stays stable. The **client contract is
not**: `web/app/actions/holds.ts:14` sends `Date.now()+duration` as an absolute `expires_at`; the edge
must switch to sending a *duration* (or nothing) in the **same PR** that drops `expires_at` from the SQL
parser, or the edge breaks mid-flight. 17 test callers pass `far_future` → all change to `ttl`.

---

## Ordered PRs (strict chain: 1 → 2 → 3 → [4, 5])

| PR | Phase | What | Gate / done-when |
|---|---|---|---|
| **PR1: clock seam + lint** | 0 | `src/clock.rs` (Clock + SystemClock + TestClock); `clock` field on `Engine`, threaded through ctors, `SystemClock` default in `main.rs`; replace the **4** `now_ms()` callers + route `reaper.rs:13,35` through `self.clock`; keep `now_ms()` as `#[cfg(test)]` for 13 fixtures; `clippy.toml` disallowed-methods **+ grep-gate** (clippy misses the FQ form). | All 178 tests green; `cargo clippy -- -D warnings`; lint REDs on a scratch `SystemTime::now()`. **Keystone: self-contained, must be first.** |
| **PR2: executable spec** | 0 | `proptest` + `cargo-mutants` dev-deps; hand-written `src/spec/invariants.rs` (model+std only) + **spec-import grep-gate**; dumb reference model + `Cmd` gen + stateful `tests/model_based.rs` (engine vs reference, assert after every op); `tests/wal_replay.rs` determinism + byte-identical re-serialization; `spec` CI job; `.mutants.toml` **excluding the costume** (`sql.rs`/`wire.rs`/`tests/`). | `model_based` + `wal_replay` green & PR-blocking; mutants baseline committed + ratchet (not fail-on-any-survivor day one). **The artifact v1 lacked.** |
| **PR3: sim + the RED seed** | 0→1 | In-process seeded poll-order executor `tests/sim_contention.rs`: N actors `place_hold → YIELD → release → YIELD → confirm` on one cap-1 slot; assert `never_double_book` globally after every commit. Run against **current** flow → capture the double-book **seed** into `tests/sim_seeds.txt`. | **Must RED on current code.** If it can't, the scheduler isn't crossing the seam. STOP and fix it (else another false-green). **Highest-risk PR.** |
| **PR4: Bug 2: authority expiry** | 1 | `place_hold` `expires_at → ttl`, engine computes from `self.clock`; drop `expires_at` from `Command::InsertHold` + parser; update 17 test callers **and the edge** (`holds.ts`) in this PR. | Authority-assigned deterministic expiry; `HoldPlaced` shape unchanged; edge sends duration. |
| **PR5: Bug 1: atomic CommitHold** | 1 | `Event::HoldCommitted` + `apply_event` arm; `exclude: Option<Ulid>` on `check_no_conflict` (3 callers → None); `commit_hold` mutation (one lock, idempotent on `booking_id`); `Command::CommitHold` + dispatch; collapse edge release-then-confirm into one `commitHold(holdId, ulid(), label)`. **Decide Open Q1 first.** | The captured PR3 **seed turns GREEN**; unit tests (exclude-self, missing→NotFound, idempotent, stolen-slot→Conflict, expiry-clamp). **Do not declare done on green unit tests alone**: the seed is the arbiter. |

`cargo-mutants` ratchet and `loom` (batch-lock primitive only) are **follow-ups, not first-ship gates**.

**Shape:** ~5-7 PRs, strict dependency chain (doesn't parallelize across one engineer). Effort is bounded
by *getting the cross-seam interleaving right* (PR3) and *reference-model fidelity* (PR2): "get-it-
actually-working" risks, not line count. No false precision on calendar time.

---

## Two backfilled research verdicts (the rate-limited dives)

**Thread-per-core / shared-nothing: NO, Phase 2.5+ / YAGNI.** deltat's per-resource `RwLock` + lock-free
`DashMap` already capture the contention win (two bookings on different resources never share a lock).
The remaining serialization point is the per-tenant WAL fsync: *I/O-bound, not a lock/scheduler problem
thread-per-core fixes*. Worse, deltat's workload is **hostile to clean sharding**: hierarchical resources
(Flight→Cabin→Seat) with ancestor availability inheritance + multi-resource atomic batches would become
cross-core transactions (the exact "multi-partition atomicity" case the manifesto warns is hard), and a
hot resource (a popular flight = *literally the product*) becomes a hot shard that pins one core while
others idle, **no work-stealing escape valve**, so it can *worsen* tail latency for skewed traffic. The
71%-tail-reduction result (Enberg et al., ANCS 2019) was for a *hash-partitioned KV store* and doesn't
transfer. The TigerBeetle-relevant lesson is its **determinism + simulation discipline** (already Phase 0),
*not* a per-core shard mesh. Rust TPC runtimes (Glommio/monoio) are Linux+io_uring-only, force `!Send`
single-threaded rearchitecture, and Glommio is effectively unmaintained. io_uring's real win for deltat
(batched WAL fsync/SQPOLL) is obtainable **without** abandoning tokio.

**100-year format discipline** (freeze the *spec*, not the binary):
- **Magic number + version nibble/byte** on the frame (the 1-byte frame prefix in §13). Negotiate, don't assume.
- **Never reuse or renumber** a field/enum discriminant; mark removed ones **reserved** (Protobuf rule). The `Event`/`Command` variants get explicit, frozen tags.
- **Must-ignore-unknown** for forward compat (IETF rule): old readers skip unknown optional fields.
- **The durability mechanism is a conformance test suite**, not prose: a cross-language (Rust↔TS) round-trip corpus gating CI, exactly like the SQLite format's test obsession.
- Freeze the four invariants from §13 (i64-µs instant; unified `Interval{kind}`; availability derived; single-home commit) as a versioned `FORMAT.md` spec separate from the implementation.

---

## What needs your decision

1. **Open Q1: hold-capability auth** (gates PR5/step 12). Recommendation: *possession of the `Ulid`
   `hold_id` authorizes commit*, valid while connections are tenant-authenticated and `hold_id`s aren't
   cross-tenant enumerable, **with** the capability-token upgrade trigger written into `V2-DESIGN.md` §9
   now. Confirm or override before PR5.
2. **Greenlight to start PR1** (the clock seam): low-risk, additive, all tests stay green, no behaviour
   change. This is the keystone the rest depends on.
