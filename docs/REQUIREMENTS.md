# open-tap / deltat — Requirements (the forever spec)

> The single source of truth for **what this project is supposed to be and do.** Each line is a
> factual, checkable requirement with a stable ID. Add to it, edit it, never delete an ID (mark it
> `WITHDRAWN` instead). Status reflects the repo as of the last edit; when code changes, update the
> status, ideally backed by a test named after the ID.
>
> **Fact-checked against HEAD (`feat/clock-seam`) on 2026-06-18** by a full-repo sweep (deltat + tap).
> The engine core is faithful; the additions below (TIME-09+, MODEL-11+, AVAIL-11+, INV-10+, ENG-11+,
> PROTO-10+, SEC-07+, TEST-09+, EX-14+, GAP-08+) are verbatim-checkable facts pulled from the code.

**Status legend:** ✅ done & verified · 🟡 partial / degraded · 📋 planned (next) · ⏸ deferred by design
(until a real trigger) · ❌ explicitly excluded (anti-requirement) · ❓ open decision.

Companion docs: [`../V2-DESIGN.md`](../V2-DESIGN.md) (rationale), [`FORMAT.md`](FORMAT.md) (target wire/storage
format), [`PHASE-0-1-PLAN.md`](PHASE-0-1-PLAN.md) (build plan). **Note:** FORMAT.md, V2-DESIGN.md, the
README and both CLAUDE.md still describe the removed kernel `Schedule` and pgwire-as-destination — they
are stale w.r.t. HEAD (see GAP-08). This doc is authoritative where they conflict.

---

## VISION — the North Star (what we want this to become)

- **VIS-01** ⏸ One universal booking/scheduling protocol that can replace siloed booking systems.
- **VIS-02** ⏸ A **confederation** of independent self-hostable servers anyone can join (like email / the web), no central gatekeeper.
- **VIS-03** ⏸ **AI-native discovery**: any AI agent can search the federation for "bookable X near me, free at T" and book it.
- **VIS-04** ⏸ Every resource publishes its own **identity + location + data** into the federation, searchable.
- **VIS-05** ⏸ Geographic search across the confederation ("near me").
- **VIS-06** 🟡 Sub-millisecond scheduling/queries is an **in-region RAM-read / amortized-write** property only: a cache/RAM-hit read is ~100 ns and an interval-tree query ≈ `depth × ~100 ns` DRAM miss; **never** a single durable commit (one fsync ≈ 0.14–3.8 ms, HW-06) nor cross-region (speed-of-light ~100–250 ms, SCALE-04). Measured by the stress bench, not gated (TEST-11).
- **VIS-07** ⏸ Works across **multiple planets** eventually — design-for, build never until a second time-frame physically exists.
- **VIS-08** 📋 **No-shows are the #1 booking problem to solve**, via payment-backed commitment.
- **VIS-09** Adoption thesis: open + federated + AI-friendly + payment-trustworthy — not mere efficiency.

---

## PRIN — Principles (non-negotiable, apply to all code)

- **PRIN-01** First-principles: understand the problem before coding; ask "why" before "how."
- **PRIN-02** KISS / Occam: the simplest solution that works wins; no complexity you can't justify.
- **PRIN-03** DRY, but no premature abstraction.
- **PRIN-04** SOLID; small composable functions; no god functions/classes.
- **PRIN-05** No over-engineering: build only what's needed now.
- **PRIN-06** No duplicated state: one source of truth; derive, don't store twice.
- **PRIN-07** Comment the *why*, never the *what*.
- **PRIN-08** No panics in hot paths (Rust): errors are values. *(The untrusted-input panic via `Span::new` is now closed — SEC-09; internal asserts remain as guaranteed invariants, full fallibility tracked by TIME-05.)*
- **PRIN-09** Accidental complexity gets deleted; essential complexity gets relocated to the right layer — never into the kernel.
- **PRIN-10** A requirement is "met" only when running code verifies it; planning/docs are not progress.
- **PRIN-11** TDD: tests pass at every step; never leave tests broken between changes (deltat CLAUDE.md).
- **PRIN-12** **Test-first (red→green).** For a bug fix, write the failing test that reproduces it *before* the fix and confirm it goes red, then green; for a feature, extend the executable spec (TEST-01/02) alongside the code. A fix without a test that would have caught it is incomplete. Verify the test isn't hollow — a property/regression test must fail when the code is mutated (this session, mutation-testing the availability spec caught a real boundary blind spot). Practiced this session: GAP-12, GAP-13, and the seat hold→book race each had a red test before the green fix.

---

## TIME — The time model

- **TIME-01** ✅ An instant is a single integer tick count, not a calendar datetime. Today `Ms = i64` **milliseconds** (`src/model.rs:5`).
- **TIME-02** 📋 Canonical instant becomes **`i64` microseconds since the Unix epoch, UTC**. Sub-ms granularity at zero arithmetic cost.
- **TIME-03** ✅ Calendar date/time/zone is a **display projection** at the edge — never stored in the kernel.
- **TIME-04** ✅ Intervals are **half-open `[start, end)`**; adjacency is not overlap.
- **TIME-05** 🟡 Untrusted input now uses fallible `Span::try_new` (`model.rs`); `Span::new` still **asserts** for internal, guaranteed-valid spans. Full migration of *all* construction to `Result` remains 📋.
- **TIME-06** ✅ No time zones / DST / leap seconds / calendars in the kernel.
- **TIME-07** ⏸ Interplanetary: a single optional `frame` byte at the wire layer + a cross-frame comparison guard. Zero interplanetary code in the kernel.
- **TIME-08** ❓ Canonical scale = UTC-µs (chosen over TAI). But the only value compared to `now` (hold-expiry) MUST move to a **monotonic** source (HW-01/HW-03) so a backward `CLOCK_REALTIME` step — NTP correction or post-2035 negative leap second — can't make an expired hold read as live. Revisit TAI only if duration-across-leap-second correctness matters.
- **TIME-09** ✅ `a.overlaps(b)` iff `a.start < b.end && b.start < a.end`; equal-endpoint adjacency is never overlap (`model.rs:24-26`).
- **TIME-10** ✅ `Span::contains_instant(t)` is half-open: `start <= t < end` (`model.rs:29-31`).

---

## MODEL — The kernel data model

- **MODEL-01** ✅ One `Interval { id: Ulid, span: [start,end), kind }`; `kind ∈ { NonBlocking, Blocking, Hold{expires_at: Ms}, Booking{label: Option<String>} }` (`model.rs:42-59`).
- **MODEL-02** ✅ A `Resource` is anything bookable; resources form a **parent/child tree** (`parent_id`).
- **MODEL-03** ✅ `capacity: u32` = max concurrent allocations; the **kernel requires an explicit value** — "default 1" is an edge/SQL concern, not a kernel property (`model.rs:79,87`).
- **MODEL-04** ✅ `buffer_after: Option<Ms>` (0 when None) extends each allocation's **effective end** (`span.end + buffer`); it is not a separate interval.
- **MODEL-05** ✅ **Availability is derived, never stored** (`engine/queries.rs:76`).
- **MODEL-06** ✅ Children inherit open hours from ancestors; non-blocking OVERRIDES (nearest ancestor wins), blocking ACCUMULATES (`engine/queries.rs:18-74`).
- **MODEL-07** ✅ **Kernel admission rule (frozen):** a field may enter the kernel **only if computing single-resource availability is impossible without it** — args: `intervals, capacity, buffer_after, query-span, now`. Everything descriptive/geo/monetary/identity is exiled to a layer. *(No "schedule" arg — recurrence is edge rules.)*
- **MODEL-08** 🟡 `name: Option<String>` is the single grandfathered descriptive field; `Booking{label: Option<String>}` is a **second** descriptive String that violates the rule (GAP-02). The V2-DESIGN "clippy/review guard banning a second String" **does not exist** — `clippy.toml` bans only `SystemTime::now` (TEST-10).
- **MODEL-09** ❌ No business data in the kernel (specialty, price, photo, category, lat/lng, payment, reputation).
- **MODEL-10** 📋 Optional **`booking_group: Ulid`** correlation id on booking events → multi-resource bookings cancellable/queryable as one unit (GAP-01/GAP-09).
- **MODEL-11** ✅ The WAL/event vocabulary is **exactly 10 flat `Event` variants**: ResourceCreated/Updated/Deleted, RuleAdded/Updated/Removed, HoldPlaced/Released, BookingConfirmed/Cancelled (`model.rs:131-184`). No Schedule event.
- **MODEL-12** ✅ Entity IDs are **caller-supplied Ulids**; the engine never mints an id inside a mutation (no `Ulid::new()` in `engine/mutations.rs`).
- **MODEL-13** ✅ `ResourceState.intervals` is one unified `Vec` sorted by `span.start`; insert `O(log n)` (binary_search), remove `O(n)` linear by id (`model.rs:83,99-114`). *(ENG-06 replaces this with a tree.)*

---

## AVAIL — Availability & booking math (the calculations)

- **AVAIL-01** ✅ Availability = **open windows − blocking rules − active allocations (bookings + live holds)**, each allocation extended by `buffer_after`. The read path scans a **buffer-expanded window** (`[query.start − buffer, query.end)`) so an allocation whose buffer tail reaches into the window still subtracts — matching `check_no_conflict`'s search window (GAP-12). Verified by the property test (TEST-01) and by `buffer_straddling_query_start_blocks_availability` (`engine/availability.rs`).
- **AVAIL-02** ✅ A booking is **1-D interval collision** on the resource timeline; conflict iff buffered spans overlap (`engine/conflict.rs:17-52`).
- **AVAIL-03** ✅ **"2-D" = N coupled 1-D timelines keyed by resource id, bound by atomicity** — not a metric 2-D index.
- **AVAIL-04** ✅ Multi-resource availability = per-resource availability fed to a +1/−1 sweep with a `min_available` threshold (`engine/queries.rs:113-166`). Verified: Alice ∩ Bob.
- **AVAIL-05** ✅ Capacity > 1: occupancy via +1/−1 sweep-line; free = where occupancy < capacity (`engine/availability.rs:64-75,126-163`). Verified: hotel cap 5 vs cap 1.
- **AVAIL-06** ✅ **Atomic multi-resource booking** (`batch_confirm_bookings`, `mutations.rs:190-272`): sorted+deduped locks, two-phase validate-then-commit, all-or-nothing.
- **AVAIL-07** 📋 **`CommitHold(hold_id)`**: atomic hold→booking under one lock, excluding that hold from the conflict check. **Absent at HEAD** (no `commit_hold` symbol); hold→book is currently non-atomic.
- **AVAIL-08** 📋 Hold expiry should be **authority-assigned**; today `place_hold` takes `expires_at: Ms` as a **caller arg** and stores it verbatim (`mutations.rs:133-153`).
- **AVAIL-09** ✅ A child's rules must be **covered by the parent's availability**, else `NotCoveredByParent` (`mutations.rs:107-120`).
- **AVAIL-10** ⏸ "Reserve any k of N specific seats" / seat **adjacency** — resolved client-side then batch-committed; no kernel verb (GAP-06).
- **AVAIL-11** ✅ A hold counts only while `expires_at > now`; expired holds are ignored in availability and conflict (`availability.rs:34`, `conflict.rs:30,64`).
- **AVAIL-12** ✅ `buffer_after` extends only the **end** of an allocation, never the start.
- **AVAIL-13** ✅ Sweep-line tie-break sorts by time then delta ascending (−1 before +1 at equal time): an allocation ending at T frees capacity before one starting at T consumes it (`availability.rs:140`).
- **AVAIL-14** ✅ Queries for an unknown `resource_id` return **`Ok(empty)`**, not `NotFound` (`queries.rs:86-89`).
- **AVAIL-15** ✅ `min_duration_ms` post-filters free spans shorter than the threshold; `min_available == 0` or empty `resource_ids` → empty result. **Multi-resource results are `merge_overlapping`-merged BEFORE the `min_duration` filter** so coverage handed off between resources at a shared boundary is one window, not droppable fragments (GAP-13, `queries.rs`). Regression: `multi_avail_merges_adjacent_coverage_before_min_duration`.
- **AVAIL-16** 🟡 Allocation conflict is checked **against allocations only, not blocking rules** — a booking inside a blocked window does not conflict (documented limitation, `tests.rs:1636-1649`). *(T-03 decision pending: honor blocking rules in the conflict check.)*
- **AVAIL-17** ✅ An empty or inverted query window (`end <= start`) returns `Ok(empty)`, never a panic or error (`queries.rs:84-89`). Untrusted bookings/holds/rules with `end <= start` are rejected with SQLSTATE 22007 (T-01 / SEC-09).

---

## INV — Correctness invariants (must hold; back each with a test named after the ID)

- **INV-01** ✅ **Never double-book**: ∀ resource, ∀ instant, count(active overlapping) ≤ capacity. Verified as an executable property: `availability()` is checked against an independent brute-force point-sampling reference over 2000 generated cases (`availability.rs` mod `spec`; TEST-01). The stateful whole-tenant version (command sequences) is still pending (TEST-02).
- **INV-02** ✅ **Reconciliation**: an instant is free **iff** open ∧ ¬blocked ∧ (active count < capacity). Proven by the same property test, which reassembles free runs from per-instant truth and asserts equality with the engine's output.
- **INV-03** 📋 **Batch atomicity**: a rejected batch leaves every resource byte-identical; an accepted one applies exactly the batch.
- **INV-04** 📋 **Both-or-neither** for a multi-resource booking group (needs MODEL-10).
- **INV-05** 📋 **WAL replay determinism**: replay reproduces byte-identical state.
- **INV-06** 📋 **Idempotent commit**: re-committing a committed `Ulid` is a success echo.
- **INV-07** 📋 **No side effects on rejection**.
- **INV-08** 📋 **Hierarchy inheritance correctness** vs a hand-derived ancestor-walk.
- **INV-09** ✅ Per-resource `Arc<RwLock<ResourceState>>` serializes writes → no-double-book true by construction on one node (`mod.rs:26`; `mutations.rs:102,144,177`).
- **INV-10** ✅ Inheritance + create-resource walks bound depth at `MAX_HIERARCHY_DEPTH` (50) and detect cycles → `LimitExceeded("hierarchy too deep")` / `CycleDetected` (`queries.rs:32-39`, `mutations.rs:31-42`).
- **INV-11** ✅ On WAL-append failure the in-memory state is **not** mutated: `persist_and_apply` does `wal_append().await?` before `store.apply_event` (`mod.rs:236-247`).

---

## ENG — Engine & storage

- **ENG-01** ✅ A single self-contained Rust binary; no external database (DashMap `InMemoryStore`, `store.rs:8-12`).
- **ENG-02** ✅ In-memory state machine + append-only WAL with CRC framing and safe-truncation replay.
- **ENG-03** ✅ Group-commit WAL writer: one `flush_sync` per batch (`mod.rs:50-111`).
- **ENG-04** ✅ Multi-tenant: per-tenant Engine + WAL, lazy, path-sanitized, bounded.
- **ENG-05** ✅ All wall-clock reads flow through an **injected `Clock`** (`src/clock.rs`); `Engine::now_ms()` is the single read point; `SystemClock` is the only `SystemTime::now` caller (vDSO `clock_gettime(CLOCK_REALTIME)`, ~13–30 ns/read); availability/conflict take `now: Ms` as a param. Verified by `engine_reads_now_from_injected_clock`. **Known gap:** that read is `CLOCK_REALTIME` yet feeds hold-expiry/conflict math — a *steppable* source for elapsed time (HW-01); fix = the wall/monotonic split (HW-02, GAP-11).
- **ENG-06** 📋 Index = **max-end-augmented interval tree + id→node map** (`O(log n + k)` overlap, `O(log n)` writes), nodes ≤ 64 B / one cache line (HW-10) in a contiguous arena with 4 B handles (HW-12); query cost bounded as `depth × ~100 ns` with `depth = ceil(log2 n)` (HW-11). Today: sorted `Vec` + binary search + linear remove (MODEL-13).
- **ENG-07** 📋 **Index-in-RAM + interval-bodies-on-NVMe** spill tier.
- **ENG-08** 📋 Snapshots + segmented WAL so cold-start is O(working set).
- **ENG-09** ⏸ Per-shard replication (Raft) for HA.
- **ENG-10** ❌ Thread-per-core rewrite until one shared-memory node is proven insufficient.
- **ENG-11** ✅ WAL record framing = `[u32 LE length][bincode Event][u32 LE CRC32 of payload]`; length excludes the CRC; **no magic, no version byte** (`wal.rs:7-23`).
- **ENG-12** ✅ WAL replay terminates `Ok(events-so-far)` on the first truncated entry / CRC mismatch / deserialize failure — trailing corruption never errors (`wal.rs:122-161`).
- **ENG-13** ✅ Compaction is two-phase: write+fsync a sibling `.tmp` outside the lock, then atomic rename + reopen under the lock (`wal.rs:71-97`).
- **ENG-14** ✅ `EngineError` has exactly **9 variants**: NotFound, AlreadyExists, Conflict, NotCoveredByParent{rule_span,uncovered}, CycleDetected, HasChildren, CapacityExceeded(u32), LimitExceeded(&'static str), WalError(String) (`engine/error.rs:5-19`).
- **ENG-15** ✅ `LimitExceeded` uses a fixed message set: "too many resources", "resource name too long", "hierarchy too deep", "too many intervals on resource", "label too long", "batch too large", "query window too wide", "too many resource IDs", "timestamp out of range", "span too wide".
- **ENG-16** ✅ Kernel hard limits (`src/limits.rs`, prod): `MAX_RESOURCES_PER_TENANT=100_000`, `MAX_INTERVALS_PER_RESOURCE=100_000`, `MAX_TENANTS=1_000`, `MAX_BATCH_SIZE=1_000`, `MAX_IN_CLAUSE_IDS=1_000`, `MAX_HIERARCHY_DEPTH=50`, `MAX_NAME_LEN=1_000`, `MAX_LABEL_LEN=10_000`, `MAX_QUERY_WINDOW_MS=90d`, `MAX_SPAN_DURATION_MS≈10y`, valid instant range `[0, 32503680000000]` (≈ year 3000). *(These 1e5 caps are the truth; V2-DESIGN's "~1e8" is wrong — GAP contradiction.)*
- **ENG-17** ✅ Three limits shrink under `#[cfg(test)]`: `MAX_IN_CLAUSE_IDS`/`MAX_INTERVALS_PER_RESOURCE`/`MAX_RESOURCES_PER_TENANT` → 200 (`limits.rs:6-18`).
- **ENG-18** ✅ `compact_wal` rewrites the WAL to the minimal event set reproducing current state, parents before children (topological), one event per live interval (`mutations.rs:371-446`).
- **ENG-19** ✅ `gc_past_intervals` collects only past Bookings (`end < now − retention`) + expired Holds; **Rules are never collected**; locked resources skipped (best-effort) (`mutations.rs:330-369`).
- **ENG-20** ✅ Events bubble to ancestors: notify the target, then walk `parent_id` up the tree notifying each (`mod.rs:244-258`).
- **ENG-21** ✅ The group-commit channel is a tokio `mpsc` capacity 4096; the writer is a background task spawned at construction (`mod.rs:158-159`).
- **ENG-22** ✅ Config env vars + defaults: `DELTAT_PORT=5433`, `DELTAT_BIND=0.0.0.0`, `DELTAT_DATA_DIR=./data`, `DELTAT_PASSWORD=deltat`, `DELTAT_MAX_CONNECTIONS=256`, `DELTAT_COMPACT_THRESHOLD=1000`, `DELTAT_GC_RETENTION_MS=604800000` (7d), `DELTAT_METRICS_PORT` (off if unset), `DELTAT_TLS_CERT`/`KEY` (off if unset) (`main.rs:15-40`).
- **ENG-23** ✅ Three per-tenant background tasks: hold reaper every 5s, WAL compactor every 10s (when `appends_since_compact ≥ threshold`), GC every 60s (`reaper.rs:9-57`).
- **ENG-24** ✅ Connections capped by a `Semaphore(DELTAT_MAX_CONNECTIONS)`; over-limit rejected + counted; SIGTERM/ctrl-c drains in-flight up to 10s (`main.rs:46,87-137`).
- **ENG-25** ✅ Prometheus metrics exposed only when `DELTAT_METRICS_PORT` is set, via `/metrics`; fixed set: `deltat_queries_total`, `deltat_query_duration_seconds`, `deltat_connections_active/total/rejected_total`, `deltat_tenants_active`, `deltat_auth_failures_total`, `deltat_wal_flush_duration_seconds`, `deltat_wal_flush_batch_size` (`observability.rs:8-34`).

---

## PROTO — Protocol & interfaces

- **PROTO-01** 📋 Target core = a framed `Command`/`Response`/`Event` protocol (NDJSON-default, postcard-optional) per FORMAT.md §3. *Not built.* (FORMAT's "same encoding as the WAL" is aspirational: today the WAL is bincode with no magic/version, ENG-11.)
- **PROTO-02** 📋 (planned removal — **not done**) pgwire + full SQL parsing is the **current core transport** (`pgwire 0.37` + `sqlparser 0.59`, `Cargo.toml:8-9`; `src/wire.rs` + `src/sql.rs`, the only transport at HEAD). Slated for deletion in favor of PROTO-01. SQL-the-language ≠ pgwire-the-protocol.
- **PROTO-03** 📋 **HTTP/JSON adapter** — the universal external surface (POST a Command; GET cacheable availability).
- **PROTO-04** 📋 **MCP tool surface** (`search_bookable`/`get_availability`/`book`) — the AI-native interface.
- **PROTO-05** ⏸ **pgwire-compat** as an *optional, build-time-gated, read-only* SQL adapter for the v2 framed core.
- **PROTO-06** ❌ gRPC; ❌ a bespoke "thin SQL over a simple transport."
- **PROTO-07** 📋 Per-connection authenticated handshake `{ tenant, credential }`. *Today: PROTO-11.*
- **PROTO-08** 📋 Subscriptions push native frames off the broadcast hub. *Today: pgwire LISTEN/NOTIFY (PROTO-12).*
- **PROTO-09** 🟡 One canonical vocabulary (VOCAB) is the **target**; today the engine Rust API uses the lifecycle verbs but the **protocol uses SQL CRUD** (`Insert/Update/Delete/Listen`) and the **TS SDK** uses `rules.create/delete`, `bookings.create/cancel`, `holds.place/release`, `events.listen` — none match VOCAB-02; no `commit` verb exists at any layer.
- **PROTO-10** 🟡 Current external transport: PostgreSQL wire (pgwire 0.37, simple + extended query); SQL parsed by `sqlparser 0.59` (PostgreSqlDialect) into a **20-variant `Command` enum** (`sql.rs:11-99`).
- **PROTO-11** 🟡 Tenant = pgwire connection **database name** (default `default`); SQL username ignored; auth = single shared cleartext password `DELTAT_PASSWORD` (`wire.rs:71-84`, `auth.rs:18-44`).
- **PROTO-12** ✅ (current) LISTEN/NOTIFY channels are `resource_{ULID}`; events pushed as pgwire `NotificationResponse` with a JSON `Event` payload; a listener lagging > 256 events (broadcast capacity) is silently dropped (`wire.rs:86-101,724-776`).
- **PROTO-13** ✅ (current) Error→SQLSTATE map: parse→42601, engine→P0001, tenant→08006, invalid LISTEN/bad ULID→42000, **invalid time range (start≥end)→22007**, query too long→54000 (`wire.rs`).
- **PROTO-14** 🟡 Multi-row INSERT is honored **only for bookings** (`BatchInsertBookings`); multi-row resources/rules/holds silently keep the first row (`sql.rs:543-557`) — the engine-side root of GAP-03.
- **PROTO-15** 🟡 The demo hold→confirm→release lifecycle is a per-connection **WebSocket** protocol in `tap/demo/server.ts` (`{hold|subscribe|confirm}` → `{confirmed|error|Event}`); non-atomic (AVAIL-07 unbuilt) and hold expiry is client-supplied (`Date.now()+300000`).

---

## VOCAB — Terminology (one name per concept; see FORMAT.md). *Target vocabulary; PROTO-09 notes current divergence.*

- **VOCAB-01** Nouns: **Instant, Span, Interval, Resource, capacity, buffer, Rule (open/closed), Hold, Booking, availability.**
- **VOCAB-02** Lifecycle verbs, identical everywhere: Resource `create/update/delete` · Rule `add/update/remove` · Hold `place/commit/release` · Booking `confirm/cancel` · Subscription `subscribe/unsubscribe`.
- **VOCAB-03** Deletion is never one word: resource *deleted*, rule *removed*, hold *released*, booking *cancelled*.

---

## EDGE — Client / adapter layer (the timezone & calendar boundary)

> deltat is *just a database* of integer instants; **everything human — timezones, calendars, recurrence,
> display — lives at the edge** (the `tap` SDK + apps), never in the kernel. These are the requirements
> for that adapter layer. It is implemented today in `tap` (pre-existing); we routed seeds through it but
> did **not** fix its DST-naivety.

- **EDGE-01** ✅ The kernel operates only on integer UTC instants; **all** timezone / calendar / recurrence / display conversion is the edge's responsibility (the kernel↔human boundary). (Restates TIME-03 as a layer contract.)
- **EDGE-02** ✅ The SDK provides the time/calendar adapter helpers: `timeToMinutes`/`minutesToTime`, `daysOfWeekMask`/`daysFromMask`, `localUtcOffsetMinutes` (`tap/packages/client/src/schedules.ts:12-39`).
- **EDGE-03** ✅ Recurrence is expanded **at the edge** into concrete non-blocking Rules via `expandRecurrence` (`tap/packages/client/src/recurrence.ts`); the kernel never sees a recurrence pattern. All demo seeds use this (`addSchedule`→`rules`).
- **EDGE-04** 🟡 `expandRecurrence` is **DST-naive and uses the runtime's local timezone** (`new Date`+`setHours`), so a DST transition shifts a slot's absolute ms (= EX-15). A correct adapter should expand in the resource's *declared* zone.
- **EDGE-05** ✅ Human-readable display (times/dates/weeks) is computed in the **viewer's local timezone** via `@open-tap/shared` (`formatTime(ms)`, `toLocalDateString`, `dayBounds`, `weekStart/End/Days`); the kernel emits only ms.
- **EDGE-06** 🟡 `localUtcOffsetMinutes` is a single `-getTimezoneOffset()` snapshot (no per-date DST) used by two callers: the **dead** kernel-Schedule demo path (`tap/demo/app/actions/schedules.ts:15`, GAP-07) and the **live** `tap/calendar` `saveSchedule` action (`tap/calendar/app/actions/setup.ts:32`, EX-14).
- **EDGE-07** ❓ No resource carries an **IANA timezone**; for DST-correct recurring availability the edge needs the resource's declared zone (lives in the business-data layer, expanded at the edge). Decide + build when a real cross-DST schedule matters.
- **EDGE-08** ✅ deltat is "just a database": the edge (SDK + apps) is the **only** layer that knows about humans, calendars, timezones, recurrence, and rendering.

---

## SCALE — Scale & longevity

- **SCALE-01** Design target: resources/node = tens-to-hundreds of millions. **Today's enforced hard caps are 1e5** (`MAX_RESOURCES_PER_TENANT`/`MAX_INTERVALS_PER_RESOURCE`, ENG-16) — the target requires raising them + the spill tier.
- **SCALE-02** **Intervals** per node = billions (RAM) → tens of billions with the NVMe spill tier (ENG-07).
- **SCALE-03** ~8B people = ~10–100 home nodes, sharded **one-home-per-resource**.
- **SCALE-04** Sub-ms is an **in-region** property; cross-region commit is speed-of-light bound (~100–250 ms RTT). The in-region floor itself splits: read ~100 ns (RAM/cache hit) vs durable write ≥ one fsync (0.14 ms enterprise / 1.4–3.8 ms consumer NVMe, HW-06) — sub-ms holds for reads/amortized writes, not single durable commits.
- **SCALE-05** Binding constraint = **memory-latency → I/O, never CPU**: by the roofline, deltat's low-arithmetic-intensity (i64 compares per node) work is memory-bound, so query time ≈ `tree_depth × ~100 ns` DRAM miss and throughput is ceilinged by DRAM bandwidth (~25–77 GB/s), not instructions. "Faster language" is a category error; the levers are fewer cache lines touched / fewer DRAM hops (HW-10…HW-13).
- **SCALE-06** **Rust** (no-GC predictable tail latency, memory safety, longevity); not switching.
- **SCALE-07** "Never change for 100 years" = the **FORMAT/spec**, not the binary.
- **SCALE-08** Format-stability rules: magic + version byte; never reuse/renumber a discriminant; additive-only; must-ignore-unknown; a cross-language conformance corpus is the durability mechanism. *(Current WAL has no magic/version — ENG-11; this applies to the v2 format.)*

---

## HW — Hardware, clocks & performance physics (the accuracy & latency floors)

> The physical floors the design rests on, each with its cited number. These turn "fast"/"precise" into
> "fast/precise *because* X at layer Y." Most are 📋 (the seam exists; the optimization/fix doesn't yet).

**Clocks & time**
- **HW-01** 📋 Hold-expiry & conflict comparisons (`expires_at <= now` `conflict.rs:30,64`; `> now` `availability.rs:34`) MUST evaluate against a **non-steppable monotonic** source, never `CLOCK_REALTIME` — which can jump backward (NTP step, post-2035 negative leap second) and make an expired hold read as live. Today `now` is `SystemClock → CLOCK_REALTIME` (`clock.rs:24`) — a **known wrong source** for elapsed-time math.
- **HW-02** 📋 Split the `Clock` trait (`clock.rs:16`) into `now_wall() -> Ms` (`CLOCK_REALTIME`, for persisted/human absolute instants) and `now_mono() -> i64` (`CLOCK_MONOTONIC`, for all duration/expiry/conflict arithmetic) — mirroring Rust std's `SystemTime` vs `Instant`.
- **HW-03** 📋 Store/compare expiry as a **monotonic elapsed delta vs a fixed TTL** (`mono_now − placed_mono >= ttl`, saturating i64), not `wall_now >= stored_wall_deadline` — the delta form is immune to backward wall jumps.
- **HW-04** 📋 For expiry use `CLOCK_MONOTONIC` (NTP-slewed, jump-immune, vDSO-fast), **not** `CLOCK_MONOTONIC_RAW` (ignores NTP discipline → drifts at raw-crystal ±20 ppm; may fall off the vDSO into a ~122–762 ns syscall).
- **HW-05** 📋 A `now()` read MUST resolve via the kernel **vDSO (~13–30 ns, no ring transition)**, never a real syscall (~100–700+ ns) or a legacy clocksource (HPET ~1–2 µs read; ACPI PM slower); CI SHOULD assert the selected clock is vDSO-backed.
- **HW-20** 📋 Clock-injection tests SHOULD include **adversarial clocks** — one jumping backward, one stalling — asserting expiry/conflict invariants still hold (proves HW-01/03 are wired, not just documented). `TestClock.set()/advance()` already supports this.
- **HW-19** ⏸ A raw-TSC fast-path for `now()` is explicitly **not** a baseline (deltat is memory/I/O-bound; a ~20 ns vDSO read is never the bottleneck). If ever added: gate on invariant TSC (`constant_tsc`+`nonstop_tsc`), use `RDTSCP` with core-migration detection (`IA32_TSC_AUX`), else fall back to the vDSO.

**Memory hierarchy (the cost model — deltat is memory-latency-bound, formalizes SCALE-05)**
- **HW-10** 📋 An augmented-interval-tree node (ENG-06) MUST fit one **64-byte cache line**; assert `size_of::<Node>() <= 64` in a test named for this ID (i64 lo/hi/max = 24 B + 4 B arena handles fits with margin). A two-line node doubles DRAM misses per visit.
- **HW-11** 📋 An overlap/point query MUST cost ≤ `tree_depth` dependent DRAM references; document expected latency as **depth × ~100 ns** (one DRAM miss per pointer-chasing descent) and bound depth to `ceil(log2 n)`. Balance (shallower tree), not CPU micro-tuning, is the latency lever.
- **HW-12** 📋 Interval-tree nodes MUST live in a contiguous **arena (`Vec<Node>` + integer-index handles)**, not `Box`-per-node — arena order makes range scans near-sequential (~13 ns/line prefetched) vs random heap chases (~100 ns/hop), and shrinks handles 8 B→4 B.
- **HW-13** ✅ Range/availability scans MUST iterate contiguous memory in sorted order so the prefetcher engages (sequential ~37 cyc/line vs random ~100 ns); today's sorted-`Vec` (MODEL-13) satisfies this and MUST keep it through the ENG-06 migration.
- **HW-14** 📋 Any per-resource lock/atomic/shard counter written concurrently MUST be **128-byte padded/aligned** (e.g. `crossbeam::CachePadded`) so independent resources never share a line — the x86 spatial prefetcher pulls 64 B pairs, so 128 B is the safe unit; else false sharing serializes them invisibly.
- **HW-15** ❓ The sharded store (`DashMap`, ENG-01) MUST run `shard_count >= core_count` with cache-line-isolated shard control structures; a check MUST confirm no two hot atomics share a 64 B line.
- **HW-16** 📋 Benchmarks (TEST-11) MUST report **ns/query + cache-lines-touched/query** (`perf` cache-misses), not only ops/sec, and pin to one NUMA node during measurement (avoids the ~1.5–3× remote-DRAM noise).
- **HW-17** ❓ On multi-socket hosts, keep each tenant's hot index **local to one NUMA node** (process affinity) or document the ~1.5–3× remote-DRAM penalty on cross-socket pointer-chases.
- **HW-18** 📋 When an in-memory arena exceeds L2-STLB TLB reach (>~6 MB w/ 4 KB pages), run on **2 MB huge pages** (opt-in): ~512× TLB reach, measured ~2.4× lower per-access latency (2.41 vs 5.83 ns) on Skylake.

**Storage & durability floors (formalize SCALE-04 / ENG-03)**
- **HW-06** 📋 deltat MUST NOT claim sub-ms **durable-commit** latency on commodity SSD without power-loss-protected cache: the floor is **one fsync ≈ 0.14 ms (enterprise NVMe) to 1.4–3.8 ms (consumer)**. A lone durable commit cannot beat one fsync.
- **HW-07** 📋 The WAL durable path (ENG-03) MUST use **`fdatasync`** (≈2× faster than `fsync` on NVMe: 1.4 vs 3.8 ms) unless metadata is pending; durable throughput MUST be specified as **txn/sec under group commit** (>7,000 ACID txn/s on PC-3700-class NVMe), never per-commit latency.
- **HW-08** 📋 The WAL MUST stay strictly **sequential append-only** (LSM-like); the spill tier MUST NOT use an in-place B-tree (write amplification 7–14× higher). Carry the **SSD-GC tail warning**: on a full/under-provisioned drive, p99.99 can blow from ~30 ms to ~25 s after hours of scattered small writes — mitigate with sequential append + over-provisioning; p99 dashboards hide it.
- **HW-09** 📋 The spill tier (ENG-07) MUST budget cold random reads at **10–70 µs per dependency-chained lookup at QD1**, and MUST NOT assume the device's ~1M-IOPS rating (a QD≥32 figure) applies to a single dependent lookup. Read units near the ~32 KB SSD sweet spot.

### HW — recorded numbers & decisions (authoritative)
- **Two-clock decision:** `CLOCK_REALTIME` (vDSO, ~13–30 ns) is the source of record for persisted/external absolute instants **only**; `CLOCK_MONOTONIC` is the source of record for **all** duration/ordering/expiry/conflict. Today's single `CLOCK_REALTIME` read for expiry is a recorded known-wrong source (HW-01).
- **Precision ≠ accuracy:** precision = tick width (ms today / µs planned); single-node **accuracy** vs true UTC is bounded by the host sync regime — ±20–100 ppm (1.7–8.6 s/day) undisciplined quartz, ~1–50 ms internet NTP / sub-ms LAN NTP, sub-100 ns only with PTP hardware timestamping. **No µs/sub-ms cross-host accuracy claim on NTP-only hosts.**
- **i64 overflow horizon:** i64-ms ≈ ±292 M yr; i64-µs (planned) ≈ ±292,471 yr (safe); i64-ns ≈ year 2262 (**rejected** for range).
- **Memory cost constants (Skylake-class):** cache line 64 B; L1 ~1 ns / L2 ~3.4 ns / L3 ~12–14 ns / **DRAM ~100 ns** (≈200× L1); branch mispredict ~5 ns; uncontended mutex ~25 ns; NUMA remote ~1.5–3×. Query time ≈ `depth × ~100 ns`; throughput ceiling = DRAM bandwidth (~25–77 GB/s), not instruction count. **Lever = fewer cache lines / DRAM hops, never faster arithmetic.**
- **Durable floor:** one fsync ≈ 0.14 ms (enterprise) to 1.4–3.8 ms (consumer NVMe), ~50× a ~10 µs non-durable write; power-loss-protected cache collapses it to ~10 µs. **Sub-ms is read/amortized-write only.**
- **Reference frame:** single Earth-surface (geoid) frame — a flat i64 count is physically exact far below µs there; relativistic TT/TCG/TCB (~1e-9 rate diffs) are out of scope until off-geoid (TIME-07 / NOT-04).

---

## FED — Federation / discovery / geo (deferred until a real second operator)

- **FED-01** ⏸ Topology = **AT-Protocol three-tier**: authoritative home node (single writer) · relay (signed availability *summaries*) · indexer/AppView (search; a stale hint, never a commit point).
- **FED-02** ✅ (principle) No-double-book is not invariant-confluent → commit-time coordination at the single home is unavoidable. **CP for commit, AP for discovery.**
- **FED-03** ⏸ Cross-server booking = **Try-Confirm-Cancel escrow**, idempotency-keyed.
- **FED-04** ⏸ Identity/discovery: WebFinger/`.well-known`, DID, DKIM-style origin signing.
- **FED-05** ⏸ Fencing: monotonic ownership epoch + per-resource sequence number + per-op nonce.
- **FED-06** ⏸ Geo lives in the **indexer edge only**; one scheme (**S2**); cell-covering radius fan-out.
- **FED-07** ⏸ Cross-node multi-resource atomicity is an **unsolved saga** — documented, not faked. Mitigation: co-locate a booker's calendar with what they book.
- **FED-08** ❌ Building any federation/relay/indexer/identity/signing code before a real second operator exists.

---

## PAY — Payments / no-show prevention

- **PAY-01** 📋 `hold → confirm → capture` = Try-Confirm-Cancel + payment as a side-channel keyed by the kernel idempotency `Ulid`.
- **PAY-02** 📋 Two modes: manual-capture auth hold (short horizon); saved-card off-session charge as a fallible step (long horizon).
- **PAY-03** 📋 Instruments: deposit / prepay / card-hold / no-show fee; cancellation window is a policy layer.
- **PAY-04** 📋 **The protocol never custodies funds** (Stripe Connect *direct charges*; resource's PSP = merchant of record).
- **PAY-05** ❌ Any payment field in the kernel.

---

## EX — Examples / demos (in `tap/demo`; double as integration tests)

- **EX-01** ✅ A standalone Bun CLI (`tap/demo/scripts/seed-all.ts`) idempotently seeds the whole catalog (12 roots / 480 resources by manual count; counts logged, not asserted). The live app also lazily seeds each demo's root on page mount (`findRootByName`).
- **EX-02** ✅ Examples open **realistically occupied** (47 seats/rooms pre-booked at seed time).
- **EX-03** ✅ **Airline** (plane) — 2 flights, cabins, seats, showtimes; some seats pre-sold. Route `/demos/airline`.
- **EX-04** ✅ **Theater / Stadium** — sections, seats, showtimes. Routes `/demos/theater`, `/demos/stadium`.
- **EX-05** 🟡 **Cinema** — 4-screen multiplex seed exists (films/showtimes/seats, some sold) but there is **no `/demos/cinema` route**; data is created by seed-all and rendered by no page.
- **EX-06** ✅ **Restaurant / Parking** — sections/tables, floors/zones/spots. Routes `/demos/restaurant`, `/demos/parking`.
- **EX-07** ✅ **Personal calendar** — recurring availability windows; seeded (`seed-personal-calendar.ts`) and surfaced via the shared sidebar `PersonalCalendarProvider`, not a `/demos` route.
- **EX-08** ✅ **Meet** — two calendars; find common-free (intersection) and book on both atomically. Route `/demos/meet`.
- **EX-09** ✅ **Hotel** — room *types* with capacity (multi-night, capacity-aware). Route `/demos/hotel`.
- **EX-10** 🟡 **Availability** (Calendly-style) — loads + books off rules; owner *edit-schedule save* still calls dead kernel-Schedule (GAP-05). Route `/demos/availability`.
- **EX-11** ✅ All seeds use **edge recurrence** (`expandRecurrence`/`addSchedule` → rules), never a kernel Schedule.
- **EX-12** ✅ Exactly **10 `/demos/*` routes** (airline, availability, calendar, hotel, meet, parking, restaurant, scheduling, stadium, theater) — **not 1:1 with the 12 seeded roots** (cinema + personal-calendar seeded without a route; `scheduling` has a route).
- **EX-13** 🟡 `dev.sh` one-command run uses stock deltat on :5433; to target a local clock-seam build use `DELTAT_PORT=5434 DELTAT_PASSWORD=deltat`.
- **EX-14** ✅ `tap/calendar` is a separate single-tenant Calendly-style app (login + dashboard + public `/book/[slug]`) that depends on the SDK but **not** the removed kernel Schedule; it stores its weekly schedule in a local JSON file and projects it into rules via `expandRecurrence` over a 90-day horizon.
- **EX-15** 🟡 `expandRecurrence` is **DST-naive** and uses the runtime's **local timezone** (`new Date` + `setHours`), so a DST transition shifts a slot's absolute ms; it also supports explicit-segment passthrough + an `excludeDates` set (`packages/client/src/recurrence.ts:36-66`).
- **EX-16** ✅ The 2-D smoke test (`tap/demo/scripts/smoke-two-schedules.ts`) uses a fixed instant, asserts Alice∩Bob == 12-17, the atomic 2-resource booking splits to 12-14 & 15-17, and double-booking 9-10 is rejected; exits non-zero on failure.

---

## TEST — Testing & quality discipline (how we know it's right)

- **TEST-01** ✅ An **executable spec, hand-written, independent of the engine** (INV-01/02): `availability.rs` mod `spec` property-tests `availability()` against a brute-force point-sampling reference (open ∧ ¬blocked ∧ active < capacity, then reassemble free runs). 2000 cases/run, also wired into CI. Integer coordinates + half-open spans make point-sampling exact, so any disagreement is a real engine bug. This pass **found and fixed** the buffer-straddle read/write inconsistency (GAP-12).
- **TEST-02** 📋 **Stateful property testing** against a dumb whole-tenant reference model (command sequences: create/rule/hold/book/cancel/replay). TEST-01 covers the pure availability function; this covers the state machine — still pending.
- **TEST-03** 📋 **Deterministic simulation testing**: seeded multi-actor scheduler reordering across the release→confirm boundary; regressions are a seed.
- **TEST-04** 📋 `cargo-mutants` CI gate.
- **TEST-05** 📋 Metamorphic + fault-injection tests.
- **TEST-06** ✅ **Examples are integration tests** (the smoke test + the demos exercise every primitive).
- **TEST-07** ✅ **Build → run → observe → fix in tight loops** — running surfaces unknown-unknowns (this caught ~6 real bugs).
- **TEST-08** ✅ The clock seam (ENG-05) makes the engine deterministically simulatable (prerequisite for TEST-03).
- **TEST-09** ✅ CI gate = 4 steps: `scripts/check-no-ambient-time.sh` (grep-bans `SystemTime::now` outside `src/clock.rs`), `clippy --all-targets -D warnings`, `cargo test --lib` (skipping the 2 slow limit tests), `cargo test --test listen_notify` (`ci.yml:22-25`).
- **TEST-10** ✅ `clippy.toml` disallows exactly one method, `std::time::SystemTime::now` (belt-and-suspenders to the grep gate).
- **TEST-11** 🟡 The stress bench (`benches/stress.rs`, `harness=false`) prints n/avg/p50/p95/p99/max over the pgwire path but **asserts no latency threshold and is not in CI** — VIS-06/SCALE-04 sub-ms is measured, never gated.
- **TEST-12** 🟡 deltat coverage is **example-based only**: ~309 inline unit tests (engine 164, sql 56, wire 27, availability 19, model 18, wal/tenant/notify/clock/auth/reaper the rest) + **17 integration tests** (`listen_notify`, full pgwire path). **No** property/sim/mutation/fuzz deps (confirmed absent in `Cargo.toml`); the hold→confirm race class is **uncovered** (the one concurrency test, `group_commit_batches_appends`, uses *different* resources).
- **TEST-13** 🟡 tap SDK has **one** test file — `packages/client/src/__tests__/schedules.test.ts` (time/recurrence helpers, via `bun test`). No other SDK unit tests.
- **TEST-14** ❌(today) The demos + `tap/calendar` have **no automated tests and no E2E framework** (no Playwright/Cypress/vitest); the only demo verification is manual data-layer smoke scripts (`seed-all`, `smoke-two-schedules`), **not run in CI**.
- **TEST-15** 🟡 The Next.js demos are **build-verified + data-path-verified** (SDK→pgwire scripts), **not browser-run**; no UI/E2E has executed. "Examples are integration tests" (TEST-06) holds only at the data layer today.
- **TEST-16** 📋 **Coverage is measured, never guessed.** Wire `cargo llvm-cov` (line + region) into CI to *report* per-module coverage and surface untested critical paths (WAL replay, batch atomicity, hierarchy walks, the conflict check, hold expiry). Deliberately **not** a blunt %-gate: a coverage number rewards shallow tests that execute lines without asserting behaviour — the real quality signals are `cargo-mutants` (TEST-04, does a test *fail* when code is mutated?) and the executable spec (TEST-01/02). **Measured baseline** (`cargo llvm-cov --lib`, 310 tests): **82.76% region / 83.87% line / 87.71% function**. The split is the story: the durable time-allocation **core is ~93–100%** (availability.rs 99.75%, conflict.rs 100%, model.rs 100%, verify.rs 100%, queries.rs 96.9%, store.rs 98.9%, mutations.rs 92.4%, wal.rs 93.9%), while the number is dragged down almost entirely by the **transitional transport/plumbing**: wire.rs 51%, sql.rs 85%, observability.rs 15%, tls.rs 0%, engine/error.rs 0% (Display impls). That layer is exactly what the framed-protocol migration (PROTO-01/02) replaces — so low coverage there is acceptable today, but should be wired into CI to *report* (not yet automated). The SDK (TEST-13, `bun test --coverage`) and the demos/E2E (TEST-14/15) are outside `cargo` coverage and need their own measurement — "full code testing" spans all three surfaces, and two of them are near-zero today.
- **TEST-17** 📋 **Every fix carries the test that would have caught it** (the operational half of PRIN-12): the regression test lands in the *same* change as the fix, named after the requirement/GAP it protects (e.g. `multi_avail_merges_adjacent_coverage_before_min_duration` for GAP-13, `smoke-seat-hold-booking.ts` for the hold→book race). "Why was there no test?" should never be answerable twice for the same class of bug.

---

## SEC — Security / privacy / multi-tenancy

- **SEC-01** 📋 Per-connection authenticated handshake; full tenant isolation at the security layer. *Today: PROTO-11 (shared cleartext password).*
- **SEC-02** 📋 **Visibility/ACL**: `GetAvailability`/`GetBookings`/… authorization-gated per `(tenant, resource_id)` — not public. "Everything searchable" = "everything a publisher chose to publish via the discovery edge."
- **SEC-03** ❓ Hold-capability model: does possession of a `Ulid` `hold_id` authorize `CommitHold`? Decide before CommitHold ships.
- **SEC-04** ❓ GDPR right-to-be-forgotten on an append-only signed federated log — mitigated by keeping PII out of the kernel (GAP-02).
- **SEC-05** 📋 DoS / rate-limiting / quotas on the framed protocol + public endpoints. *(Partial today: ENG-24 connection cap.)*
- **SEC-06** ✅ Never display secrets / credentials in logs or output (streaming-safety).
- **SEC-07** ✅ Tenant **data** isolation (separate Engine + WAL per database) is implemented + tested; tenant names sanitized to `[A-Za-z0-9_-]` before becoming the WAL filename; empty-after-sanitization rejected (path-traversal guard) (`tenant.rs:46-57`).
- **SEC-08** ✅ TLS optional via `DELTAT_TLS_CERT` + `KEY` (both-or-neither); rustls, no client auth, ALPN `postgresql`; a connection not completing pgwire startup within 60s is dropped (`tls.rs:8-37`, `wire.rs:638-652`).
- **SEC-09** ✅ Untrusted SQL with `start >= end` is rejected cleanly (SQLSTATE 22007) at the wire boundary **and** the availability-query path via `Span::try_new` — the connection survives (test `inverted_span_errors_without_panicking`). Internal `Span::new` asserts remain for engine-derived spans (a guaranteed invariant); full fallibility = TIME-05.

---

## GAP — Known gaps / things to fix (the actionable backlog)

- **GAP-01** 📋 No durable link between multi-resource bookings → add `booking_group: Ulid` (MODEL-10) before the format freezes.
- **GAP-02** 📋 `label: String` is a second free-text/PII field → replace with `external_ref: Ulid`.
- **GAP-03** 📋 Engine silently truncates multi-row `INSERT` (PROTO-14) → must error, not truncate; resolved by the framed protocol. (SDK works around it.)
- **GAP-04** ❓ Open-ended / variable-duration bookings vs the frozen `start < end` invariant.
- **GAP-05** 🟡 Availability demo owner-edit save uses dead kernel-Schedule → point at `addRecurringRules`; then delete the dead demo `schedules` action.
- **GAP-06** ❓ Whether a kernel "reserve k-of-N specific" / adjacency verb is wanted.
- **GAP-07** 📋 The **demo** availability owner-panel is the only remaining caller of `dt.schedules.set` (`tap/demo/app/actions/schedules.ts`). The `tap/calendar` app does **not** use it (EX-14). Remove the dead path once the owner-panel points at `addRecurringRules`.
- **GAP-08** 🟡 **Docs reconciled to HEAD (mostly):** FORMAT.md Schedule commands/events removed; V2-DESIGN admission-rule `schedule` arg dropped; README + deltat/CLAUDE.md now note pgwire is *transitional* and Schedule is removed. **Remaining:** the TS SDK still exports a dead `Schedules` class + Schedule `DeltaTEvent` variants (= GAP-07 / #9); a few historical `schedule` mentions in V2-DESIGN §1/§4 (the v1 audit) are intentionally kept as history.
- **GAP-09** 🟡 Meet-booking cancel has no `booking_group` (MODEL-10) → the demo re-finds the mirror booking by matching `(start,end)` and cancels it separately — fragile if two bookings share a span (`tap/demo/app/actions/bookings.ts:34-46`).
- **GAP-10** 🟡 Orphaned `VERSION` file (`0.1.0`) duplicates the Cargo.toml version and is referenced by nothing — wire it up or delete (DRY).
- **GAP-11** 📋 **Hold-expiry/conflict math reads steppable `CLOCK_REALTIME`** — split the `Clock` into `now_wall`/`now_mono` and compare a monotonic elapsed-delta vs TTL (HW-01…HW-04); add adversarial backward/stalled-clock tests (HW-20). A real backward-jump correctness hazard, not cosmetic.
- **GAP-12** ✅ **RESOLVED — buffer-straddle read/write inconsistency.** `availability()` scanned `overlapping(query)` on the *raw* span, while `check_no_conflict` expands its search by `buffer`. A booking ending at `T` with `buffer_after = B` would read as *free* in a query window starting in `(T, T+B)`, yet a booking there is *rejected* — the read path advertised an unbookable slot. Fixed by scanning `[query.start − buffer, query.end)` in `availability()` (rules, which carry no buffer, are skipped when they end ≤ `query.start`). Found by TEST-01, regression-locked by `buffer_straddling_query_start_blocks_availability`. (Distinct from T-03, which is the *blocking-rule* read/write disagreement and still open.)
- **GAP-13** ✅ **RESOLVED — multi-resource availability fragmented continuous windows under `min_duration`.** `compute_multi_availability` ran its own sweep-line but, unlike the single-resource path, never merged its result. When coverage of one continuous window is handed off between resources at a shared half-open boundary `T` (resource A free `[a,T)`, resource B free `[T,b)`), the sweep's `(time, delta)` tie-break drops the live count below threshold at `T`, emitting two adjacent spans. The instant *set* was correct, but `min_duration` then length-checked each fragment in isolation — so a genuinely continuous window long enough to qualify was split into sub-threshold pieces and **all dropped**, returning "no slot" when one existed (e.g. "find a 6h window where ≥2 of N rooms are free"). Fixed by `merge_overlapping` on the collected segments before the `min_duration` retain (mirrors the single-resource path). Found by the adversarial bug-hunt + confirm pass (two independent failing repros); regression-locked by `multi_avail_merges_adjacent_coverage_before_min_duration`; the bug-codifying assertion in `multi_avail_exact_boundary_touch` was corrected to expect the merged window.
- **GAP-14** ✅ **RESOLVED — atomic batch ignored capacity (intra-batch overlap rejected unconditionally).** `batch_confirm_bookings` validated batch members against each other with a pairwise `overlaps()` check that returned `Conflict` for *any* two same-resource members sharing a span — with **no `capacity` term**. So booking N units at once on a capacity-N pool (e.g. N GA tickets from a capacity-2000 stadium section) failed even though the resource had room; only sequential `confirm_booking`s worked. The GAP-AUDIT note had confirmed batch *atomicity across resources* but never exercised an overlapping batch on a high-capacity resource, so the path was untested. Fixed by branching on capacity: capacity-1 keeps the pairwise check (any overlap conflicts); capacity-N folds the batch members into the same buffer-aware sweep as committed allocations (`check_batch_capacity`, `conflict.rs`) and rejects only when concurrency would exceed capacity (`compute_saturated_spans(combined, capacity + 1)`). Found by the stadium-pool demo prerequisite check; regression-locked by `batch_capacity_books_n_units_same_span_atomically`, `batch_capacity_rejects_over_capacity_atomically`, and `batch_capacity_accounts_for_committed_load` (all red→green, asserting both the success and the all-or-nothing reject).
- **GAP-AUDIT** ℹ️ **Adversarial verification pass (13 agents) results.** Beyond GAP-12/GAP-13, an independent refutation sweep produced runnable evidence that the following are **correct as built** (each had a failing-to-refute probe): capacity ≥ 4 sweep ordering (the `-1`-before-`+1` tie-break is correct for half-open spans), realistic large buffers (500 / 60 000 ms) on both read and write paths, capacity+large-buffer interaction (400-trial fuzz), **INV-03 batch atomicity** (Phase-1 validate-all then Phase-2 commit; a rejected batch leaves other resources byte-identical), and **INV-05 WAL replay determinism** (byte-identical replay verified with an ordering trap). These remain test-unbacked in-repo (probes were ephemeral) — see backlog. The pass **reconfirmed** the known-open items as genuine: AVAIL-16/T-03 (blocking-rule read/write disagreement, by-design pending decision), AVAIL-07 (no atomic `CommitHold`; `release_hold`→`confirm_booking` is a real TOCTOU window), and HW-01/GAP-11 (steppable `CLOCK_REALTIME` feeds expiry math).

---

## TENSIONS — requirements that pull against each other

> Pairs of requirements in tension. **OPEN** ones are genuine contradictions needing a decision/fix;
> **RESOLVED** ones legitimately coexist — the note records *why*, so the reconciliation isn't relitigated.

### OPEN — genuine contradictions (act / decide)

- **T-01** ✅ **RESOLVED** (was open) — `PRIN-08` ⟷ `Span::new` panic from untrusted SQL. Fixed: the wire boundary + the availability-query path use `Span::try_new` → clean 22007 error, connection survives (test added). Internal asserts remain (full fallibility = TIME-05).
- **T-02** `MODEL-09`/`NOT-02` (no business data in kernel) ⟷ `MODEL-08` (`name` **and** `label` are descriptive Strings in the kernel). `name` is grandfathered; `label` is the unresolved one → `GAP-02` (`label → external_ref`).
- **T-03** `AVAIL-01` (availability **subtracts blocking rules** → a blocked window reads as *unavailable*) ⟷ `AVAIL-16` (the booking conflict-check **ignores blocking rules**, so a direct `ConfirmBooking`/`PlaceHold` into a blocked window **succeeds**). **The availability view and the write guard disagree** — a client is told a slot is unavailable yet can still book it. Not an `INV-01` (capacity) violation, but a real semantic inconsistency. → decide: should the conflict check also reject blocked windows?
- **T-04** `ENG-16` (enforced hard caps = 1e5 resources/intervals) ⟷ `SCALE-01`/`SCALE-02` (target: tens-to-hundreds of millions / billions). The code *actively forbids* the vision's scale today. → raise caps + build the spill tier (`ENG-07`) when warranted.
- **T-05** ✅ **mostly resolved** — docs reconciled to HEAD (FORMAT.md, V2-DESIGN.md, README, deltat/CLAUDE.md; GAP-08): kernel `Schedule` marked removed, pgwire marked transitional. Only the dead SDK `Schedules` class remains (GAP-07 / #9).
- **T-06** `MODEL-12` (caller-supplied Ulids — enables `INV-06` idempotent commit) ⟷ `SEC-03` (a guessable/leaked `hold_id` = slot-hijack once `CommitHold` exists). The same property that buys idempotency creates a capability-security question. → decide the hold-capability model before `AVAIL-07` ships.

### RESOLVED — coexist by design (don't relitigate)

- **T-07** `VIS-06` (sub-ms) ⟷ `SCALE-04` (cross-region = speed-of-light) + `ENG-03` (durable commit is fsync-bound ~0.3–2 ms). Resolved: sub-ms is an **in-region read / amortized-batched-write** claim, never a single durable commit or a cross-region round-trip.
- **T-08** `MODEL-05`/`PRIN-06` (availability derived; no duplicated state) ⟷ `FED-01` (relay caches signed availability *summaries*). Resolved: the **kernel** never caches (always derives); the discovery edge holds a **stale AP hint**, never the source of truth, and every commit re-validates at the single home (`FED-02`).
- **T-09** `VIS-02`/`VIS-03` (open; anyone joins; everything searchable) ⟷ `SEC-02` (authz-gated, publisher-opt-in) + `FED-02` (single-writer/CP) + `FED-05` (anti-replay/sybil). Resolved: "searchable" = "what a publisher **chose** to publish via the edge"; openness is protocol-level, not a mandate to expose private/kernel data.
- **T-10** `PRIN-05`/`NOT-06` (no over-engineering) ⟷ the large deferred `VIS`/`FED`/`PAY`/`SCALE` design + `FORMAT.md` + this doc. Resolved: **boundaries drawn now (free), layers built only on a real trigger (expensive)** — everything forward-looking is ⏸/📋, documented-not-built.
- **T-11** ⚠️ **superseded by HW-01** — the "≤1 s leap drift is irrelevant" rationale was wrong: a backward NTP step or post-2035 *negative* leap second can make an expired hold read as live. Resolution is now HW-01/HW-03 (expiry math on a **monotonic** clock, delta-vs-TTL), not "UTC is close enough." `TIME-08` keeps UTC for *stored* instants; *elapsed/expiry* moves to monotonic.
- **T-12** `VIS-09` (adopt because open/good) ⟷ `VIS-08`/`PAY-*` (the wedge is no-show payments). Resolved: an **open, neutral protocol** with payments as a **custody-free edge layer** (`PAY-04`) — the protocol never touches funds.
- **T-13** `PRIN-10`/`NOT-07` (planning isn't progress) ⟷ this spec being mostly 📋/⏸. Resolved: status markers separate **wanted** (📋/⏸) from **built** (✅); the doc is the tracker/spec, not a claim of done.

---

## NOT — Explicitly NOT doing (anti-requirements — resist scope creep)

- **NOT-01** ❌ Calendars / time zones / DST / recurrence rules in the kernel (edge concern).
- **NOT-02** ❌ Business/descriptive/geo/monetary/identity data in the kernel.
- **NOT-03** ❌ pgwire/SQL as the *long-term* core transport (it is the current-but-transitional transport — PROTO-02; the v2 target is the framed protocol).
- **NOT-04** ❌ Any interplanetary/relativistic code now (one wire byte of intent only).
- **NOT-05** ❌ Federation/relays/indexers/geo/signing/identity before a real second operator / near-me customer.
- **NOT-06** ❌ Switching languages for speed; thread-per-core rewrite; speculative scale machinery before the math demands it.
- **NOT-07** ❌ Treating planning/docs as progress — a requirement is "met" only when running code verifies it.
