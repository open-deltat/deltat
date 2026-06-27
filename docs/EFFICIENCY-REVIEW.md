# deltat + tap — efficiency & cleanliness review

> Multi-agent review (6 dimensions × adversarial fact-check). 41 raw findings → **14 confirmed
> real *and* impactful**, 27 dismissed (premature micro-opts, deliberate tradeoffs, or already
> spec-planned). Fact-checked against the perf philosophy in `REQUIREMENTS.md` (SCALE-05:
> memory-latency bound, not CPU; HW-*/ENG-06 roadmap).

## Implementation status (all confirmed findings actioned)

Each shipped as its own PR (TDD test → clippy `-D warnings` / build → CI/local verify → merge).

| Finding | Status | PR(s) |
|---|---|---|
| **W2** Lagged kills forwarder | ✅ done | deltat #6 |
| **N1** seat-map N+1 availability | ✅ done (engine + SDK) | deltat #7, tap #17 |
| **N3** multi-row rule INSERT | ✅ done (kernel + SDK) | deltat #8, tap #18 |
| **N4** serial schedule deletes | ✅ done (`Promise.all`) | tap #18 |
| **N5** multi-row resource INSERT | ✅ done (kernel + SDK) | deltat #9, tap #19 |
| **N2** memoize calendar resource id | ✅ done | tap #20 |
| **DRY-1** centralize calendar resolution | ✅ done | tap #20 |
| **DRY-2** SDK `replaceOpenHours` | ✅ done | tap #21 |
| **E3** reaper earliest-expiry watermark | ✅ done | deltat #10 |
| **E1** linear interval storage | ⏸ **deferred = ENG-06** — the suggested `id→index` interim doesn't actually help (`Vec::remove` still shifts O(n) *and* the map needs O(n) re-index); a non-improving half-measure violates KISS. The real fix is the planned interval tree (ENG-06). |
| **E2** WAL coalescing window | ⏸ **skipped** — doesn't help the single-sequential writer (the next write isn't in-flight until the current returns) and concurrent writes are already drained by `try_recv`; would add latency to every commit for marginal gain. |
| **W1** apply event deltas vs full re-read | ⏸ **superseded + scoped** — N1 already collapsed the per-event re-read from `1+1+N` to one bounded round-trip. The literal client-side delta-apply is **rejected** (violates `seatmap-read-architecture`: never derive availability client-side → overbooking risk). The safe form (targeted re-read of only the affected resource) is a deferred demo polish with low post-N1 value and multi-component UI risk. |

Original findings below.

---

## TL;DR — the three that actually move the needle

1. **The apps ignore deltat's deltas and re-read *everything* on every event — and that re-read is an N+1.** A NOTIFY payload already says exactly what changed (which seat, booked/held), but the demos throw it away and re-fetch the whole venue's state via `getSeatState` — which itself fans out to **one availability query per seat**. So *one* booking by *anyone* triggers, on *every* watching browser, `1 + 1 + N` queries (N = seats). This is the biggest speed/compute win and it sits squarely in your two priority areas (networking + websocket). Two independent fixes compound: **(a)** apply the event as a delta to local state instead of re-reading; **(b)** add `availability.getMany` (one `IN (...)` query) so the unavoidable re-reads are `1+1+1`.

2. **Kernel `overlapping()` / `remove_interval` are O(n) on the hot path** — but this is the *already-planned* ENG-06 interval-tree (MODEL-13 documents it). Confirmed real and dominant at the 1e5 cap, **not a new defect**. The cheap standalone interim win is an `id→index` map to kill `remove_interval`'s O(n) `position()` scan on every hold-release/cancel.

3. **A single `Lagged` error permanently kills a connection's event forwarder** (`wire.rs`). `while let Ok(event) = rx.recv()` treats transient lag (recoverable) identically to channel-closed (terminal), so a momentarily-slow subscriber loses its live subscription *forever*. One-line-ish fix: `continue` on `Lagged` (optionally emit a resync nudge), `break` only on `Closed`.

---

## Confirmed — networking (tap ↔ deltat)  ← priority area

### N1. `getSeatState` N+1 availability fan-out  · **major** · `demo/app/actions/availability.ts:14-23`
`getMultiResourceAvailability` does `Promise.all(resourceIds.map(id => dt.availability.get({resourceId:id})))` — one SQL query + one engine sweep **per seat**. Bookings and holds already collapse N→1 via `getMany` (`IN (...)`); availability has no batched analog, so `getSeatState` is `1 + 1 + N`. Runs on the WS hot path (every NOTIFY → `loadSeatData` → `getSeatState`). At theater (~76) / airline (~150) seats that's ~76–150 concurrent queries serialized through postgres.js's default 10-conn pool.
**Fix:** add `availability.getMany(ids, start, end)` → `SELECT * FROM availability WHERE resource_id IN (...)` returning `Record<id, Slot[]>`; needs a `SelectAvailabilityMulti` engine command (the multi-resource sweep already exists for `getCombined`, but that *merges* and loses the per-seat breakdown). Collapses to `1+1+1`.

### N2. `ensureCalendarResource` round-trips on every calendar action · **moderate** · `calendar/lib/calendar-resource.ts:8-15`
Every authed action (`getWeekData`, `getBookings`, `cancelBooking`, `saveSchedule`) calls it → `SELECT * FROM resources WHERE parent_id IS NULL` + linear scan by `cal:<slug>` name, just to resolve a **fixed** resource id that never changes once created.
**Fix:** memoize the resolved id in a module-level cache (create-on-miss stays, hit once per process). Same pattern reimplemented in `findResourceBySlug` (public.ts) and demo `findRootByName` — see DRY-1.

### N3. `rules.create` issues one INSERT per row · **moderate** · `packages/client/src/rules.ts:29-32`
Not defensive — *forced* by the kernel: `sql.rs parse_insert` for `rules` takes only `values.rows[0]` (multi-row silently dropped), unlike `bookings` which has `BatchInsertBookings`.
**Fix:** add `BatchInsertRules` (mirror bookings) in the kernel, then emit one multi-row INSERT.

### N4. Schedule projection ≈ 128 sequential durable round-trips for a 90-day schedule · **moderate** · `calendar/lib/schedule-projection.ts:39-46`
Expands the weekly pattern over 90 days → one Rule/day, then deletes stale one-at-a-time and creates one-INSERT-at-a-time (N3). ~64 creates + ~64 deletes, each its own round-trip + group-commit.
**Fix:** (1) `Promise.all` the independent deletes — immediate, no kernel change; (2) batch the creates once N3 lands.

### N5. `createSeats` — one round-trip per seat (no batch-resource insert) · **moderate/architecture** · `demo/app/actions/seed-helpers.ts:55-68`
Nested loop, one `dt.resources.create` INSERT + fsync per seat (airline ~300, theater ~76, stadium ~92 sections). Kernel-forced — no `BatchInsertResources`.
**Fix:** add `BatchInsertResources` + multi-row `resources` VALUES parsing (mirror bookings); one INSERT + one group-commit. (Seed-time, so lower urgency — but it's the same missing kernel primitive as N3.)

---

## Confirmed — websocket / live updates  ← priority area

### W1. Every NOTIFY → full re-read instead of applying the delta · **moderate (compounds to major)** · `demo/examples/live/live-room.tsx:77-100`, `seat-booking-page.tsx:133-143`, `stadium/index.tsx:287-299`
The event payload carries `resource_id + kind + span` — exactly what changed — but the handlers ignore the body and call `getSeatState` over the *entire* venue (which is N1's N+1). So one booking → every client re-runs `1+1+N` queries. **This is the "delta updates we missed": deltat emits deltas; the apps re-derive from scratch.**
**Fix:** patch the one changed seat from the event body into the in-memory state Map; fall back to a full `getSeatState` only on gap/resync (pairs with W2). Turns "N+1 per event per client" into "O(1) local update."

### W2. One `Lagged` error permanently kills a forwarder · **moderate (correctness)** · `wire.rs:850-859`
`while let Ok(event) = rx.recv().await` ends the loop on **both** `Lagged(n)` (transient — fell behind the 256-slot ring) and `Closed` (terminal). A briefly-slow subscriber silently loses that resource's live stream until it re-LISTENs.
**Fix:** `match` the error — `continue` (+ optional `__resync` nudge) on `Lagged`, `break` only on `Closed`. (This also makes the "any event → re-read the truth" recovery actually trigger instead of going dark.)

---

## Confirmed — deltat engine

### E1. Linear interval storage (O(n) scan + O(n) remove) · **major, but spec-planned** · `model.rs:129-137, 119-125`
`overlapping()` binary-searches the right bound but then linearly filters the whole prefix (→ ~O(n) for a window late in a long timeline); `remove_interval` is `position()` + `Vec::remove` (O(n)) on every release/cancel/rule-removal. On the read path (availability) **and** write path (`check_no_conflict`). At the 1e5 cap this is the dominant cost.
**Status:** this *is* ENG-06 (📋, max-end-augmented interval tree + id→node arena, HW-10/11/12) — a known roadmap item, not a new defect. **Interim standalone win:** an `id→index` sidecar map removes `remove_interval`'s O(n) `position()` scan without the full tree.

### E2. WAL writer has no coalescing window — sequential clients flush batches of 1 · **moderate** · `engine/mod.rs:63-99`
The batch is only what `try_recv()` returns *right now*; a single client awaits each append (holding the resource lock) before the next, so it gets one fsync per booking. (Concurrent clients *do* batch — the gap is the sequential case.)
**Fix:** a tiny bounded coalescing delay (~50–200µs) before the first flush when the batch is small. Trades a hair of latency for real throughput on bursty single-writer paths.

### E3. Reaper/GC full-scan all resources on a timer regardless of due work · **moderate** · `engine/mutations.rs:380-394, 399-437`
`collect_expired_holds` (every 5s) and `gc_past_intervals` (every 60s) iterate **every** resource + every interval even when nothing is due.
**Fix:** a min-heap/BTreeMap keyed by next-expiry (or even just an "earliest pending expiry" watermark) so the reaper pops only what's due. Scales with tenant size today.

---

## Confirmed — DRY / architecture

### DRY-1. Calendar resource resolution reimplemented 3× (+ `cal:<slug>` name built 4×) · `calendar/lib/calendar-resource.ts`, `calendar/app/actions/public.ts`, `demo/app/actions/seed-helpers.ts`
`get({roots:true})` + `find(name == cal:slug)` in three places. **Fix:** one `calendarResourceName(slug)` + one `findCalendarResource(slug)`.

### DRY-2. "expand recurrence → replace stale open-hours rules" duplicated across apps · `calendar/lib/schedule-projection.ts` ≡ `demo/app/actions/rules.ts` (`setWeeklyAvailability`)
Identical cal.com-style op incl. the crash-safe create-before-delete ordering. **Fix:** move `replaceOpenHours(dt, resourceId, segments)` into the SDK (it already owns `expandRecurrence`); both apps expand their own pattern then call it.

---

## What was dismissed (27) — the fact-check earned its keep

The verify pass rejected real-but-non-impactful mechanisms, e.g.:
- **Micro-opts on a memory-bound system (SCALE-05):** redundant `sort` calls that turned out to operate on already-sorted runs; buffering result Vecs before streaming; `substitute_params` allocating a `Vec<char>`; `prepare:false` re-parsing ~60-byte control-plane SQL; per-connection ping/timer churn.
- **Spec-acknowledged / deliberate:** per-resource RwLocks not cache-padded (HW-14's false-sharing mechanism isn't actually present); the single porsager connection funnel (real, but the apps don't hit that scale and it's the documented v2 transport story); `AppendAtomic`/`Compact` bypassing group-commit (correct by design).
- **Real-but-trivial DRY:** 5 row→type mappers, `getMany` duplicated across bookings/holds, the 7 `wire.rs` Select arms, `DayName→number` declared twice, mutation-prologue repetition, client-config duplication — all genuine but ~lines-of-glue, no runtime cost. Worth tidying opportunistically, not urgent.
- **Dead/seed code:** `updateResourceSettings` (no callers), `getResources()` full-table fetch (mount-only, hundreds of rows), `prebookSeats` per-seat loop (seed-time, N≤36).

---

## Suggested order of attack (if you act)

1. **W1 + N1 together** — apply event deltas client-side **and** add `availability.getMany`. Biggest user-visible speed/compute win; kills the per-event N+1 storm. (tap SDK + one engine command + the demo handlers.)
2. **W2** — fix the `Lagged`-kills-forwarder bug. Small, correctness-relevant, and it makes the live path robust under load.
3. **N2** — memoize the calendar resource id. Trivial, removes a round-trip from every authed action.
4. **N3 + N5** — `BatchInsertRules` / `BatchInsertResources` in the kernel (one primitive pattern); unblocks N4 and fast seeding.
5. **E2 / E3** — WAL coalescing window + reaper heap. Engine throughput; do when touching that area.
6. **E1** — the interval tree is ENG-06; the `id→index` interim map is the cheap partial win.
7. **DRY-1/2** — opportunistic.
