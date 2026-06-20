# deltat / open-tap — v2 design

> A first-principles rethink of the time-allocation database and the protocol around it.
> Researched and stress-tested by a 19-agent review of both repos + 6 prior-art domains
> (federation, calendar standards, interval data structures, geo-indexing, interplanetary
> time, distributed consistency). This doc is the synthesis and the build plan.

---

## TL;DR — the verdict

**Refactor, don't rewrite.** The engine core is the product and it is genuinely good. What
made v1 feel like "too much" is not the idea — it's that you wrapped a ~24-verb key-value-ish
interval store in a **full PostgreSQL costume** (1,195 LOC `wire.rs` + 1,576 LOC `sql.rs`),
which is *larger than the entire engine* and is per-request overhead on every call.

The whole v2 is **subtractive and surgical**:

1. **Delete the costume.** Remove `wire.rs`, `sql.rs`, and the `sqlparser` dependency. Promote
   the `Command` enum — which is *already* the true protocol contract — directly to the wire.
2. **Fix two real correctness bugs** found during the review (hold→confirm is not atomic;
   hold expiry is client-supplied). Both verified in source.
3. **Fix the one efficiency ceiling**: replace the sorted-`Vec` + linear left-scan with an
   augmented interval tree → true `O(log n + k)` overlap and `O(log n)` writes.
4. **Widen time `ms → µs`** for sub-millisecond granularity at zero arithmetic cost.
5. **Keep everything else verbatim** — the unified interval model, the availability/conflict
   sweep-lines, the WAL, group-commit, per-tenant isolation. 4,748 LOC of engine tests survive.
6. **Federation, geo, and interplanetary are documented seams, not built code.** They are added
   behind clean hooks *only when a real second operator / near-me customer / second planet
   actually exists.* Building them now is the exact "I tried too hard" trap, one layer out.

The grand vision (a confederation that swallows every booking silo, searchable by location,
sub-ms, multi-planet) is **correct as a destination and wrong as a starting point.** The path
to it runs *through* a ruthlessly clean single node, not around it.

### Round-2 refinements (the parts that actually decide whether this works)

Three additions, each answering a specific founder concern, each elaborated in a new section below:

- **Why v1 "passed tests but felt off" is now *proven*, not mysterious** — and the cure is a
  different *kind* of testing, made a CI gate, not more unit tests. The hold→confirm bug lives
  only in the *seam* between three individually-correct functions; the 178-test suite is
  sequential and its one "concurrency" test uses 20 *different* resources, so it is structurally
  blind to same-slot contention. See **§10**. This is the single most important thing to get
  right, and it must be built *before* any other v2 code.
- **Deleting complexity is safe because we delete *accidental* and relocate *essential*.** Your
  instinct — "isolate time to one DB, push business data to other DBs, reference by ID" — is
  correct and ~99% already done. Formalized as a six-layer architecture with a one-line **kernel
  admission rule**. See **§11**.
- **The moat is the destination; the *wedge* is no-shows.** "People adopt it because it's so
  good" is not a go-to-market — a federation is worth ~nothing at n=1. What an operator pays for
  *today* is killing no-shows with payment-backed commitment. So **payments is promoted to a
  near-term phase and federation/discovery come after the first paying operator.** See **§12**.
  The scale/language/100-year answers are in **§13** (short version: you're memory-bound not
  CPU-bound, Rust is correct, and you future-proof the *format*, not the binary).

---

## 1. What you actually built (honest map)

### The elegant core — keep all of this

| Piece | Where | Why it's right |
|---|---|---|
| **Unified interval model** | `model.rs:42-59` | Rules, holds, bookings are *one* `Interval { id, span:[start,end), kind }`. One sort order, one `overlapping()` primitive, one mutation funnel serve every feature. This is the conceptual win. |
| **Availability is derived, never stored** | `availability.rs:44-113` | NonBlocking OVERRIDE / schedule fallback → Blocking ACCUMULATE subtract → capacity-aware allocation subtract. No duplicated state. |
| **Sweep-line primitives** | `availability.rs:116-198` | `subtract_intervals` (two-pointer), `merge_overlapping`, `compute_saturated_spans` (+1/−1 sweep with capacity==1 fast path). Textbook-correct, allocation-light, well-tested. |
| **Pure-integer schedule projection** | `availability.rs:9-34` | `div_euclid`/`rem_euclid`, epoch-Thursday math, *no date library*. Keep, re-point ms→µs. |
| **`apply_event` single funnel** | `store.rs:96-206` | Replay and live writes go through the same code → they can never diverge. The invariant that makes event-sourcing trustworthy. |
| **WAL + group-commit** | `wal.rs`, `mod.rs:49-134` | `[len][payload][crc32]`, safe truncation, atomic-rename compaction, one fsync per batch. Protocol-agnostic. Keep (swap `bincode`→`postcard`). |
| **Batch lock discipline** | `mutations.rs:190-272` | Sort+dedup resource ids before locking, two-phase validate/commit. The correct deadlock-avoidance template. |

### The costume — delete it

| Piece | LOC | Verdict |
|---|---|---|
| `wire.rs` (pgwire) | 1,195 | A Postgres protocol emulator over an interval store. |
| `sql.rs` (sqlparser → `Command`) | 1,576 | Parses a *full SQL grammar* to recover ~6 scalars per call into a **fixed 24-verb enum**. |
| Extended Query Protocol | (in wire.rs) | Parse/Bind/Describe/Execute, `substitute_params`, `schema_for_sql`. **Correction:** this is *not* dead code — the shipped client sets `fetch_types:false` (not `prepare:false`) and uses `sql.unsafe(query, params)`, which makes postgres.js use the **extended** path, so `substitute_params` (`wire.rs:565`) → `parse_sql` (`wire.rs:573`) is the **live and only** path the client exercises. It re-interpolates params into SQL text and *re-parses* — pure round-trip overhead to recover ~6 scalars we already hold as a typed `Command`. Delete it on that ground (accidental complexity > the engine), not "it's unused." |
| 7 hand-written `*_schema()` pgwire type tables | | TEXT-encode-everything machinery for a describe path nothing exercises. |
| Shared cleartext password, username ignored, tenant = unauthenticated startup string | `auth.rs:18` | Not a security model. |

**The smoking gun:** `sql.rs:11-113` defines the `Command` enum — 24 variants
(`InsertResource`, `InsertBooking`, `SelectAvailability`, `Listen`, …). That enum *is* your
protocol. Everything upstream of it (SQL text → AST → walk → extract) and downstream
(`Command` → pgwire rows) is machinery to avoid admitting you have a fixed RPC vocabulary.

---

## 2. The two correctness bugs (verified in source)

These are real and worth fixing on line one of v2, independent of everything else.

### Bug 1 — hold→confirm is not atomic (TOCTOU)

`confirm_booking(id, resource_id, span, label)` at `mutations.rs:222-247` **takes no `hold_id`**.
It just runs a *fresh independent* `check_no_conflict` and inserts a `Booking`. It has no idea
which hold it is "confirming." Consequences:

- At capacity 1, the client's *own* active hold collides with the booking → confirm fails,
  unless the SDK releases the hold first.
- So the edge (`tap` `server.ts:53-60`) does **release-then-confirm as two awaited calls** —
  which opens a window where the slot is free and a competing booker can take it.

Either way, "place hold, then atomically book it" — described as the protocol's best idea — is
**not actually atomic today.**

**Fix:** a single `CommitHold(hold_id)` op that converts a specific named hold into a booking
**under the same write lock, in one event**, excluding that hold from the conflict check. Hold
becomes a genuine reservation transfer, not two independent checks.

### Bug 2 — client-supplied hold expiry

`place_hold` takes `expires_at` as a parameter; the TS edge sets it to `Date.now() + 300_000`
(`server.ts:37`) — the *booker's* wall clock. A skewed or dishonest client controls how long it
locks a slot.

**Fix:** the **authority** (the owning engine) assigns `expires_at` on its own clock and returns
it to the client as opaque. Closes the clock-skew double-book / slot-squat window.

---

## 3. First principles: what is an instant?

> "Time is not even a primitive… a datetime in Postgres is not simple enough to be efficient."

Correct. The primitive is **a single integer count of ticks from an epoch.** A *calendar
datetime* (year/month/day/tz/DST) is a **derived display projection**, and belongs in the client,
never in storage. v1 already gets this right with `i64` Unix ms. v2 refines exactly two things:

### Decision: store `i64` **microseconds**, UTC, single integer. Nothing more.

- **Microseconds, not ms** → buys sub-millisecond granularity (the one genuinely new
  requirement) at *zero* arithmetic cost. Range: ±292,000 years. The same `i64` add/compare.
- **Not a frame-tagged `(tick, frame_id, epoch_id)` tuple.** That was proposed and **rejected**
  by both adversarial reviewers as speculative generality. An `i64` µs count is *already*
  planet-agnostic — its arithmetic carries no Earth assumption. A `frame_id` byte in storage is
  the "just in case" your own principles forbid, and it introduces a correctness footgun (a Mars
  µs silently compared to an Earth µs).
- **UTC, not TAI** — a deliberate, reasoned divergence from the textbook ("store TAI, project
  UTC"). Rationale specific to *this* system: the only field ever compared against `now()` is
  **hold expiry**, and both the engine (`SystemTime::now()`) and the edge (`Date.now()`) already
  produce UTC. Declaring the canonical scale "TAI, leap-seconds banished" would force a maintained
  IERS leap-second table at *every edge* just to compare a hold against the wall clock — *more*
  leap-second surface, not less. The cost of staying UTC is that a duration spanning a leap second
  is off by ≤1 second, which is irrelevant to booking. (If that ever matters, TAI lives behind the
  same one-byte seam below.)

### The interplanetary seam — one byte of *intent*, zero bytes of code

"Must work across multiple planets eventually" is honored honestly:

- The seam is **one optional `frame` byte at the wire-protocol layer**, added the day a second
  time-frame physically exists — *not* in the stored `Instant`, *not* in the WAL, *not* now.
- Plus a guard that **rejects cross-frame interval comparison**, so a Mars instant can never be
  silently compared to an Earth instant.
- Everything relativistic — time dilation, light-delay-aware expiry (DTN / one-way-light-time),
  cross-frame conversion (TCB/TCG/TDB-style), Coordinated Mars/Lunar Time — is **forbidden in the
  kernel forever** and deferred to an edge library that does not exist until Mars ships.

We pay one byte of *design intent* today and write zero interplanetary code. Documented as
deferred, never mistaken for a solved feature.

---

## 4. The v2 kernel ("tapd")

### Data model — unchanged except the index underneath

```
Instant   = i64   // microseconds since Unix epoch, UTC
Span      = { start: Instant, end: Instant }   // half-open [start, end); Span::new -> Result, no panic
Interval  = { id: Ulid, span: Span, kind: IntervalKind }
IntervalKind = NonBlocking | Blocking | Hold{expires_at} | Booking{label}
ResourceState = { id, parent_id?, name?, capacity, buffer_after?, intervals, schedule? }
```

Availability stays derived. The sweep-line primitives are lifted verbatim. **Do not** do the
speculative "business data leaves the kernel / opaque 16-byte ref" refactor — `name` is cheap and
already there; that's YAGNI for a single-node store.

### The one structural change: augmented interval tree

v1's `overlapping()` (`model.rs:131-139`) binary-searches the **right** bound only, then **linearly
scans from index 0** filtering `end > query.start`. For a future-dated query against a resource
with history, `right_bound → N`, so a query overlapping a handful of intervals still costs `O(N)`.
At the 100k-interval ceiling this directly breaks the "sub-millisecond" promise. `remove` is also
an `O(N)` scan (`model.rs:122`), and cold-start WAL replay is `O(N²)`.

**Replace the sorted `Vec` with a max-end-augmented interval tree** (red-black/AVL, keyed on low
endpoint, subtree carries max high endpoint — CLRS ch. 14) **+ an `id → node` map.** This is the
canonical fix and the *actual* sub-ms enabler:

| Op | v1 | v2 |
|---|---|---|
| overlap query | `O(log N + right_bound)` → `O(N)` | `O(log N + k)` |
| insert | `O(N)` (Vec memmove) | `O(log N)` |
| remove | `O(N)` (linear scan) | `O(log N)` |
| cold WAL replay | `O(N²)` | `O(N log N)` |

The change is **behind `ResourceState`'s existing interface** — `model.rs` only. Everything that
calls `overlapping()` is untouched. (For pure capacity/occupancy counting, roaring bitmaps over
slot-discretized time are a fast secondary option; not needed for phase 1.)

> Baseline-to-beat worth noting: a Postgres GiST exclusion constraint over a `tstzrange` gives
> race-safe single-resource collision detection with zero hand-rolled tree. The interval tree wins
> on in-memory sub-ms latency and on owning the WAL/event model — but if a node ever wants
> durability-via-Postgres, GiST is the honest fallback, not a regression.

### The protocol — promote `Command` to the wire

Delete pgwire + sqlparser. The wire *is* the `Command` enum:

```
frame  = [u32_le len][ body ]
body   = Command            (request)   // postcard or NDJSON — see below
       | Result<Rows | Tag, EngineError> (response)
```

- One canonical encoding across **wire + WAL** (replaces v1's *three* representations: SQL text in,
  bincode in WAL, JSON out over NOTIFY). DRY, fewer conversions, fewer clones.
- **NDJSON-first, postcard optional.** NDJSON needs no cross-language codegen (kills the Rust↔TS
  encoder-drift risk), is the debug format anyway, and `psql`-free. Decide binary-vs-NDJSON for the
  hot path by **one benchmark on batch-booking** — do not build both speculatively.
- Subscriptions replace `LISTEN/NOTIFY`: push native frames off the existing `NotifyHub` broadcast,
  dropping the per-subscription `serde_json::to_string` forwarder task (`wire.rs:790`).
- **Auth = per-connection authenticated handshake `{ tenant, credential }`** — full tenant isolation
  at the security layer, a fraction of the hot-path cost, and *no* per-op signing (per-op ed25519 is
  a federation-edge concern, forbidden in the single-node kernel).

### The SDK — `@open-tap/protocol`

`tap` is lightly refactored, not rebuilt. Reusable verbatim:

- `expandRecurrence` + `RecurrencePattern`/`RuleSegment` (`recurrence.ts`) — pure, transport-agnostic.
- Schedule bitmask + time helpers + their 20-case test suite (`schedules.ts`) — the only tested SDK unit.
- The **hold→confirm→release connection-lifecycle concept** (`server.ts:21-66`): socket-open = place
  hold, confirm = **`CommitHold` (atomic)**, socket-close = auto-release, expiry as backstop. Promote
  into a typed, validated `@open-tap/protocol` schema (kills the `msg: any` at `server.ts:21,43`).
- The sub-client facade shape (`client.ts:28-50`): generalize the `Options | Sql` DI seam to
  `Options | Transport`; swap raw-SQL bodies for framed `Command` calls.
- Client types (`types.ts`) — widen ms→µs; **codegen from the deltat structs** rather than hand-maintain.
- Read-back from the engine instead of fabricating returned objects locally (`resources.ts:20-23`).

Add: **idempotent commit** — the client-generated `Ulid` booking id is reused on retry, and the
authority treats a re-commit of an already-committed id as a success echo (Stripe-style). Closes the
lost-ack exactly-once gap. And a **cross-language wire round-trip test gating CI** so the encoder can
never silently corrupt the WAL or a frame.

---

## 5. The vision layer — designed as seams, built on demand

Everything below is **architecture, not code.** The trigger for building each is a *real* second
party, not an anticipated one. This is the entire point of honoring "I tried too hard."

### Federation (Phase 5 — only when a real second operator exists)

Adopt the **AT Protocol three-tier shape** (the research's closest fit for booking):

1. **Authoritative home node** = the v2 kernel. Each bookable resource has **exactly one home
   server = its single writer.** Every commit is a synchronous round-trip to that home. This is the
   *only* safe decomposition (see §6).
2. **Relay** = a firehose of **signed availability *summaries*** (never full state — Matrix's
   full-DAG replication is the explicit anti-pattern for high-churn availability; joining big rooms
   takes 10–60 min and can cripple small servers).
3. **Indexer / AppView** = builds network-wide search. **The index is always a stale hint, never a
   commit point.** It answers "where to look"; the home node answers "is the slot actually free."

Identity & discovery: **WebFinger / `.well-known`** resolves a booking handle (`studio@acme.com`) to
its home server's API + public key (RFC 7033). **DKIM/SPF-style signing**: every emitted record is
signed at origin so consumers verify *which* server produced it. **Nostr NIP-65 outbox** pattern lets
a known resource declare its authoritative server without any central index.

Two hard requirements both candidate designs underspecified — **gate any federation build on these:**

- **(a) A monotonic ownership epoch / fencing token** the commit path checks — otherwise "one home
  per resource" double-books on failover or `did:key` host migration.
- **(b) A per-resource sequence number on every emitted record + a nonce on every signed op** —
  *signature ≠ freshness*; a replayed-but-valid summary otherwise resurrects a gone slot.

No labeler/reputation layer and no payment escrow until a paying marketplace customer needs them.

### Geo discovery (Phase 6 — only when a real "near me" query exists)

The only geo-shaped code today is *stadium seat x/y geometry* — not geography. "Find X near me" has
zero users right now. When it lands:

- **Geo lives entirely in the indexer/AppView edge, never the kernel.** The kernel only ever knows
  ticks, intervals, capacity.
- **Separate orthogonal indexes, not a combined 4D index**: one spatial key for "near me" + one
  interval index for "free at T", intersect the candidate sets. (Bookable things are spatially static
  with discrete availability — a fused spatiotemporal index is premature.)
- **One spatial scheme: S2** (true spherical cells, clean parent/child containment, no
  antimeridian/pole seams). Not H3 *and* geohash — that double-machinery is premature. A signed
  `ResourceAnnounce { resource_id, node_url, s2_cell, category }`; a radius query computes the covering
  cell set and fans out **only** to the indexer shards owning those cells.
- Commit still goes synchronously to the authoritative home node.
- Body-agnostic addressing (per-body geodetic frame + body id) is documented, not built — same
  discipline as the time frame byte.

---

## 6. Why federated booking is hard — and the one model that works

The hard problem: don't double-book seat 1A when the seat and the booker live on different servers.

- **I-confluence (Bailis et al., VLDB 2015):** "no double-booking" is **not** invariant-confluent →
  commit-time coordination at the resource owner is **unavoidable**. You cannot CRDT your way out of a
  uniqueness/capacity invariant. Use this as the design filter.
- **Pat Helland, "Life Beyond Distributed Transactions" (CIDR 2007):** make each bookable unit an
  entity with a **single home**; never run a transaction across entities. This *is* the home-server
  invariant.
- **Try-Confirm-Cancel (TCC) escrow** = the cross-server handshake: booker's server calls **Try**
  (time-boxed hold) on the resource's home, does payment/etc., then **Confirm** or **Cancel**. This is
  exactly your hold→confirm pattern, made distributed. (And the reason `CommitHold` must be atomic.)
- **Idempotency keys (Stripe):** Try/Confirm keyed by a client-generated booking id, persisted in the
  same step as the state change. Retries are safe.
- **Bounded-counter / escrow CRDT (ElectricSQL):** use **only** for *fungible* capacity (N
  interchangeable general-admission seats) to allow low-latency local sells. **Never** for a specific
  named seat.
- **Saga + compensation:** for end-to-end flows spanning multiple homes + payment (hold → charge →
  ticket); failure compensates by cancelling holds. Cross-home multi-resource atomicity ("a room AND
  its projector on different homes") is **explicitly deferred and unsolved** — document it, don't fake it.

Net: **CP for commit, AP for discovery.** Both adversarial reviewers endorsed this split. The kernel is
already linearizable per resource (per-resource write lock); federation just preserves that by routing
every write to the one home.

---

## 7. Calendar standards — what to adopt, what to refuse

The research overwhelmingly validates deltat's core thesis: **keep the engine timezone-free,
recurrence-free, calendar-free.** Every standard that put timezones/recurrence *in the data model*
became a bug farm.

- **Adopt:** iCalendar's `FREEBUSY`/period-list + half-open interval thinking (RFC 5545); CalDAV's
  "server owns canonical state, answers free-busy" (minus WebDAV's XML weight); JMAP Calendars'
  busy-status precedence merge (`confirmed > unavailable > tentative`) as a model for layering.
- **Refuse:** `RRULE`/`EXDATE`/`RECURRENCE-ID` in storage — the single richest source of cross-vendor
  bugs (UNTIL-must-be-UTC vs tz'd DTSTART, DST wall-clock preservation, master/override reconciliation).
  Recurrence is **expanded at the edge** (`expandRecurrence`) into concrete segments. Storing only the
  absolute instant is the strongest possible position. `iTIP`/`iMIP` (RSVP/negotiation) is correctly
  out of scope.

---

## 8. Roadmap — testing first, wedge early, federate on demand

Reordered after the round-2 review. Two structural changes from the original plan: **a new Phase 0
(determinism + executable spec) precedes everything** — because without it v2 ships hollow like v1 —
and **payments is promoted to Phase 1.5** (it's the wedge, not a late federation feature). "Strip the
costume" (delete pgwire/SQL) is folded across Phases 0–3 since it's mostly deletion.

| Phase | Goal | Deliverable |
|---|---|---|
| **0 — Determinism + executable spec** *(NEW, precedes all)* | Make it testable for real so v2 can't ship hollow | `Clock` trait + WAL-fsync + TCP behind traits (route the ~77 `SystemTime::now`/`Ulid::new`/`now_ms()` sites through it; the reaper too); a CI lint banning direct clock/RNG/IO; the **hand-written** `NEVER-double-book` + `availability == capacity − allocations` invariants; a stateful proptest harness (engine vs a dumb reference model) gating CI; `cargo-mutants` as a gate. |
| **1 — Fix correctness, prove it with a *seed*** | Atomic reservation; regression is a seed, not a hand-written ordering | Atomic `CommitHold(hold_id)` (one lock, one event, excludes the named hold from the conflict check); authority-assigned opaque expiry; idempotent commit on `Ulid` retry; **decide the hold-capability/security model** (Open Q); a 2-actor `madsim`/`turmoil` sim whose scheduler reorders *across* the release→confirm boundary, proving the TOCTOU window is closed; WAL-replay determinism property. |
| **1.5 — Payment-backed commitment** *(PROMOTED — the wedge)* | Make a single node useful to a paying operator by killing no-shows | `hold→confirm→capture` state machine: Stripe `capture_method=manual` (short-horizon auth hold) + `setup_future_usage=off_session` mandate (long-horizon charge-on-event, modeled as a *fallible* async step); deposit/prepay/card-hold instruments; cancellation-window *policy* layer; **Stripe Connect direct-charge custody** (resource's PSP = merchant of record; protocol never custodies funds); fault-injection on lost-ack re-commit. |
| **2 — Fix efficiency** | Sub-ms at the 100k-interval ceiling | Max-end-augmented interval tree + `id→node` map behind `ResourceState`; `Span::new → Result`; the proptest model-vs-tree comparison guards the swap (any divergence = tree bug); benchmarks proving `O(log n)` overlap/remove and non-`O(N²)` replay. |
| **3 — µs time + SDK; SHIP single-node v2** | Sub-ms granularity, one protocol, **first paying operator live** | `ms→µs` widening *gated on* the metamorphic refinement + WAL-replay properties being green (it's a flag day); `@open-tap/protocol` package; demos ported to the framed transport; cross-language wire round-trip test gating CI. Freeze `name` as the single grandfathered business field with a no-second-field rule. Confirm a per-resource monotonic sequence is derivable in the WAL so federation stays a *seam*. |
| **4 — Identity + AI-native discovery seam** | Make supply agent-discoverable; draw the moat's first edge | Portable `Ulid`/`did:web` ids + `.well-known` resolution stubbed; a signed `/.well-known/bookable.json` manifest (schema.org graph as a **W3C Verifiable Credential**, JWS/Ed25519) for ONE node; an **MCP tool surface** (`search_bookable`/`get_availability`/`book`) + OpenAPI query endpoint over the single node — agent-readable supply with *zero* federation; a **Cal.com adapter** as the supply wedge. Key-management design written, not wired. |
| **5 — Federation** | *Only on a real 2nd operator sharing supply with #1* | Relay firehose of signed availability **summaries**; synchronous signed commit to the home node; monotonic ownership epoch / fencing token; per-resource seq + per-op nonce vs replay; **ACP/AP2 handoff at the book step**; registry-of-pointers (ANS-style) so no party is the gatekeeper; index = stale hint. |
| **6 — Geo discovery** | *Only on a real near-me query* | Indexer/AppView over the firehose; **one** spatial index (S2); cell-covering fan-out to home shards; commit still synchronous to home; geo lives entirely in the AppView edge, never the kernel. |

The first *shippable, sellable* milestone is the end of **Phase 3**: a clean single node with atomic,
provably-no-double-book bookings and deposit-backed no-show prevention. Everything from Phase 4 on is
net-new edge processes behind documented seams — which is precisely why this stays a *refactor*, not a
rewrite.

---

## 9. Open questions (decide before the relevant phase)

1. **Hold capability model — decide in Phase 1, do not defer.** Does possession of a `Ulid` `hold_id`
   authorize `CommitHold`, or must the placing connection commit it? A guessable/leakable `Ulid` that
   converts *someone else's* hold into a booking is a slot-hijack vector. This is a *security* decision;
   if "possession authorizes," a hold needs a capability secret, not a bare `Ulid`.
2. **Who is the first real operator?** The honest wedge needs ONE operator with a *quantified* no-show
   ledger (a clinic, a salon chain, a high-end restaurant group, or a Cal.com power-user). Mom-Test them
   on real numbers — no-show rate, lost revenue, what they pay today to prevent it — *before* building
   the payment instrument mix in Phase 1.5.
3. **DST effort cap.** Full FoundationDB/TigerBeetle-grade simulation for a *single-node* store is its
   own "I tried too hard" costume. Agreed ceiling: stateful proptest first (days, high ROI), a small
   seeded multi-actor loop second, `loom` only for the lock primitive — explicitly **not** a bespoke
   deterministic hypervisor.
4. **NDJSON vs binary postcard** for the wire — decide by one benchmark on the batch-booking hot path.
   Don't build both.
5. **ms→µs WAL migration** is a flag day — confirm no external consumer has byte-persisted data (audit
   found no released wire clients; verify before Phase 3). Same pass: confirm a per-resource monotonic
   sequence number is derivable/cheaply addable so federation (Phase 5) is a seam, not a re-format.
6. **Signed-but-stale availability / liability.** Signatures prove *provenance*, not *truthfulness*.
   Decide slot TTLs, idempotent confirmation against the kernel, and a revocation/reputation mechanism
   *before* opening the discovery layer — or agents book ghost slots and the protocol gets blamed.
7. **Standards-convergence bet.** ACP vs AP2 vs UCP, competing `/.well-known` conventions, multiple DID
   methods. Strategy = a thin composition layer over the most-adopted primitives (schema.org + VC/DID +
   MCP + ACP/AP2 handoff), date-versioned, rather than betting on a single winner.
8. **Key rotation** (`did:plc`-style) + historical-booking signature verification needs a published
   rotation log — scope only when Phase 5 triggers.
9. **Cross-node multi-resource atomicity** ("a room AND its projector on different homes") has no
   2PC/saga story — explicitly deferred and documented as unsolved until a real use case appears.
10. **Does any demo actually need `capacity>1` + buffer at scale**, or is `capacity==1` the only hot
    path? Confirms whether the interval-tree treatment is needed for the sweep-line path or only the
    cap==1 fast path.

---

## 10. Make it actually work — why v1 felt hollow, and the cure

> "Unit and integration tests passed, but it felt off and things were missing." — this is now
> **diagnosed, not mysterious.**

### Why it felt off (proven in source)

The hold→confirm bug is the whole story in miniature. `place_hold`, `release_hold`, and
`confirm_booking` each pass their own unit tests — *individually* they are correct. The bug exists
**only in the interleaving of three correct functions across an RPC seam**, and the test suite cannot
see it:

- The 4,748-line / 178-test suite is **sequential**.
- Its *one* "concurrency" test (`group_commit_batches_appends`, `tests.rs:3699`) spawns 20 tasks that
  each create a **different** resource (`Ulid::new()`, `format!("R{i}")`) — so by construction it never
  has two clients contend for the *same* slot.
- `Cargo.toml` has **zero** property / simulation / concurrency-checker dependencies.

This is the textbook LLM-built failure mode: **tests validate what each function *does* in isolation,
never what the *system must guarantee* across the seam.** LLM-generated tests kill only ~20% of injected
mutants — coverage climbs while defect-detection collapses. More unit tests would reproduce the same
hollowness.

### The cure: write the missing artifact — an *executable specification*

The fix is not more tests of the code; it's a small spec **written by hand, independent of the code**,
made a **CI gate**. deltat is almost perfectly pre-adapted to this because it's event-sourced
(`apply_event`, `store.rs:96-206`, is the single funnel for both replay and live writes), uses integer
time (a seeded counter trivially replaces the clock), and serializes per resource (legal interleavings
are a small, enumerable set). Priority order:

| Pri | Technique | What it catches |
|---|---|---|
| **P0** | **Hand-written invariants**, checked after every committed event: `NEVER double-book` (∀ resource, ∀ instant: count(active Bookings overlapping) ≤ capacity) and its sibling `derived availability + active allocations == capacity` | The entire double-booking class, including today's TOCTOU. *Must not be generated from the engine* — that re-encodes the bug. |
| **P0** | **Stateful model-based property test** (`proptest`): drive identical random op-sequences through the real engine **and a dumb `HashMap<resource, Vec<non-overlapping spans>>` reference model**; assert agreement after every op; shrink failures | The seam bug **and** the Phase-2 interval-tree swap (any clever-tree vs dumb-model divergence = a tree bug, e.g. wrong max-end on rebalance). This *is* the executable spec v1 lacked. |
| **P1** | **Seeded multi-actor deterministic simulation** (`madsim`/`turmoil`): N bookers contend for one cap-1 slot; the scheduler **reorders frames *across* the release→confirm boundary**; loop thousands of seeds | The interleavings no human imagined. **Critical:** the scheduler must cross RPC boundaries or the per-resource lock serializes everything → false green. The regression artifact is a **seed**, not a hand-written ordering. |
| **P1** | **`cargo-mutants` as a CI gate** | The objective signal separating a real spec from coverage theater. A surviving mutant = an untested behavior. |
| **P1** | **WAL-replay determinism** property: apply random history, snapshot, replay cold, assert byte-identical | The event-sourcing invariant; also gates the ms→µs migration against off-by-1000 / truncation. |
| **P2** | **Fault injection** (Buggify-style): drop the WAL ack after fsync; crash between `wal_append` and `apply_event`; jump the clock backward | Validates the *fixes themselves* — idempotent re-commit, atomic batch replay, authority-assigned expiry under skew. Unit tests structurally can't reach these (one happy clock, a disk that never lies). |
| **P2** | **Metamorphic tests** for availability: composition (`avail[t0,t2] == avail[t0,t1] ∪ avail[t1,t2]`), add/cancel round-trip identity, batch-reorder commutativity, ms→µs refinement-preservation | Sweep-line bugs where the exact answer is hard to state but *relations* are easy. |
| **P3** | **`loom`** (exhaustive) on the `batch_confirm_bookings` sorted-lock two-phase commit only | Deadlock/race in the lock primitive. Reserve for that snippet only — over-applying it is its own "tried too hard" trap. |

**The single most important edit this implies:** the original Phase-1 line "tests covering the old
TOCTOU race" is itself the trap — a hand-authored interleaving is exactly what let v1 ship hollow. The
regression for that race must be **a failing seed from the simulation harness**, not a human guess.

---

## 11. Complexity: what's deleted vs what's relocated

**The rule, in one line:** *accidental* complexity (pgwire + SQL) is **deleted** and never needed
again; *essential* complexity (identity, business-data, discovery, payments, federation) is
**relocated** into clean layers, never pulled into the kernel.

Run every piece through Brooks' essence-vs-accident test (*No Silver Bullet*). pgwire + sql.rs +
sqlparser = 2,771 LOC, verified larger than the engine, solving **no problem the product has now or in
the vision** — it parses a full SQL grammar to recover ~6 scalars into a fixed 24-verb enum, then
re-encodes that enum back to Postgres rows. Pure accident. **Delete with zero regret.** v1's mistake was
*not* missing features — it was a huge accidental wrapper plus *zero* essential vision features. The v2
mistake to avoid is the inverse: pulling essential complexity *into* the kernel.

### The kernel admission rule (freeze this in writing)

> **A field may enter the kernel ONLY if computing derived availability for a single resource is
> impossible without it.** Availability is a pure function of `(intervals, capacity, buffer_after,
> query_span, now)`. If a proposed field is not an argument to that function, it does not
> belong in the kernel. *(`schedule` was removed — recurrence is edge rules; see REQUIREMENTS MODEL-07/EDGE-03.)*

That admits ticks, spans, capacity, buffer, hold-expiry. It **forbids forever** anything
descriptive (specialty, price, photo, category), geographic (lat/lng, S2 cell), monetary, or
reputational. The one live slippery slope: `name: Option<String>` stays (ripping it out is busywork) but
is **frozen as the single grandfathered exception**, with a written "no *second* descriptive field may
ever be added to the kernel" rule + a review-checklist/clippy flag on any new `String` on
`ResourceState`/`ResourceInfo`/`Event`. A slippery slope converted to a one-line policy.

### The six-layer architecture (boundaries drawn now, layers built on trigger)

Drawing a boundary is **free** (a naming + dependency-direction decision); building a layer is
**expensive** (done only when a real party triggers its phase). Draw them now because *merging* a
concern into the kernel is irreversible (it corrupts the WAL/event format and breaks per-resource
linearizability), while leaving a boundary undrawn forces a future un-merge = a rewrite.

| # | Layer | Owns | Built |
|---|---|---|---|
| **1** | **Time/Availability kernel ("tapd")** | i64-µs instants, spans, the unified `Interval{kind}`, `ResourceState`, the interval tree, WAL, per-resource linearizability, atomic `CommitHold`, *derived* availability. **Knows only ticks + ids + capacity.** The only CP/single-writer layer. | Phases 0–3 |
| **2** | **Identity** | resource-id ↔ portable identity (a `Ulid` today; `did:web` / `.well-known` / key material at Phase 4), home-server resolution | Phase 4 |
| **3** | **Business-data / profile** | the doctor's specialty, the room's photos/price/amenities — rich, mutable, queryable, **keyed by resource-id**. *This is your "business data in other DBs."* | When a product needs richer-than-`name` |
| **4** | **Discovery / search (AppView)** | geo (S2) + category + availability-**hint** index from a firehose. Answers *where to look*, **never** a commit point (I-confluence). AP/stale is fine. | Phase 6 |
| **5** | **Payments / commitment** | charges, escrow, idempotency ledger; drives the kernel via `Try`(`place_hold`) → `Confirm`(`CommitHold`) / `Cancel`(`release`). **The kernel learns nothing about money.** | Phase 1.5 |
| **6** | **Federation transport (Relay)** | signed availability *summaries*, the firehose, ownership-epoch/fencing tokens, per-resource seq + per-op nonce. Routes every **write** synchronously to the resource's home kernel. | Phase 5 |

**Direction of dependency is the whole game.** Every upper layer references *into* the kernel by stable
resource-id (the identity-layer portable id from Phase 4 on); the kernel holds **no foreign key out** —
no `profile_id`, no `listing_id`, no `price_id` on `ResourceState`, ever (Dependency Inversion: the
kernel is the stable abstraction everything depends on, depending on nothing).

### Your "separate DBs, reference by ID" instinct — validated, with two refinements

It's correct and ~99% already implemented (the only kernel leak is `name`). It's a recognized pattern
(Helland's single-home entity; DDD bounded contexts), not a hack. Two refinements:

1. **Join on the *identity-layer portable id*, not a kernel-internal offset** — even though that id is
   just a `Ulid` today. Otherwise a Phase-5 home-server migration silently orphans every profile/
   payment/listing record. Key business data on the portable id from day one.
2. **The reference is strictly one-directional, into the kernel** (see above).
   *Guardrail:* don't over-apply "business data goes elsewhere" to `schedule`/`capacity`/`buffer_after`
   — they look business-y but are **arguments to the availability function**, so they stay in. The
   admission-rule function-test draws the line correctly.

---

## 12. The moat and the wedge

**The moat is not efficiency and not the kernel.** It is being the **open, federated, signed,
AI-native index of bookable supply, with payment-backed commitment that kills no-shows.** As of mid-2026
the agent-commerce stack is bifurcated and *discovery is the unclaimed leg*: schema.org gives the
vocabulary, **ACP** (OpenAI + Stripe) and **AP2** (Google + ~60 partners) solve agentic *payment*,
**A2A + MCP** solve agent *transport* — all with giant sponsors seeding both sides. **Nobody owns open,
federated, cross-vendor discovery of "bookable X near me with live availability."** ACP explicitly stops
at the transaction *after* merchant identification. That gap is the moat-shaped object.

**Why open+federated+signed+AI-native beats the silos:** Reserve-with-Google, Apple Business Connect,
OpenTable, Resy optimize for a *human* in *their* app under a *partner contract*; the self-serve
Reserve-with-Google API was discontinued July 2024; an independent agent can only book an OpenTable
table by impersonating a browser. A silo *structurally cannot* offer cross-vendor breadth (it has only
its contracted merchants) or an open query API (that disintermediates its own surface). The federation
wins on exactly those axes — breadth (any merchant, self-hosted Cal.com instances as first-class),
neutrality (no gatekeeper contract), verifiable trust (merchant-signed descriptions using the *same*
JWS/Ed25519/VC/DID crypto AP2 and A2A already mandate), and AI-nativeness.

### But the federation is the *destination*, not the first move

"People adopt it because it's so good" is a non-falsifiable belief, not a go-to-market. A federation has
**near-zero value at n=1 and the weakest cold-start of the entire stack.** Email and the web won because
each node had **standalone value before any network existed**. This design's standalone payoff is
**no-show prevention** — and the evidence is overwhelming and *quantified*:

- OpenTable deposits cut no-shows **~57%**; card holds **~16%**; hotel guarantees take them to **~5%**.
- Healthcare no-shows run **23–33%**, ~**$150B/yr**.
- It's a problem operators **already pay to solve.**

So: **ship a single-tenant, payment-backed product first** (useful with zero other nodes), then **turn
on federation for customer #2 who shares supply with customer #1** — never before. Solve cold-start by
refusing to have one.

### The AI-native protocol — three composable interfaces, reuse don't invent

Concentrate *all* original work on the discovery+query layer; reuse standards for the rest.

1. **DISCOVER** — a signed, self-describing manifest at `/.well-known/bookable.json` (mirroring A2A
   signed Agent Cards). It declares resource type, geo (S2 cell), category, constraints, the resource's
   portable DID, capacity model, and the query/book endpoints — as a **W3C Verifiable Credential**: a
   schema.org graph (`Place`/`LocalBusiness`/`Service` + `makesOffer` + `potentialAction:ReserveAction`)
   signed with the merchant's DID (JWS/Ed25519). Identical crypto to A2A/AP2 → one shared trust fabric,
   review is "the accepted model applied to inventory," not novel crypto.
2. **QUERY** — *the genuinely new artifact.* Request = `{resourceType, geo (radius/bbox),
   timeWindow{start,end}, partySize|duration, typed constraints (price ceiling, attributes,
   cancellation policy, accessibility)}`. Response = a **signed** list of schema.org `Offer`/slot objects
   with a **short TTL** (signature proves provenance, *not* freshness → pair with idempotent confirmation
   against the kernel so an agent never books a ghost slot). Publish as **both** an OpenAPI REST endpoint
   **and an MCP tool surface** (`search_bookable`/`get_availability`/`book`), date-versioned like ACP.
3. **BOOK** — a schema.org `ReserveAction` whose `EntryPoint` hands off to an **ACP** checkout and/or an
   **AP2** Cart+Payment mandate; the buyer's scoped funding token settles. Payment-backed,
   non-repudiable, liability resolved by the mandate trail. **Never touch raw card data.**

Federation = a **registry-of-pointers, not a central inventory DB** (ANS-style DNS+PKI): aggregators
hold `{DID, geo, resourceType, manifest URL}`; "near here" fans out to merchant-hosted manifests;
results are merchant-signed so an aggregator can't forge price/availability. Anyone can run an
aggregator; the *protocol*, not a company, owns the namespace. This maps onto the AT-Protocol three-tier
shape already in §5: kernel = PDS, federation = Relay, discovery = AppView.

### No-shows → the `hold→confirm→capture` state machine (= TCC + payments)

`REQUESTED → HELD (instrument secured) → CONFIRMED → [CONSUMED | NO_SHOW | CANCELLED_HONORED |
CANCELLED_LATE] → SETTLED`. `Try` = `place_hold`; the payment layer runs the charge in **its own** ledger
keyed by the kernel's `Ulid` idempotency key; `Confirm` = atomic `CommitHold`; `Cancel` = `release`. The
kernel never learns what a charge is — which is *why* atomic `CommitHold` is a **prerequisite** for
payments, not just a bug fix. A hard physical constraint forces **two modes** because auth holds expire
(~5–7 days):

- **Short-horizon** (within the auth window): live auth hold via Stripe `capture_method=manual` →
  capture on no-show (optionally a partial fee via `amount_to_capture`), cancel to release. This is the
  literal "escrow" — funds reserved on the booker's card via the *resource's* PSP, nothing pooled.
- **Long-horizon** (weeks out — the common appointment case): save a card with an up-front mandate
  (`setup_future_usage=off_session`), charge at/after the appointment. Model "charge no-show fee" as a
  **fallible async step** (SCA exemptions aren't guaranteed; issuers can decline).

**Federated custody — the protocol never holds funds.** Stripe Connect **direct charges**: the
resource's home server / its PSP is the merchant of record; funds settle there; disputes hit the
*connected* account; the protocol takes at most an `application_fee` and is out of the
money-transmission/liability path. "Escrow" is *logical*, decided by the resource's single-writer home
node — never a pooled account.

---

## 13. Scale, language, and "never change for 100 years"

*(From the capacity/longevity review. Three of the four scale goals were category errors as literally
stated; all three have a true, achievable reframing.)*

### "Billions of resources in one instance" — reframe to be true

- **Billions of *resources*: false.** A `ResourceState` (id + `Arc<RwLock>` + map bucket + `name`) is
  ~200–500 B → a node holds **tens-to-low-hundreds of millions** of bookable things. (Today's
  `limits.rs` caps a process at ~1e8.)
- **Billions of *intervals* (booking/hold history): true.** ~56–64 B/interval → 1e9 ≈ 60 GB, 5e9 ≈ 300
  GB. *Note the Phase-2 interval tree makes per-interval density ~2–2.5× worse* (pointers + augmented
  max-end + id→node map) — a real tradeoff to measure.
- **The missing pillar that the literal goal needs: index-in-RAM + interval-bodies-on-NVMe** (the
  **Aerospike hybrid pattern** — ~64 B index/record in RAM, body on flash; their petabyte benchmark hit
  **sub-ms P99 (0.92–0.99 ms) at ~200–250k TPS/node**). With it, **tens of billions of intervals/node at
  sub-ms P99.** *New Phase 2.5*, built only when a node's RAM is the proven binding constraint — but the
  spill must be a real seam in `ResourceState`'s storage interface in Phase 2, not a paragraph. Same
  cold/hot boundary serves **history compaction** (snapshot + segmented WAL so cold-start is O(working
  set), not O(all history ever) — fixing the O(N²) replay).

### "Sub-millisecond" and "7–50 instances for everyone" — physics, not code

- Sub-ms is **true for RAM-resident reads and batched writes**; a single *durable* commit is fsync-bound
  (~0.3–2 ms) and only amortizes to sub-ms under group commit.
- **Cross-continent commit is speed-of-light bound (~100–250 ms RTT)** — no language or index fixes
  that. So sub-ms is honestly an **in-region** promise, each resource committing at its one home.
- Storage-wise, 8.1B resources fit in **~10–100 home nodes** trivially (~80–800M resources/shard); "7"
  is too few for cross-continent *latency*, but the *count* is right for *storage*.

### The binding constraint is **memory → I/O, never CPU**

Priority order: (1) **RAM bytes-per-interval** (no spill path exists today — this is the hard ceiling on
objects/node); (2) **fsync-bound durable-write throughput** (one `wal_writer_loop` per tenant, one
`flush_sync` per batch — a single hot tenant is capped at one core's batch-fsync rate); (3) NVMe queue
depth, network. **CPU is over-provisioned by ~100–1000×.**

### Language verdict: **stay on Rust. "Faster language" is a category error.**

- The only large, reproducible latency win across *every* studied rewrite (Discord Go→Rust killed
  10–40 ms GC spikes; Scylla/Redpanda beat the JVM mainly on GC tail) was **eliminating the GC** —
  which Rust already has none of. You're already on the right side of the only line that ever mattered.
- Among no-GC native languages (Rust/C++/Zig) there is **no meaningful runtime gap** (within ~5–10%,
  trading wins). A TigerBeetle engineer says picking Zig was "fewer moving parts, not performance."
- The kill-shot: under sustained load, Redpanda's **C++** engine hit **~26-second p99.99** due to NVMe
  SSD garbage collection. **Once you're I/O-bound, the language is irrelevant.** Chasing language
  micro-speed polishes the one bottleneck that is provably not binding.
- Tiebreakers for a decades-lived store all favor Rust: compile-time temporal+spatial memory safety (now
  *mandated* by CISA/NSA for new long-lived infra — C++ lacks it, Zig only partial), ecosystem/hiring
  longevity, and correctness tooling. `redb` proves pure Rust reaches LMDB-class storage perf, so
  there's no performance reason to drop to C++/Zig. (Adopt Zig *only* for a TigerBeetle-shaped
  single-threaded static-allocation ledger — not this.)

### "Never change for 100 years" = freeze the **format**, not the binary

The binary, index, and storage engine *will and should* be rewritten many times. What you publish as a
**versioned, conformance-tested format spec** and freeze for a century: (1) the instant primitive (i64
µs from the Unix epoch, UTC, carrying no calendar assumption); (2) the unified `Interval{id, span, kind}`
model; (3) "availability is **derived**, never stored"; (4) single-home-per-resource commit (forced by
I-confluence, won't reverse). Add a 1-byte version/frame prefix + a cross-language round-trip
conformance test gating CI. That's how IPv4/TCP/DNS/Unix-time/the SQLite file format lasted — minimal
format + version seam, spec separated from implementation. **The longevity predictor the research
actually credits is deterministic simulation testing (§10), not language speed** — FoundationDB ran in
simulation ~18 months before going live.

### Pillars to add to the design (beyond what was already there)

| Pillar | Why | Where |
|---|---|---|
| **Index-in-RAM + bodies-on-NVMe spill** (Aerospike hybrid) | The one thing the literal "billions/node, in memory" goal requires | New **Phase 2.5** (seam in `ResourceState` storage interface at Phase 2) |
| **Snapshot + segmented/checkpointed WAL** | Cold-start = O(working set), not O(all history); fixes O(N²) replay | Folds into Phase 2.5 (same hot/cold boundary) |
| **Deterministic simulation testing** | The real decades-longevity guarantee (FDB/TigerBeetle) | **Phase 0** + a gate on the Phase-3 ship (see §10) |

---

## 14. Reality check — what the lessons actually validate, what's a bet

*An honest audit against the prior art, so we don't fool ourselves. The point of this section is the
middle column (bets) and the right column (dissent), not the left.*

### Strongly validated by other projects (build with confidence)

- **Kill the SQL/pgwire costume** (as the *core* — keep SQL as an optional adapter, see §15). Pure
  KISS; the audit is the proof: 2,771 LOC (larger than the engine) to round-trip SQL text through a
  full grammar and recover ~6 scalars into a fixed 24-verb typed `Command`, then re-encode it as
  Postgres rows. Accidental complexity, not dead code (the extended path is live).
- **Single-home-per-resource, CP-for-commit / AP-for-discovery.** This is as *settled as distributed
  systems gets*: I-confluence (Bailis, VLDB 2015) **proves** no-double-book requires coordination at the
  owner — you cannot CRDT it. Helland's single-home entity is the same conclusion from 2007. Not a
  preference; a theorem.
- **Deterministic simulation testing to make it actually work.** FoundationDB ran ~18 months in
  simulation before launch; TigerBeetle's VOPR is its whole reliability story. deltat is *unusually*
  well-suited (event-sourced, integer time, one `apply_event` funnel). This directly answers the v1
  "felt off" failure.
- **Rust, not a "faster" language.** Discord Go→Rust (GC spikes), Scylla/Redpanda vs JVM (GC tail), and
  the Redpanda-C++-hit-26s-p99.99-under-SSD-GC kill-shot (IO-bound → language irrelevant) all point one
  way. `redb` proves pure Rust = LMDB-class. Decisive.
- **Billions via hybrid RAM-index + NVMe-body** (Aerospike: 64 B/record, petabyte bench at sub-ms P99),
  and **augmented interval tree** (CLRS canonical) for the verified O(N) flaw. Standard, proven.
- **No-shows via payment-backed commitment**, custody-free (Stripe Connect direct charges). The numbers
  are real and quantified (OpenTable deposits −57%; healthcare 23–33% / ~$150B). Operators already pay
  to solve this.

### Bets the prior art does NOT settle (where we could be wrong)

- **The moat thesis — that an *open federation* becomes the AI booking layer.** The *gap* is real
  (nobody owns federated discovery today). That an *open, neutral* protocol wins it is a **bet**, not a
  precedent. The base rate is against federation: email/web/DNS won, but Diaspora, Solid, and most
  fediverse projects stayed niche; ActivityPub only broke out on Twitter's collapse. The likelier threat
  isn't another startup — it's **OpenAI/Google vertically integrating their own booking graph** and
  never federating. *This is the single biggest unvalidated assumption in the whole plan.*
- **That a real operator will pay.** The entire "ship single-node no-show product, federate for customer
  #2" wedge rests on one un-run Mom-Test. We have **zero** validation that a specific clinic / salon /
  restaurant group will pay to prevent no-shows *via this*. Until that conversation happens with real
  numbers, the wedge is a hypothesis.
- **That we'll actually execute the testing discipline.** DST is only as good as the hand-written
  invariants and the scheduler's interleaving coverage. The failure mode is a v2 that's *all-green and
  still subtly wrong* — the same hollowness one level up. We've *named* false-green as risk #1; naming
  isn't doing.

### Where the textbook actively disagrees with us (defensible, but flagged)

- **µs-UTC instead of TAI.** The time research recommended storing TAI (monotonic SI) with UTC as a
  projection. We chose UTC for a real reason (only hold-expiry compares to `now`; TAI would *add*
  leap-second surface at every edge). Defensible, and the frame seam keeps TAI available later — but a
  system claiming "100-year correctness" storing leap-lossy UTC is a small, conscious compromise. Named,
  not hidden.
- **Hand-rolled interval tree vs the GiST-exclusion-constraint baseline.** The interval-DS research said
  "make Postgres GiST the baseline to *beat* before hand-rolling a tree." Our justification (in-memory
  sub-ms + owning the WAL/event model) is sound, but we are skipping the step of proving the baseline
  insufficient. Cheap to honor: benchmark against it once.
- **Refactor, not rewrite.** We bet the v1 engine core is sound and only the wrapper rotted. The
  proptest-vs-dumb-reference-model (Phase 0) is *exactly* the instrument that will confirm or refute that
  bet — good that the check is built in, but it is a bet until the harness is green.

### The meta-risk (the one that actually killed v1)

v1 died of "too much / tried too hard." The plan is disciplined about *features* (defer
federation/geo/payments-as-layers until a real trigger). But this conversation has produced **three
design docs and zero lines of shipped code** — and writing a detailed 100-year `FORMAT.md` before a
single paying user is, itself, a candidate for the same trap one level up. The honest tell of whether
we're "doing the right thing" is **not another design artifact** — it is PR1→PR3: the walking skeleton
with the executable spec and a *red* contention seed. Everything in these docs is theory until the
simulator turns a real bug red and the atomic fix turns it green. **Bias to building the smallest
end-to-end slice next; treat further planning as the thing to be suspicious of.**

---

## 15. Interface strategy & model generality (resolved)

### Interface — layered, not "delete SQL". SQL-the-language ≠ pgwire-the-protocol.

The universal-interface goal is real and kept. The mistake was making *one heavy protocol* the core.
The resolution is **one canonical core + thin adapters**:

- **Core: the framed `Command` protocol** (`[magic|version|flags|len|body]`, NDJSON-default,
  postcard-optional, same encoding as the WAL). One vocabulary, source of truth. Build now.
- **HTTP/JSON adapter** — the *universal external surface* (POST a Command as JSON; GET cacheable
  availability). Build early (Phase 3–4). This, not pgwire, is the real "anybody can connect / host
  anywhere / fast remote access" answer for the edge+agent world.
- **MCP tool surface** (`search_bookable`/`get_availability`/`book`) — the AI-native interface. Build
  Phase 4. In 2026 agents call *typed tools*, not raw SQL; a 3-tool typed contract is **more reliable**
  for an agent than free-form text-to-SQL (~73–77% execution accuracy on real schemas, BIRD 2025).
  "AI knows SQL" is, in disguise, "AI wants a low-ambiguity action surface" — MCP gives that.
- **pgwire-compat adapter** — OPTIONAL, build-time-gated, separately shipped, **read-only**
  SQL-subset→`Command`. Built only when a real `psql`/ORM/BI customer asks. The genuine value pgwire
  buys is *SQL-tooling reach* (Tableau/Metabase/DBeaver) — a BI concern, not the booking hot path. Do
  **not** rebuild the extended-query/type-OID machinery.
- **Skip:** gRPC (its win is captured by postcard; deltat is fsync/RTT-bound, not serialization-bound)
  and a bespoke "thin SQL over a simple transport" (reaches no tools without pgwire framing, and is
  redundant with MCP).

**Fastest remote access — the ladder** (round-trip *count* and *connection reuse* dominate framing by an
order of magnitude): (1) **connection reuse** — one `{tenant,credential}` handshake then ~1 RTT/op,
vs pgwire's ~8–9 cold round-trips (TCP + 2 pgwire-auth + 1–2 TLS) before the first query; (2) **one RTT
per logical op** — atomic `CommitHold` already collapses the old release-then-confirm two-RTT race into
one; (3) **edge-cached availability *hints*** from a read-replica/POP near the caller — turns a
cross-region 100–250 ms RTT into a sub-10 ms read, legitimate *only* because availability is a derived
AP hint (the CP commit still routes synchronously to the resource's one home node); (4) **postcard**
last-mile only, by benchmark. Evidence: Neon ~10 ms HTTP (cached) vs ~37 ms cold pgwire; ClickHouse
HTTP-keepalive 14k QPS > native TCP 10k QPS on `SELECT 1`. Cross-region commit is speed-of-light bound;
the interface's only job is to add as little as possible on top of that floor.

### Generality — "is it 2D collisions?" and the zero-sum case

**It is not literal 2-D.** deltat is **N coupled 1-D interval timelines keyed by an opaque resource id,
bound by batch atomicity.** A conflict is named by a `(resource_id, time_span)` pair, but the
resource axis is **categorical (a lock key), not metric** — you cannot range-query "what's free in the
rectangle [seats 10–20] × [2–4pm]" as one operation; that's N point-lookups intersected by the caller.
`check_no_conflict` and `overlapping()` operate purely on time-span overlap within *one* resource;
`batch_confirm_bookings` runs an *independent* 1-D check per resource plus an intra-batch pairwise test
— not a single 2-D sweep. The **atomicity, not the dimensionality, is the load-bearing property**, and
"N coupled 1-D checks + atomicity" is a *stronger, more accurate* claim than "2-D collision."

**Zero-sum (book the table ⇒ consume the table AND my calendar, atomically):**
- **Single-node: fully solved today.** `batch_confirm_bookings([(b1, table, S), (b2, my_cal, S)])` —
  sort+dedup the resource ids, write-lock both in that fixed order, validate *all* (Err ⇒ zero events,
  zero mutation), then commit *all* while still holding both locks. In-process two-phase commit over
  local locks: both-or-neither. (Caveat: a "hold both, *then* confirm both" flow isn't atomic on the
  hold-transfer until `CommitHold` lands — PR5.)
- **Cross-node: deferred and unsolved**, and it's your *common* case (my calendar on my server, the
  table on the venue's). The single-lock guarantee can't span engines; no-double-book isn't
  invariant-confluent, so it degrades to a best-effort Saga with a visible window + compensation.
  **Practical mitigation until Phase 5: co-locate a booker's personal calendar with the resources they
  book.** Don't conflate *can-express* with *can-guarantee* — the ACID zero-sum headline is single-engine
  only.

**The use cases all map to the same primitives:** stadium = seat-resources (capacity 1) under a Section
parent; plane = same under a Cabin parent (grouping is just a different `parent_id` tree, *one*
mechanism); personal schedule = one capacity-1 resource; general-admission = one resource, capacity N.

**Genuine gaps — all correctly *outside* the kernel** (the admission rule exiles them to the
AppView/identity edge; listed so they're not forgotten):
- **Adjacency / "2 adjacent seats" / "contiguous block of 4"** — no coordinates/neighbors in the kernel.
  Resolve client-side, commit the chosen set via `batch_confirm_bookings`.
- **"Reserve any k of these N named seats" atomically** — no single verb; today it's query-then-batch
  (safe-but-may-fail TOCTOU, never unsafe). *Open question:* is a kernel `reserve-k-of-N` verb wanted, or
  is compose-over-batch acceptable?
- **Overlapping / many-to-many groups** (a seat in a price-tier *and* a physical-section group) — single
  `parent_id` is a strict forest. If real, add a **Layer-2/3 tag index** resolving to id-lists — never a
  second parent pointer in the kernel.
- **Non-tree / DAG resources** (a divider wall shared by two rooms) — inheritance assumes one ancestor
  chain. Out of kernel scope.
- **Group-as-aggregate** ("how many free in Section S") — `get_children` exists internally but isn't a
  query verb; surface it as a rollup (small follow-up).

---

## Appendix — prior-art citations

- **Functionality / testing:** TigerBeetle VOPR; FoundationDB simulation (Flow); Antithesis; `madsim`,
  `turmoil`, `loom`, `shuttle` (Rust); `proptest`/`quickcheck` (+ John Hughes' QuickCheck telecom bugs);
  `cargo-mutants` / mutation testing; metamorphic testing (compilers/DBMS); Buggify fault injection.
- **Complexity:** Fred Brooks, *No Silver Bullet* (essence vs accident); Eric Evans, DDD bounded
  contexts; Pat Helland single-home entity (CIDR 2007).
- **Moat / AI commerce:** schema.org `Reservation`/`Action`/`Offer`; W3C Verifiable Credentials + DID +
  JWS/Ed25519; MCP; A2A (signed Agent Cards); OpenAI/Stripe **ACP**; Google **AP2** (+ Visa Intelligent
  Commerce / Mastercard Agentic Tokens); Reserve-with-Google (self-serve API discontinued 2024-07);
  Apple Business Connect; ANS (Agent Name Service) DNS+PKI; Cal.com (open supply wedge).
- **No-shows / payments:** Stripe `capture_method=manual` (auth/capture), `setup_future_usage`,
  partial capture; Stripe Connect direct vs destination charges; OpenTable/Resy/SimplyBook/Square
  Appointments/Cliniko deposit + no-show-fee mechanics; card-auth expiry windows (Visa MIT ~5 days).
- **Scale / language:** Aerospike Hybrid Memory + petabyte benchmark; ScyllaDB/Seastar shard-per-core;
  Redis per-key overhead; Dragonfly; VoltDB; RocksDB (LSM) vs LMDB/`redb`/`sled` (mmap B-tree); NVMe
  queue-depth behavior; Marc Brooker "a database for SSDs"; Discord Go→Rust; Redpanda vs Kafka
  (Vanlightly reproduction); TigerBeetle (Zig); FoundationDB (C++/Flow); CISA/NSA memory-safety guidance.

### Original prior-art (rounds 1)

- **Federation:** AT Protocol (PDS / Relay / AppView / DID / Lexicon); WebFinger (RFC 7033) +
  ActivityPub; Nostr NIP-65 outbox; Matrix State Resolution v2 (full-state replication = anti-pattern);
  SMTP + DNS MX + SPF/DKIM/DMARC (origin signing); XMPP DNS-SRV + Dialback (DNS-only auth is weak).
- **Calendar:** iCalendar RFC 5545; CalDAV RFC 4791 + Scheduling RFC 6638; JSCalendar RFC 8984; JMAP
  Calendars (draft-ietf-jmap-calendars); iTIP RFC 5546 / iMIP RFC 6047; schema.org Reservation.
- **Interval data structures:** Augmented-BST interval tree (CLRS ch. 14); Edelsbrunner centered
  interval tree; interval skip list (Hanson); PostgreSQL GiST exclusion constraint over range types;
  roaring bitmaps.
- **Geo:** Google S2; Uber H3; geohash (Z-order/Morton); R-tree/R*-tree; PostGIS GiST/SP-GiST/BRIN.
- **Time:** TAI; UTC + leap seconds; Unix time (anti-pattern for canonical); GPS time; CGPM 2022
  Resolution 4 (drop leap seconds by 2035); IAU TT/TCG/TCB/TDB; Coordinated Mars Time / MSD; LunaNet /
  Coordinated Lunar Time; DTN Bundle Protocol (one-way light time as a scheduling constraint).
- **Distributed consistency:** I-confluence / coordination avoidance (Bailis et al., VLDB 2015); "Life
  Beyond Distributed Transactions" (Helland, CIDR 2007); Try-Confirm-Cancel; Stripe idempotency keys;
  bounded-counter / escrow CRDTs (ElectricSQL); sagas + compensation.
