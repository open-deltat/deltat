# deltat — Calendar Adapter Architecture & Plan

> **Status: v1.0 — decided.** Architecture (§4) and roadmap (§7) resolved by a design judge-panel
> (4 candidate architectures × 4 independent lenses); the Minimum Viable Adapter scope (§9) is
> distilled from a Cal.com open-source code study + a per-provider MVA pass. Evidence lives in
> [`ADAPTERS-RESEARCH.md`](ADAPTERS-RESEARCH.md) (provider→deltat mapping) and the OSS review in
> [`REVIEW-2026-06-22.md`](REVIEW-2026-06-22.md). Authoritative spec: [`REQUIREMENTS.md`](REQUIREMENTS.md).
> **Three decisions are flagged for the user in §8.3 — the rest carry sensible defaults.**

---

## 1. Why — the thesis

The vision (VIS-01/02/03) is to **move people off siloed booking systems onto one open, federated,
self-hostable layer.** You do not win that by asking people to abandon Google/Outlook/iCloud on day
one — you win it the way email and the web won: **interoperate first.** An adapter is the on-ramp.
It lets a resource live in deltat *and* stay in sync with the calendar its owner already uses, so
adoption costs nothing and the open layer accretes value until the silo is the redundant copy.

**An adapter need not be 100% faithful.** The Minimum Viable Adapter (§9) is just: **read the silo's
busy times so deltat's availability is correct, and write one event so a deltat booking shows up in
the silo.** That captures ~60–80% of a silo's value at a fraction of the cost — because **deltat is
natively a free-busy computer** (MODEL-05: availability is derived), so the import target (a Blocking
Rule) and the export source (`compute_availability`) are already deltat primitives. The expensive
long tail (recurrence round-trip, webhooks, two-way reconciliation, iMIP) is explicitly deferred.

---

## 2. The non-negotiable constraint — the kernel stays pure

The load-bearing rule, and the reason adapters are cleanly possible:

- **NOT-01 / MODEL-07 / NOT-02:** the kernel never contains calendars, timezones, recurrence,
  display, or business/identity data. deltat is "just a database of integer instants" (EDGE-08).
- **EDGE-01 / EDGE-03:** *all* timezone / calendar / recurrence / display conversion is the edge's
  responsibility. Recurrence is already expanded at the edge into concrete Rules (`expandRecurrence`).

**An adapter is therefore a pure edge component** requiring **zero kernel changes for the MVP**, and at
most two already-planned, spec-tracked fields later:

- **`external_ref: Ulid`** replacing `Booking.label` (GAP-02 / T-02 / review M2) — an *opaque* handle.
  The provider key `(provider, UID, RECURRENCE-ID)` is arbitrary-length and PII-shaped, so it stays in
  the **adapter store**, mapped to the kernel's opaque Ulid (not a hash — see §8.2). Optional for v1.
- **`booking_group: Ulid`** (MODEL-10 / GAP-01) — so a multi-resource sync stays cancellable as one
  unit. Not needed until multi-resource two-way.

Provider auth tokens, sync cursors, the UID↔Ulid map, and the resource's IANA timezone (EDGE-07) all
live in the **adapter store**, never in deltat.

---

## 3. The three directions

| Direction | Definition | deltat primitive | Cost |
|---|---|---|---|
| **Import** | Ingest a calendar's busy-times *into* deltat so availability reflects them. | Writes **blocking Rules** on a capacity-1 mirror Resource. | **Low** — pre-expanded by the provider, privacy-clean, poll-and-replace, no recurrence engine. |
| **Export / Publish** | Expose deltat availability *out* as a feed/free-busy. | Reads **derived availability** → `VFREEBUSY`/`.ics`/free-busy. | **Low** — native to deltat; real-time-capable via LISTEN/NOTIFY. |
| **Two-way sync** | Bidirectional events + write-back. | Read+write; needs idempotency + echo-suppression. | **High** — OAuth, webhooks, dedup loops, recurrence round-trip, iMIP. Deferred to the bridge phase. |

The MVP is **import + a thin export**; two-way is a later, triggered phase.

---

## 4. Architecture — decision: **C3 (Hybrid), refined**

Judge consensus was decisive: C3 ranked #1 by 3 of 4 lenses (kernel-purist, time-to-value, ops/security)
and never below #2. It wins because the three forks — cheap↔two-way, no-PROTO-03↔PROTO-03, read↔write —
**all coincide at one seam** (the Phase-1→bridge boundary), making the SDK→bridge graduation a real
trigger (T-10: "boundaries now, layers on a trigger"), not an arbitrary one.

```
              PROVIDERS (Google, MS Graph, iCloud/CalDAV, .ics feeds, JMAP, Cronofy/Nylas)
                                      │
        ┌─────────────────────────────┴─────────────────────────────┐
        │  PROVIDER ADAPTER (one per provider) implements the         │  ← single responsibility
        │  CalendarAdapter PORT (§4.1). v1 = 2 load-bearing methods.   │
        └─────────────────────────────┬─────────────────────────────┘
        ┌─────────────────────────────┴─────────────────────────────┐
        │  PURE TRANSLATION CORE (built FIRST, shared, heavily tested) │  ← RFC-5545 ⇄ Rule/Booking/Span
        │  zone-correct RRULE expansion · admission-corner handling    │     (it can't be wrong → build it first)
        └─────────────────────────────┬─────────────────────────────┘
        ┌─────────────────────────────┴─────────────────────────────┐
        │  WIRE: pgwire/SQL today (PROTO-02) → PROTO-03 HTTP/JSON      │  ← adapters CONSTRUCT Command,
        │  at the two-way trigger. Adapters build `Command`, not SQL.  │     not parse SQL → survive migration
        └─────────────────────────────┬─────────────────────────────┘
                          deltat KERNEL (pure; unchanged for the MVP)
```

**The decision, in one line:** a **pure translation core built first and shared**; **thin in-SDK
adapters reach the kernel over the existing pgwire transport** for the cheap read/import directions
(ships value today, zero new infrastructure, no kernel change); a standalone **`open-tap-bridge` +
PROTO-03 is introduced only at the two-way trigger**, where it doubles as the FED-01 federation unit
(VIS-02) — so the end-state arrives without a retrofit.

**Grafts from the runner-up architectures (all judge-endorsed):**
- *Structured-recurrence-only at the wire boundary* — the core expands RRULE into N concrete Rule
  Commands in the declared IANA zone; no opaque RRULE string ever reaches `Command`. Defense-in-depth
  alongside `Span::try_new` rejecting `start>=end` (SEC-09/AVAIL-17).
- *Lossy-by-contract is compile-time* — the export DTO has no field able to carry `capacity(N)` /
  `buffer_after` / Hold `expires_at`, so non-export is a type fact, not a runtime omission (§6).
- *Shape the eventual PROTO-03 JSON envelope toward JSCalendar* (duration + tz-as-display + structured
  recurrence) so it doubles as the PROTO-04 MCP surface and the FED-09 portable-availability surface.
- *One enforced custody boundary* — when the bridge lands, it is the single home for OAuth tokens,
  cursors, webhook timers, echo markers, and the UID↔Ulid map; the process boundary enforces
  SEC-06/07 instead of per-app discipline.
- *Hold-free, read-only beachhead* — Phase 0/1 use no Holds, so the steppable-clock hazard
  (HW-01/GAP-11) and the unbuilt atomic `CommitHold` (AVAIL-07) never gate the first ship.

> **Verified HEAD fact that shapes Phase "two-way":** `wire::execute_command_inner` returns
> `PgWireResult<Vec<Response>>` — the dispatch runner is *welded to pgwire even though `Command` is
> already transport-neutral* (`src/command.rs`). So PROTO-03's literal first task is to **carve a
> transport-neutral runner out of `execute_command_inner`** (HTTP and pgwire become siblings over one
> dispatch on the neutral `Command`). This is also the PROTO-01 prerequisite — do it as a clean
> refactor, not a rushed two-way-shaped one.

### 4.1 The `CalendarAdapter` port

**Full contract** (capability-declared; interface segregation — a feed adapter implements only the read half):

```
capabilities(): { import, export, twoWay, freeBusyOnly, push }
pullChanges(syncState) -> { events, tombstones, nextSyncState }   // two-way delta
renderFreeBusy(resourceIds, window) | renderIcsFeed(resourceIds, window)  // export
pushBooking(booking) -> ExternalRef ; cancelBooking(externalRef)  // two-way write-back
subscribe(webhookUrl) -> Subscription ; handleWebhook(payload) -> changes  // realtime
```

**v1 MINIMAL contract** — collapse to 4 methods, only 2 load-bearing (this is exactly Cal.com's
`ics-feedcalendar`, which ships `getAvailability`+`listCalendars` and *stubs* the write methods):

```ts
interface CalendarAdapter {
  listCalendars(): Promise<IntegrationCalendar[]>        // one-time setup: pick which calendars to read

  // IMPORT (READ-many) — load-bearing
  getBusy(window: { start: Ms; end: Ms }): Promise<BusyInterval[]>  // BusyInterval = {start,end}, opaque
  //   → each BusyInterval becomes one Command::InsertRule{blocking:true} on a capacity-1 mirror Resource.
  //   POLL-AND-REPLACE: wipe the prior import's rules, re-insert (WAL replay makes replace crash-safe).
  //   FAIL-CLOSED: on fetch error keep last-known rules; never drop busy→free (that would double-book).

  // EXPORT (WRITE-one) — next increment
  createEvent(booking: { span: Span; label?: string }): Promise<string>  // → provider UID, stored in the edge map
  //   called once on Booking confirm against ONE chosen destination silo. Do NOT export Holds
  //   (no provider expires_at → phantom tentative-busy leak on re-import).

  // DEFERRED (v2): deleteEvent, updateEvent, subscribe/handleWebhook, sync-token delta
}
```

v1 contract precisely: poll-and-replace only (free-busy has no IDs/tombstones — never diff); window
bounded to `MAX_QUERY_WINDOW_MS` (90d, ENG-16); drop zero-duration / `start>=end` (Span::try_new →
SQLSTATE 22007) and `TRANSP:TRANSPARENT`/`showAs:free`; one external calendar = one capacity-1,
buffer-0, flat mirror; tokens + the UID↔Ulid map in the adapter store only; **zero kernel change.**

---

## 5. Translation model — provider ↔ deltat

Spine below; the full per-provider tables, lossy points, and admission corners are in
[`ADAPTERS-RESEARCH.md`](ADAPTERS-RESEARCH.md) §1–2.

| deltat concept | Inbound (import) source | Outbound (export) target | Edge work |
|---|---|---|---|
| **blocking Rule** | a busy/opaque event or free-busy period | `VEVENT` w/ `TRANSP:OPAQUE` | RRULE→spans; tz→UTC ms; all-day→span |
| **non-blocking Rule** | recurring availability windows | published open hours | recurrence expansion (EDGE-03) |
| **Booking** | an owned event (write-back target) | a `VEVENT` (CONFIRMED) | `external_ref`↔UID idempotency map |
| **availability (derived)** | — | `VFREEBUSY` / free slots | sweep → busy/free |
| **capacity / buffer / Hold** | (no provider concept) | (not expressible) | **lossy by contract** — deltat-only |

**The moat:** capacity, `buffer_after`, the resource tree, and `min_available` have **zero**
representation in any calendar system (JMAP included). On import, one external calendar = one
capacity-1 mirror; deltat then composes N mirrors with the `min_available` sweep ("≥k of N free") —
the computation the silos structurally cannot do.

---

## 6. Cross-cutting guardrails

- **Idempotency:** caller-supplied Ulids (MODEL-12) + the edge UID map make every sync write replay-safe.
- **Echo-suppression (two-way):** filter self-authored events (by stored external id) out of the import
  stream, or a sync loop re-imports deltat's own write-back. (Cronofy does this natively via app-vs-user.)
- **Incremental sync, never full re-scan** (two-way): persist the provider cursor (sync-token/etag/delta);
  on `410 Gone`, bounded windowed re-sync. (The MVP import is poll-and-replace — no cursor.)
- **Lossy by contract, not by accident:** capacity/buffer/Holds are unrepresentable on export — declared,
  not silently dropped (compile-time, §4 graft).
- **Timezone correctness at the edge (EDGE-07):** the resource's IANA zone lives in the adapter store;
  expansion is zone-correct there (fixes EDGE-04 DST-naivety) — never enters the kernel.
- **Clock hygiene:** Holds participate in two-way sync **only after** the monotonic-clock split lands
  (GAP-11/HW-02/03) — a steppable clock under sync is a correctness hazard. (MVP is Hold-free.)
- **Security:** OAuth tokens are secrets — adapter store only, never logged (SEC-06); per-tenant
  isolation on every adapter-store query (SEC-07); a write-capable HTTP surface requires per-connection
  auth (PROTO-07/SEC-01) — the shared cleartext password (PROTO-11) must not guard writes.
- **FAIL-CLOSED on import error:** an unreachable silo is treated as busy (keep last-known rules), never
  silently free (Cal.com's `getBusyCalendarTimes` does exactly this).
- **DoS guard:** bound any in-house ICS recurrence expansion to ~365 iterations + reject sub-daily FREQ
  (mirrors commit 766e9fe4; Cal.com caps ical.js at 365).

---

## 7. Phased roadmap

Each phase is "more pure-TS/translation behind the same port." The architecture changes shape exactly
once (SDK→bridge) at the two-way trigger. Phases 0–3 need **no kernel change** and ship over today's
pgwire transport.

| Phase | Goal | Adapter / work | Kernel change | Reachable today? |
|---|---|---|---|---|
| **0** | Kill the live DST bug; get a real RRULE engine | Fix `expandRecurrence` + pure translation core | No | — |
| **1** | deltat correct + a visible round-trip | **Google free-busy IMPORT** (§9) + free-busy EXPORT endpoint | No | ✅ pgwire |
| **1b** | Bookings appear in the silo | Google EXPORT (`createEvent` on confirm) + echo-suppression | No | ✅ pgwire |
| **2** | Broadest single open adapter | CalDAV import+export (MVP scope) — iCloud/Fastmail/Nextcloud | No | ✅ pgwire |
| **3** | The business duo | MS Graph import+export (+ EWS on-prem deferred) | No | ✅ pgwire |
| **4** | Two-way trigger | `open-tap-bridge` + PROTO-03 + per-conn auth; `external_ref` (GAP-02); deltat-as-CalDAV-server + RFC 6638 iTIP; delta/webhooks; Holds gated on GAP-11 | `external_ref` | builds PROTO-03 |
| **5** | Highest-quality open two-way | JMAP/JSCalendar — validates the JSCalendar-shaped PROTO-03 envelope (reference for PROTO-01) | No | via bridge |

**Phase 0 — the first PR (red→green, PRIN-12):** `expandRecurrence` (`recurrence.ts:36-66`) uses
`new Date`+`setHours` in the *process* zone, so a recurring "09:00 local" window shifts ±1h across each
DST boundary — a present 🟡 defect in every demo seed and live `tap/calendar` (EDGE-04/EX-15). Add a
failing test pinning a weekly slot's absolute-ms across a Europe/Berlin DST transition, then replace the
hand-rolled `daysOfWeek + HH:MM` loop (which can't express FREQ/INTERVAL/COUNT/BYSETPOS/sub-daily) with
a real RFC-5545 engine (TS: `rrule-temporal` or `rrule` + `@js-temporal/polyfill`) expanding in the
resource's **declared** IANA zone (passed as a param, EDGE-07; never read from the OS). Land an RFC-5545
conformance corpus here (SCALE-08; also gives the SDK its real second test file, TEST-13). No kernel change.

> **Phase-1 first adapter = Google import (decided, §8.3).** MS Graph stays Phase 3. Native adapters
> only — no unified-API stopgap (decided). The architecture is indifferent; all are import adapters
> behind one port.

---

## 8. Resolved decisions + open user-decisions

### 8.1 Network-reachability fork — **pgwire-interim now; build PROTO-03 at the two-way trigger**
The cheap directions need no new transport: **export = a read** (`SelectAvailability`, already in
`command.rs`, dispatched by `wire::execute_command`) and **busy-import = a single `InsertRule` write** —
both reach the kernel over the SDK's existing pgwire client. The only disposable plumbing is the
transport, never the translation core. PROTO-03 is **not** a free additive call (the dispatch runner is
pgwire-welded, §4) — its first task is the transport-neutral runner carve-out, bundled with
per-connection auth (PROTO-07/SEC-01) before any write-capable HTTP endpoint.

### 8.2 `external_ref` key design — **opaque Ulid in the kernel + edge mapping table (NOT a hash)**
GAP-02's `external_ref:Ulid` is a fixed 128-bit internal id; the provider key `(provider, UID,
RECURRENCE-ID)` is arbitrary-length and PII-shaped. The kernel field stays an **opaque Ulid**; the
adapter store holds the `(provider,UID,RECURRENCE-ID)↔Ulid` table. Keeps provider semantics + PII out
of the kernel (NOT-02, SEC-04), lets each RECURRENCE-ID override map to a distinct Ulid, needs no kernel
decision for the MVP (Cal.com's `BookingReference` proves the edge-only map works). The hash option is
rejected (bakes provider semantics into the kernel; collides under arbitrary-length keys).

### 8.3 User decisions (DECIDED 2026-06-22)
1. **First-adapter provider → Google Calendar import.** ✅ Decided. The MVA pass's technical pick
   (cleanest `freeBusy.query`, lowest effort, broadest consumer reach) over the ICP-driven MS-Graph
   alternative. MS Graph remains Phase 3; its order vs Google was the only thing this changed, and
   Google leads. Same port either way — only the first PR's API client differs.
2. **Unified-API stopgap (Cronofy/Nylas) → no.** ✅ Decided. Build native adapters behind the port;
   avoid per-user cost and a central gatekeeper (VIS-02). Revisit only as an explicit runway decision.
3. **First EXPORT surface → pull free-busy endpoint** over `compute_availability` (real-time-capable).
   ✅ Default adopted. Add the broad-but-stale cached `.ics` secret-token feed only when a "subscribe in
   Google/Outlook" use-case appears (providers cache it 12–24h — never advertise it as real-time).

### 8.4 Defaulted (no action needed)
- **Adapter home/wire:** tap SDK helpers over pgwire now → migrate to PROTO-03 at the bridge (§4).
- **GAP-02 scope for v1:** edge-only UID map; leave `Booking.label`/`external_ref` opaque; defer the field.
- **Capacity export:** busy-when-full (single feed) by default; N parallel sub-feeds only if a real
  multi-capacity export consumer appears.
- **Zero-length/inverted events:** drop at the edge (kernel-inadmissible).
- **Holds:** never exported; not in two-way sync until GAP-11 lands.

---

## 9. The Minimum Viable Adapter (MVA)

> Distilled from a Cal.com source study (the proven 5-method `Calendar` port) + a per-provider MVA pass.
> The reframe: an adapter does **not** need full fidelity — "read busy + write one event" is ~60–80% of
> a silo's value at a fraction of the cost.

### 9.1 Cal.com lessons (the proven minimal surface)
Cal.com's *entire* calendar ecosystem runs on ONE interface (`Calendar` in `packages/types/Calendar.d.ts`)
+ a `type→service` factory (`getCalendar`). Every provider is a class implementing it.
- **The contract is 5 methods, only 2 load-bearing.** Required: `getAvailability`, `listCalendars`,
  `createEvent`, `updateEvent`, `deleteEvent`; the push/cache methods are optional and Google-only,
  flag-gated. **`ics-feedcalendar` implements only `getAvailability`+`listCalendars` and STUBS the write
  methods** → a read-only adapter is a fully shipping product. This *is* deltat's import-first thesis.
- **Strict READ-many / WRITE-one split.** READ: `getBusyCalendarTimes` → `getCalendarsEvents` →
  per-credential `getAvailability` returning `EventBusyDate[]` = opaque `{start,end}` (recurrence already
  provider-expanded; no capacity/buffer/tree crosses the boundary). WRITE: `EventManager.create` writes
  ONE event to a single `destinationCalendar`; the returned `uid`/`externalId` is persisted as a
  `BookingReference` — **their own edge-side UID↔id map, never in the event.**
- **Sync = on-demand poll by default;** push is a Google-only optimization above the port. **Recurrence:
  expand to read, single event to write** (no round-trip; CalDAV expansion capped at 365 iterations,
  sub-daily rejected). **Fail-closed:** an unreachable calendar returns a placeholder BUSY block.
- **What they skip = deltat's moat:** capacity/multi-slot, buffer, resource tree, holds/TTL, recurrence
  round-trip, two-way reconciliation beyond a UID map, iMIP.

**deltat copies:** the 1-port/registry shape; READ-many/WRITE-one; `{start,end}`→one Blocking Rule;
poll-and-replace; the edge UID map (confirms §8.2); the 365-cap; fail-closed.
**deltat improves:** `capacity` + `min_available` (Cal.com has no capacity>1 or "≥k of N free");
`buffer_after` + `Hold{expires_at}` (no provider analog); availability already derived + LISTEN/NOTIFY →
export is near-free and real-time, where Cal.com re-fetches every call.

### 9.2 Per-provider scope table (v1 vs deferred)

| Provider | Auth (v1, least-priv) | IMPORT (→ Blocking Rules on cap-1 mirror) | EXPORT (write one event) | Deferred | %-value | Effort |
|---|---|---|---|---|---|---|
| **Google (REST v3)** | OAuth `calendar.freebusy` + `calendar.events` | `freeBusy.query` → `busy[]` (UTC, pre-expanded), poll-and-replace | `events.insert` (UTC, `summary=label`) → store `event.id` | watch/push, syncToken delta, recurrence round-trip, attendees/Meet, `events.delete` on cancel | **~70–80%** | **Low** |
| **CalDAV (iCloud/Fastmail/Nextcloud)** | per-user Basic/TLS or app-specific password; RFC 6764 discovery | `REPORT <free-busy-query>` → `VFREEBUSY` BUSY periods, poll-only | `PUT <uid>.ics` single VEVENT, store UID+ETag | `sync-collection` delta, raw `.ics` RRULE engine, iTIP/iMIP, VAVAILABILITY, `DELETE` on cancel | **~60–70%** | **Med** |
| **MS Graph (M365 cloud)** | OAuth `Calendars.Read` + `Calendars.ReadWrite` | `getSchedule` → busy `scheduleItems`, poll-and-replace | `POST /me/events` (`showAs:busy`) → store `id`+etag | change-notifications, `calendarView/delta`, `patternedRecurrence`, `workingHours`, **EWS on-prem**, cancel | **~70–80%** | **Med** |

Across all three the v1 contract is identical: poll free-busy → wipe-and-rewrite Blocking Rules on a
capacity-1 mirror + write ONE confirmed Booking as a single timed-UTC event + an adapter-store
`(provider,UID)↔Ulid` map for echo-suppression.

### 9.3 Recommended single first adapter — **Google Calendar IMPORT**
(free-busy → Blocking Rules on a capacity-1 mirror; ~3 ops: OAuth connect → `listCalendars` →
`getBusy` via `freeBusy.query`.) Why: cheapest + highest-fidelity (pre-expanded, privacy-clean, no
recurrence engine); Low effort; IMPORT is the "makes deltat correct without leaving the silo" adoption
half; it only writes Rules → fully reachable over today's pgwire with **zero kernel change and no
PROTO-03 dependency**; and N capacity-1 Google mirrors fed into the `min_available` sweep is the "≥k of
N free" computation Cal.com structurally cannot do — deltat's differentiator, demonstrated on day one.
**Pair it** with the near-free free-busy EXPORT endpoint for an immediate visible round-trip, and land
the Phase-0 DST fix alongside it (a fix, not a build).

**First PR (concrete):** given an OAuth token, poll `freeBusy.query` for a chosen calendar every
5–15 min and wipe+rewrite `Command::InsertRule{blocking:true}` on a dedicated capacity-1 mirror Resource.
**Touches:** the shared translation lib (UTC→ms); the adapter store (OAuth tokens + UID↔Ulid map — secrets,
never logged); the pgwire interim wire (construct `Command`, not SQL). **Exit criteria:** a Google busy
block at T appears as a Blocking Rule within one poll cycle; deleting it in Google clears the Rule next
poll; a failed fetch keeps the last-known Rules (fail-closed); zero-duration/transparent events dropped.
