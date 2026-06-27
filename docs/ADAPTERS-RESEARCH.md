# deltat Calendar Adapter Map — Research Appendix

> Provenance: synthesized from a multi-agent deep-research pass (2026-06-22) over ten provider/standard
> analyses — iCalendar (RFC 5545 + 7986 + iTIP/iMIP), CalDAV (RFC 4791 + 6638/6578/6764), Google
> Calendar REST v3, Microsoft Graph/Outlook, Apple/iCloud, free-busy + ICS-feed patterns, the
> booking-platform reference designs (Cal.com/Calendly/Acuity/Bookings/Nylas/Cronofy), JMAP/JSCalendar
> (RFC 8620 + 8984), CardDAV + on-prem Exchange/EWS, and a recurrence-engine/IANA-tz survey — each
> adversarially critiqued and corrected. This is the **detailed evidence** behind the decisions in
> [`ADAPTERS.md`](ADAPTERS.md); that doc is the plan, this is the map.

## 0. Grounding against the actual kernel (re-verified in source)

`src/model.rs` is the translation target and the authority for the type-level facts:

- `Span { start, end }` — half-open `[start,end)`, integer Unix ms (µs planned). **No timezone field exists in the type.** NOT-01/02 are enforced structurally, not by convention.
- **`Span::new` ASSERTS `start < end` (`src/model.rs:16`); `Span::try_new` REJECTS `start >= end` (`src/model.rs:23-24`).** Hard admission corner: a zero-duration VEVENT (`DTSTART == DTEND`, legal in iCal for an instant/reminder) and any `start >= end` event **cannot become a Span** — the edge must drop or widen it.
- `IntervalKind` = `NonBlocking` (open hours) | `Blocking` (closed) | `Hold { expires_at }` | `Booking { label: Option<String> }` (`model.rs:42-59`). `label` is the only admissible descriptive field, and it is the *unresolved* one (GAP-02).
- `ResourceState { id, parent_id, name, capacity: u32, buffer_after: Option<Ms>, intervals }` — tree, multi-slot, trailing-buffer are first-class. **`capacity` defaults to 1.**
- WAL vocabulary = exactly the 10 flat `Event` variants, defined in `src/model.rs:142-184` (used by, not defined in, the WAL). Transport-boundary `Command` enum is a separate type in `src/command.rs:16`. No `Schedule`/recurrence variant — "expand at the edge" is forced by the kernel, not chosen.

### 0.1 Two corrections that change the build plan

**(A) `expandRecurrence` already exists, is LIVE, and is DST-BROKEN — the build-first task is to FIX it, not write it.** Confirmed cites: EDGE-02 (SDK helpers `schedules.ts:12-39`), EDGE-03 (`recurrence.ts`, all demo seeds use it), EDGE-04/EX-15 (DST-naive, runtime-local tz via `new Date`+`setHours`, `recurrence.ts:36-66`), EDGE-06 (`-getTimezoneOffset()` snapshot, live in `tap/calendar`), EX-14 (`tap/calendar` projects a weekly schedule into rules over a 90-day horizon), TEST-13 (`__tests__/schedules.test.ts`). The `tap/` dir is simply not checked out in this worktree, which misled an earlier pass into "greenfield." **Consequence: every existing demo seed and the live `tap/calendar` app are already emitting wrong absolute-ms slots across DST boundaries (EX-15 is a present 🟡 defect).** Build-rec #1 is "fix the DST/declared-zone bug + adopt a real RFC-5545 RRULE engine," not "write it" (the current code is a weekly `daysOfWeek + HH:MM` loop — it cannot express FREQ/INTERVAL/COUNT/BYSETPOS/BYWEEKNO/BYMONTHDAY/sub-daily).

**(B) "Target `Command`" is a vocabulary, not a reachable network surface today.** `src/command.rs` holds the transport-neutral `Command` enum, but **PROTO-01 (framed transport) and PROTO-03 (HTTP/JSON adapter) are NOT built; the ONLY transport at HEAD is pgwire+SQL (PROTO-02).** No HTTP/axum/hyper/MCP adapter exists in `src/`. So an adapter that "targets `Command`" cannot be reached over the network today without (a) building PROTO-01/03 first, or (b) going through pgwire/SQL. Adapters should *construct* `Command` (not parse SQL) to survive the migration, but the actual wire is pgwire/SQL until PROTO-01/03 land.

### 0.2 The GAP-02 / external_ref escape hatch

`Booking { label }` is the only descriptive kernel field; GAP-02 + T-02 plan `label → external_ref: Ulid` — an opaque INTERNAL id, **not** a documented `(provider, UID, RECURRENCE-ID)` token. The external-id↔deltat-id map must live at the edge today. Whether `external_ref` should additionally carry a provider-stable token (to make two-way dedupe/cancellation kernel-assisted) is a genuine open question.

---

## 1. Cross-provider → deltat concept mapping

Legend: ✅ clean/lossless · ⚠️ lossy (info added/dropped at edge) · ✖ no representation in provider.

### 1.1 Resource (bookable node; capacity; buffer; tree)

| deltat concept | iCal/.ics | CalDAV | Google REST | MS Graph | Apple/iCloud | JMAP | Free-busy/ICS |
|---|---|---|---|---|---|---|---|
| **Resource (leaf)** | one VCALENDAR/feed ⚠️ flat | calendar collection ⚠️ | a calendar / `calendarId` ⚠️ | one calendar (=mailbox) ⚠️ | CalDAV collection ⚠️ | a `Calendar` object ⚠️ | one feed/`{id}` ⚠️ |
| **capacity:u32** | ✖ binary busy | ✖ VFREEBUSY binary | ✖ single-track | ✖ (Bookings *service capacity* proves it's a separate axis) | ✖ implicitly 1 | ✖ single-track | ✖ "busy when full" |
| **buffer_after** | ✖ silently widens busy | ✖ bake into span | ✖ bake into end | ✖ bake into end | ✖ bake into span | ✖ | Cronofy `buffer.after` ⚠️ (trailing only; `before` lost) |
| **parent/child tree** | ✖ | ✖ flat | ✖ CalendarList flat | ✖ calendarGroup = grouping only | ✖ | ✖ | ✖ |

**Verdict:** capacity, buffer_after, and the tree are deltat's three differentiators and have **zero** representation anywhere in the calendar ecosystem (JMAP included). On export they must be pre-flattened at the edge (capacity → N parallel calendars or "busy-when-full"; tree → pre-composed inherited rules; buffer → folded into span ends). On import the canonical mapping is **one external calendar = one capacity-1 Resource**, buffer-0, flat. This is the moat, not a gap.

### 1.2 Rule — open (NonBlocking) / closed (Blocking)

| deltat concept | iCal | CalDAV | Google | MS Graph | Apple | JMAP | Free-busy/ICS |
|---|---|---|---|---|---|---|---|
| **Blocking Rule** | OPAQUE non-cancelled VEVENT / VFREEBUSY busy ⚠️ | `free-busy-query`→VFREEBUSY ⚠️ | `FreeBusy.query busy[]` ✅→⚠️ | `getSchedule scheduleItem` ⚠️ | `free-busy-query`/OPAQUE ⚠️ | `getAvailability`→BusyPeriod ✅→⚠️ | `FBTYPE=BUSY`/`busy[]` ✅→⚠️ |
| **NonBlocking Rule (open hours)** | RFC 7953 VAVAILABILITY ⚠️ (≈0 clients) | VAVAILABILITY ⚠️ | ✖ no open-hours object | `getSchedule workingHours` ⚠️ / EWS `WorkingHours` ⚠️ | VAVAILABILITY ⚠️ | ✖ no open-hours object | RFC 7953 ⚠️ |
| **busy/free gate** | `TRANSP` ✅ | `TRANSP` ✅ | `transparency` ✅ | `showAs` (5-way collapses) ⚠️ | `TRANSP`/EKEventAvailability ✅ | `freeBusyStatus` ✅ | `FBTYPE`/`status` ✅ |

**Verdict:** the **busy/free gate is the one near-universal clean mapping** (OPAQUE/`busy` → Blocking Rule; TRANSPARENT/`free` → drop) — why free-busy import is the cheapest, highest-fidelity path. Open-hours is the asymmetric pain: only Graph/EWS `workingHours` ship a positive open-hours object; everywhere else open hours must be inverted from busy or invented at the edge. RFC 7953 is the conceptual twin of NonBlocking Rules but unusable (no client support).

### 1.3 Booking (confirmed allocation) — half-open alignment, correctly scoped

| deltat concept | iCal | CalDAV | Google | MS Graph | Apple | JMAP | Free-busy/ICS |
|---|---|---|---|---|---|---|---|
| **Booking span** | timed-UTC `[DTSTART,DTEND)` ✅ | same | `confirmed`+`opaque` ⚠️ | `singleInstance` ⚠️ | confirmed VEVENT ⚠️ | `start`+`duration` ⚠️ | VEVENT in feed ⚠️ |
| **Booking.label** | `SUMMARY` ⚠️ | `SUMMARY` ⚠️ | `summary` ⚠️ | `subject` ⚠️ | `SUMMARY` ⚠️ | `title` ⚠️ | `SUMMARY` ⚠️ |
| **cancel → delete** | `STATUS:CANCELLED` ✅ | sync-collection 404 ✅ | `cancelled` tombstone ✅ | `@removed{deleted}` ✅ | `CANCELLED` ✅ | `status:cancelled` ✅ | windowed re-fetch+replace ⚠️ |

**Half-open alignment is real but was overstated by an earlier pass — corrected scope:**
- ✅ **Lossless two-way ONLY for timed events with DTSTART/DTEND both in UTC (`Z`).** RFC 5545 §3.8.2.2 defines `DTEND` non-inclusive = deltat half-open `Span`; converts with no zone work and **no ±1 fudge** (do not "fix" it by subtracting a second — a classic importer bug).
- ⚠️ **NOT lossless for all-day `VALUE=DATE` events** — DTEND is date-exclusive at *day* granularity; ms conversion requires choosing a zone (added, unrecoverable on export).
- ⚠️ **NOT lossless for DURATION-form events** (`DTSTART`+`DURATION`): needs duration arithmetic; JSCalendar `duration` can use calendar units (`P1M`/`P1Y`) resolved against `start` in its zone.
- ✖ **Kernel-INADMISSIBLE corner:** zero-duration and `start >= end` events cannot become a Span; the edge must drop or widen them.

### 1.4 Hold (tentative + expires_at)

Every provider has *tentative* (`STATUS:TENTATIVE` / `showAs=tentative` / `BUSY-TENTATIVE` / JMAP `tentative`) ⚠️ but **none has `expires_at`.** deltat Holds *require* a self-destruct timer. Import: synthesize an expiry policy at the edge or downgrade to a plain Blocking Rule. Export: a Hold becomes a tentative event but the timer is lost — the edge reaper MUST delete the external tentative event when the hold lapses, or it **leaks phantom busy on the next import** (the sync-loop hazard). Single most dangerous semantic mismatch.

### 1.5 Span / time resolution (the edge's core job — where the live DST bug bites)

| Source form | Resolution needed | Lossy? |
|---|---|---|
| UTC (`…Z`) / Google `busy[]` / Cronofy / JMAP utcStart | none — direct to Unix ms | ✅ |
| `TZID=` / Graph `dateTimeTimeZone` / JMAP `start`+`timeZone` | resolve via VTIMEZONE/IANA **per instance** (DST) | ⚠️ (tz identity dropped) |
| floating (no Z/TZID; JMAP `timeZone:null`) | edge MUST **choose** a zone | ⚠️ (choice unrecoverable) |
| all-day `VALUE=DATE` / Nylas `date` | materialize `[midnight,next-midnight)` in chosen zone | ⚠️ (all-day-ness + zone lost) |

**100% of tz/DST/all-day work is edge-only and none of it round-trips** — only the absolute instant survives. This is exactly where the live EDGE-04/EX-15 bug manifests: the current `expandRecurrence` parses with `new Date(date+"T00:00:00")` and writes via `setHours()` in the *process* local zone, so a recurring "09:00 local" window shifts its absolute ms by ±1h across each DST boundary. The recurring-event DST trap (the Unix-ms stride between instances is **not constant** across a DST boundary) is a present defect.

### 1.6 Recurrence

| | iCal | CalDAV | Google | MS Graph | Apple | JMAP | Free-busy |
|---|---|---|---|---|---|---|---|
| **stored form** | RRULE/RDATE/EXDATE + RECURRENCE-ID | same | `recurrence[]` strings | **structured `patternedRecurrence`** | RRULE | **structured JSON `recurrenceRules`** | n/a |
| **server-side expansion** | ✖ (file) | `<C:expand>` (uneven) | `singleEvents=true` ✅ | **`calendarView` only** ✅ | `calendar-query`+`expand` ✅ | `expandRecurrences=true` ✅ | **always pre-expanded** ✅✅ |

**Decisive cost split:** free-busy and "ask the server to expand" paths need **no recurrence engine on deltat's side**; only the raw `.ics`-subscribe path forces a full RFC 5545 expander (RRULE window + RDATE − EXDATE + RECURRENCE-ID + per-instance DST) at the edge — the strongest argument for free-busy-first. **JMAP/JSCalendar `recurrenceRules` is structured JSON — the best impedance match for a typed `expandRecurrence`.** MS Graph `patternedRecurrence` is structured but needs bidirectional RRULE translation.

### 1.7 Availability (derived) + multi-resource composition

| deltat concept | Closest provider construct | Fit |
|---|---|---|
| derived availability (open − blocking − allocations, ×buffer) | Cronofy Availability API; Graph `findMeetingTimes`; JMAP `getAvailability` | ⚠️ same computation, no capacity/buffer/tree |
| **`min_available` sweep** (=N / =1 / =k) | Cronofy `required:"all"\|N`; EWS multi-mailbox; JMAP multi-principal | ✅ **direct conceptual match** (FED-09) |
| free-busy lookup (export) | Google `freeBusy.query` / Graph `getSchedule` / VFREEBUSY / JMAP `getAvailability` | ✅ 1:1 with `compute_availability` |

**Best news in the map:** deltat is *natively a free-busy computer* (availability is derived, never stored). Exposing a `freeBusy`-shaped read is almost free, and Cronofy's `required:N`-over-participants is the *exact* shape of deltat's `min_available` sweep — a commercial API independently reinventing a deltat primitive.

### 1.8 Identity/descriptive/sync data — uniformly kernel-forbidden

ATTENDEE/ORGANIZER/PARTSTAT, SUMMARY/DESCRIPTION/LOCATION/GEO, conferenceData, colors/reminders, and all sync bookkeeping (`UID`/`SEQUENCE`/`DTSTAMP`/`etag`/`changeKey`/`sync-token`/`ctag`/JMAP `state`) are NOT-01/02-forbidden in the kernel and live at the edge — **except** the contested `Booking.label`/`external_ref` (GAP-02).

---

## 2. IMPORT vs EXPORT vs TWO-WAY

### 2.1 IMPORT (ingest external busy → availability reflects it) — high-leverage half

**Cheap.** Lowest-common-denominator is a free-busy query returning opaque `[{start,end}]` busy blocks: privacy-preserving (satisfies NOT-01/02 for free), **pre-expanded** (provider did RRULE/DST), per-interval → trivially a Blocking Rule on a capacity-1 mirror. Sources cheapest-first: Google `freeBusy.query` (`calendar.freebusy`), Graph `getSchedule` (`Calendars.ReadBasic`), **EWS `GetUserAvailability` for on-prem Exchange** (the coverage hole Graph can't fill — Graph is cloud-mailbox only; on-prem is EWS-only and EWS stays supported for on-prem while the 2026-2027 retirement is Exchange-*Online*-scoped), JMAP `Principal/getAvailability`, CalDAV `free-busy-query`, secret-token `.ics` (no-OAuth fallback). Import sync is **poll-and-replace** (free-busy has no IDs/tombstones; WAL replay makes replace crash-safe; no sync-token to persist).

### 2.2 EXPORT/PUBLISH

**Cheap & fresh:** a pull `freeBusy`-shaped read over `compute_availability` — 1:1 with deltat's native job, real-time (LISTEN/NOTIFY invalidates any cache instantly). **Cheap & broad:** a cached read-only `.ics` feed at a secret-token URL (mirrors Google/Outlook "secret address"). **Provider-side feed caching kills freshness** — Google refreshes subscribed feeds every 12–24h, Outlook 3–24h+; `REFRESH-INTERVAL`/`X-PUBLISHED-TTL` are advisory and ignored. **Never promise real-time via .ics export.** Inherently lossy on export: capacity (only "busy-when-full"), buffer_after (silently inflates busy — conservative-correct), live Holds (appear permanently busy in a snapshot), tree, `min_available`.

### 2.3 TWO-WAY SYNC (hardest)

Needs import+export plus durable external-identity round-tripping the kernel doesn't store: a `(provider, UID[, RECURRENCE-ID]) ↔ deltat-id` map (edge today; possibly → `external_ref`); concurrency tokens (iCal `SEQUENCE`/`DTSTAMP`; CalDAV ETag + **Schedule-Tag**; Google `etag`+410; Graph `@odata.etag`+immutable-id; JMAP per-type `state`); incremental delta (CalDAV `sync-collection`; Google `syncToken`+410-full-wipe; Graph `calendarView/delta` — one calendar + one fixed window; **JMAP `Foo/changes` — cleanest native delta**); change triggers (Google `events.watch` 7-day TTL; Graph subscriptions + lifecycle; **JMAP EventSource SSE / PushSubscription — real push**; **CalDAV/iCloud has NO push → poll-only**).

**The server direction (deltat-as-CalDAV-server) is a first-class two-way path.** With RFC 6638 auto-scheduling, an inbound **iTIP REQUEST** naming a deltat room as ATTENDEE *is the booking-admission protocol*: REQUEST → attempt Hold/Booking; admitted → `PARTSTAT=ACCEPTED`; capacity/window exhausted → `PARTSTAT=DECLINED`; CANCEL → delete. RFC 6638 explicitly does **not** do conflict detection — that is precisely deltat's core competency, so deltat is the conflict engine the protocol assumes. Requires an edge iTIP↔kernel correlation table keyed by `(UID[, RECURRENCE-ID])`, `(SEQUENCE, DTSTAMP)` idempotency, and **iMIP spoofing defense (S/MIME / trusted path) — an unauthenticated email naming the room as ATTENDEE would otherwise mint a real Booking; the kernel's capacity/window checks are NOT an identity check.**

### 2.4 Echo / hold-leak hazards

Both are edge-only and both bite the moment export and import touch the same calendar. The external-id map (or a unified API's app-vs-user flag) is the only mechanism that filters self-authored events out of the import stream; the reaper must delete external tentative events on hold expiry.

### 2.5 Admission corners every adapter must enforce

- Drop or widen **zero-length / `start >= end`** events (kernel rejects them).
- Drop `TRANSP:TRANSPARENT` / `freeBusyStatus:free` / `showAs:free|workingElsewhere`.
- Drop `STATUS:CANCELLED` / draft; treat as deletion on sync.
- Bound unbounded RRULEs to a horizon (DoS guard, mirrors commit 766e9fe4).
- Resolve every tz-bearing boundary to absolute UTC ms **per instance** in the resource's **declared** zone (the EDGE-04 fix), never the process zone.

### 2.6 Cost summary

| Direction | Cheapest path | Cost | Why |
|---|---|---|---|
| IMPORT | free-busy query → Blocking Rules | **Low** | pre-expanded, privacy-clean, poll-replace, no engine |
| EXPORT | pull free-busy endpoint over `compute_availability` | **Low** | native to deltat; real-time |
| EXPORT (broad) | cached `.ics` secret-token feed | **Low–Med** | broad compat; 12–24h provider cache; lossy |
| IMPORT (no-OAuth) | `.ics` subscribe | **Med–High** | full RRULE/tz/EXDATE engine; stale |
| TWO-WAY | events/JMAP + delta + push + id-map (or CalDAV-server + iTIP) | **High** | echo, hold-leak, token fragility, recurrence round-trip, iMIP spoofing |

---

## 3. Adapter notes that shape the ranking

- **"Apple/iCloud" is NOT a separate adapter** — iCloud has no REST API; server-side it collapses into the CalDAV adapter at `caldav.icloud.com` (app-specific-password auth, partition hosts, poll-only, full-PUT-only). Building CalDAV *is* building Apple.
- **CardDAV is a red herring** for availability — the contacts/vCard sibling of CalDAV (RFC 6352), no temporal data; relevant only for edge-side attendee-identity resolution.
- **iCalendar (RFC 5545) + iTIP/iMIP is not a network adapter** — it is the payload + scheduling-workflow format shared by CalDAV, the .ics feed, and Google/MS event bodies; the RFC 5545 parse/serialize + the (existing, DST-broken) `expandRecurrence` + tz resolution is a **prerequisite every other adapter consumes**.
- **Google CalDAV scoping** ("free-busy-query REPORT not implemented / no MKCALENDAR") is commonly cited but Google has shifted CalDAV behavior and steers developers off it — **verify against the current Google CalDAV guide.** It doesn't change the plan: do Google free-busy via REST `freeBusy.query` regardless.
