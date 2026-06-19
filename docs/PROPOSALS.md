# Proposals — future deltat primitives & adapters

Forward-looking ideas surfaced while building the demos. Not built yet; captured so the design
intent isn't lost. `docs/REQUIREMENTS.md` remains authoritative for what's implemented.

## SYNC-01 — Synchronous (contiguous, stable-unit) availability ✅ ALREADY GUARANTEED

**The question.** For a capacity-N pool of interchangeable units, can a stay occupy *the same*
unit across N consecutive periods — i.e. without the guest switching rooms mid-stay? Real venues
require it (a hotel guest keeps one room; a desk-booker wants the same desk all week).

**Finding: the capacity sweep already guarantees this — no new primitive needed.** A stay is a
single interval booked against the pool. Bookings on a pool form an **interval graph**, whose
chromatic number equals its maximum clique (max concurrent overlap). So **a stable single unit is
free for a whole span ⟺ the max concurrent occupancy over that span is `< capacity`** — which is
*exactly* what `check_no_conflict` / the saturation sweep already enforce when a stay is committed
as one booking (AVAIL-01/AVAIL-06). The worry that "each of three nights reads free yet no single
unit is free all three" cannot occur: if every instant in the span has `< N` booked, the intervals
are N-colourable and one colour (unit) stays free across the whole span.

Corollaries, both already supported:
- **Booking** a multi-night stay as one `[check-in, check-out)` booking on the pool is accepted iff
  a stable unit exists, and rejected (would require a switch) otherwise — automatically.
- **Listing** the stable multi-night openings = `availability(resource, horizon, min_duration = run)`
  (AVAIL-15) — the free runs long enough to fit the stay on one unit.

**Locked by test:** `sync_stable_unit_multi_night_availability` (engine/tests.rs) — capacity-2 pool,
two overlapping stays saturate the middle night; a 3-night stay across it is rejected, a clear stay
is accepted, and `compute_availability(.., Some(2 nights))` lists exactly the stable openings.

**The genuinely-open extension: named-unit assignment.** The pool model answers "*a* unit," not
"room 207." Telling a guest *which* physical room (and keeping that stable across the stay) is the
real future work — either model each unit as a capacity-1 child (N resources, fully named) or add a
thin assignment layer that pins a stay to a concrete unit id. That's a product/edge decision on the
assigned-vs-pool axis, not a gap in the availability math.

## EDGE-GCAL — Google Calendar adapter (read-through)

**Goal.** Let Google Calendar (and other CalDAV/ICS consumers) read a deltat resource's
availability without anyone rebuilding a calendar UI. The demo's `Calendar` is intentionally
minimal — the real integration story is "deltat is the source of truth; existing calendars read
from it," not "deltat reships Google Calendar."

**Shape.** A thin HTTP/MCP adapter (lives at the edge, not the kernel — PROTO-02) that exposes a
resource's availability as an ICS feed / CalDAV `free-busy` / Google Calendar API responses,
translating deltat `Span`s (Unix-ms, half-open) to RFC 5545 events. Read-only first (publish
availability + bookings as busy blocks); write-back (book via the calendar) is a later phase once
the framed `Command` protocol lands.

**Status.** Future. Captured here so the demo's "the calendar is fine — we'll adapt Google
Calendar to read deltat" decision is on record.
