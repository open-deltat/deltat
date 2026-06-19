# Proposals — future deltat primitives & adapters

Forward-looking ideas surfaced while building the demos. Not built yet; captured so the design
intent isn't lost. `docs/REQUIREMENTS.md` remains authoritative for what's implemented.

## SYNC-01 — Synchronous (contiguous, stable-unit) availability

**Problem.** The capacity sweep answers "is *a* unit free in window W?" (AVAIL-01/AVAIL-06). It
does **not** answer "is *the same* unit free across N consecutive windows?" For a capacity-N pool
of interchangeable units, the engine may report a window bookable on each of three nights even
though no *single* unit is free all three nights — i.e. you could only honour it by making the
guest switch units between nights. Real venues forbid that: a hotel guest keeps one room for the
stay; a desk-booker wants the same desk all week.

This is **not** hotel-specific — it's the multi-period generalisation of the capacity sweep, and
applies to any capacity-N resource booked over a sequence (hotel room-types, rental fleets,
recurring desk/locker bookings, multi-session course seats).

**Proposed primitive.** Given a capacity-N resource, a sequence of periods `P₁..Pₖ` (e.g. nights),
and a required run length `r`, return the placements where a *single notional unit* is free for
`r` consecutive periods. Concretely: model the N units as an assignment problem over the periods —
a contiguous run of length `r` is satisfiable iff a matching exists that keeps one unit assigned to
the whole run without exceeding capacity in any period. The sweep already yields per-period
occupancy `occ(Pᵢ)`; a run `[Pᵢ, Pᵢ₊ᵣ)` is **stable-unit available** iff `occ(Pⱼ) < N` for every
`j` in the run AND the committed bookings can be packed so one unit stays free across the run
(for interchangeable units the first condition is necessary; the packing check handles the case
where existing multi-period bookings fragment the units).

**Why it belongs in the kernel.** It's pure interval/capacity math over the same `Span` +
`ResourceState.capacity` the engine already owns — defining it once gives every edge (hotel,
fleet, desks) correct "no-switching" availability instead of each frontend re-deriving it (badly).

**Sketch API.** `synchronous_availability(resource, periods: &[Span], run: usize) -> Vec<Span>`
(or a `min_consecutive` arg on the existing availability query). Red→green tests: 5 double-rooms
intermittently booked → a 3-night run is offered only where a single room is genuinely free all
three nights, not where coverage requires a switch.

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
