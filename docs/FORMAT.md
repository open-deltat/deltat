# open-tap wire & storage format: v0 (DRAFT)

> The thing designed to outlive every implementation. Per [`../V2-DESIGN.md`](../V2-DESIGN.md) §13,
> "never change for 100 years" applies to **this format**, *not* to the Rust binary, the index, or the
> storage engine; those will be rewritten many times. IPv4 (1981), TCP, DNS, Unix time, and the SQLite
> file format lasted because the *format* was minimal, versioned, and separated from its implementation.
>
> **Status:** DRAFT v0, not yet ratified. Nothing here is frozen until v1.0 ships with the conformance
> corpus (see §8). Until then, break it freely. After it, the rules in §7 govern every change.

---

## 1. What this format is (and is not)

It is the complete contract between a client and an open-tap node, and between a node and its own
write-ahead log. It has exactly four conceptual objects: an **instant**, an **interval**, a **command**,
and an **event**. Everything else (calendars, recurrence, geography, identity, money, business data) is
**out of scope by construction**: it lives in higher layers that reference resources by id (V2-DESIGN
§11). The format's smallness is the feature; it is why it can be frozen.

**Non-goals, permanently:** time zones, calendar dates, recurrence rules, latitude/longitude, prices,
descriptions, payment state, reputation. A reader that needs any of these is at the wrong layer.

---

## Vocabulary: one name per concept (frozen at v1.0)

A protocol that lasts is one whose words don't drift. Today the *domain* vocabulary is good and
meaningful, but it is inconsistent across layers: the engine's event log uses rich lifecycle verbs, the
(doomed) SQL layer flattens everything to `Insert/Select/Delete`, the TypeScript SDK is only partly
aligned, and the README says "segment" where the code says `Span`. v2 collapses to ONE vocabulary, the
engine's existing event verbs, used identically in the protocol, the SDK, and the docs.

### Nouns

| Term | Means | Not |
|---|---|---|
| **Instant** | one i64-µs point on the UTC number line | "timestamp", "datetime" |
| **Span** | a half-open time range `[start, end)`: pure geometry, no identity | "segment", "slot", "period" |
| **Interval** | a `Span` placed on one resource, with an id and a `kind` | "booking" (a Booking is one *kind* of Interval) |
| **Resource** | anything bookable; forms a parent/child tree | "entity", "object", "venue" |
| **capacity** | how many allocations may overlap on a resource at once | "slots"/"seats" (those are child resources) |
| **buffer** (`buffer_after`) | forced gap after each allocation | "padding", "cooldown" |
| **Rule** | an Interval that **opens** (`NonBlocking`) or **closes** (`Blocking`) availability | "schedule", "hours" |
| **Hold** | a self-expiring tentative allocation | "lock"; "reservation" (that's the *act*) |
| **Booking** | a committed allocation | "appointment", "order" |
| **availability** | the *derived* free Spans on a resource | "openings", "slots" |

> "Segment"/"slot" are fine as *intuitive teaching words* in prose; in anything normative the type is
> **Span** (a range) or **Interval** (a placed range). `IntervalKind::NonBlocking`/`Blocking` map to the
> user-facing words **open**/**closed**. Prefer those two in docs and SDK. (Scheduling/recurrence is an
> *edge* concern expanded into Rules, not a kernel noun; see V2-DESIGN §7/§11.)

### Lifecycle verbs: one per transition, identical across protocol, SDK, and docs

| Resource | Rule | Hold | Booking | Subscription |
|---|---|---|---|---|
| create · update · delete | add · update · remove | place · **commit** · release | confirm · cancel | subscribe · unsubscribe |

These are the engine's existing **event** verbs (`ResourceCreated`, `RuleAdded`, `HoldPlaced`,
`BookingConfirmed`, …), already meaningful, promoted to be THE names everywhere. They replace the SQL
layer's generic `Insert*/Select*/Delete*` (which die with the costume) and the SDK's partial drift
(`bookings.create` → **confirm**; `rules.delete` → **remove**). **Deletion is never one word:** a
resource is *deleted*, a rule *removed*, a hold *released*, a booking *cancelled*: distinct, meaningful
transitions the vocabulary must preserve. `place → commit` (the atomic `CommitHold`) is the hold→booking
transfer; `place → release` is the abandon path.

---

## 2. The instant: the one time primitive

```
Instant := i64        // signed microseconds since the Unix epoch (1970-01-01T00:00:00Z), UTC
```

- **Microseconds** (not milliseconds): sub-millisecond granularity at zero arithmetic cost. Range:
  ±292,471 years from the epoch, past every practical horizon.
- **UTC**, not TAI. A deliberate, reasoned choice (V2-DESIGN §3): the only field ever compared to "now"
  is hold expiry, which every clock in the system already produces as UTC; storing TAI would *add*
  leap-second surface, not remove it. A duration spanning a leap second is off by ≤1s, irrelevant to
  booking.
- A **calendar datetime is a pure display projection** computed at the edge. The format never carries
  year/month/day/zone. (Schedule projection is pure integer `div_euclid`/`rem_euclid` arithmetic, no
  date library; see the engine, not this format.)
- **Arithmetic is planet-agnostic**: an `i64` µs count carries no Earth assumption. The interplanetary
  seam is **one optional `frame` byte at the frame layer** (§3.1), added the day a second time-frame
  physically exists, never in the stored `Instant`, never in the WAL. A guard MUST reject comparison of
  instants from different frames.

```
Span := { start: Instant, end: Instant }   // half-open [start, end); REQUIRE start < end
```

Half-open is mandatory and load-bearing: adjacent spans (`a.end == b.start`) do **not** overlap.
Overlap is defined exactly as `a.start < b.end && b.start < a.end`.

---

## 3. The frame (transport envelope)

Every message on the wire is a length-prefixed frame:

```
frame := MAGIC(2) VERSION(1) FLAGS(1) LEN(u32, little-endian) BODY[LEN]
```

| Field | Bytes | Meaning |
|---|---|---|
| `MAGIC` | 2 | `0x74 0x70` (`"tp"`). A reader that doesn't see this closes the connection. |
| `VERSION` | 1 | Format major version. v0 = `0x00`. A reader MUST reject a `VERSION` it does not implement (no silent best-effort). |
| `FLAGS` | 1 | Bit 0 = body encoding: `0` = NDJSON (UTF-8 JSON, no embedded newline), `1` = postcard (binary). Bits 1-7 reserved, MUST be `0`, MUST be ignored on read until defined. |
| `LEN` | 4 | Body length in bytes, little-endian `u32`. Hard cap (e.g. 16 MiB) enforced by the reader. |
| `BODY` | `LEN` | One encoded `Command`, `Response`, or `Handshake` (§4-§5). |

### 3.1 The interplanetary seam (not built in v0)

A future `FLAGS` bit or a 1-byte `frame` prefix on `Instant`-bearing bodies declares the time-frame.
**v0 reserves the space and writes zero code.** When a second frame exists, the rule is: instants are
only comparable within the same frame; a cross-frame interval operation is a protocol error, not a
silent coercion.

### 3.2 Encoding choice

NDJSON is the **default and the debug format** (no cross-language codegen → no Rust↔TS encoder drift).
postcard is the optional compact binary, selected per-connection. **Decide which is the hot-path
default by one benchmark on batch-booking** (V2-DESIGN §9). Do not maintain two encoders speculatively
beyond what the benchmark justifies. Whatever the encoding, the *logical schema* below is identical.

---

## 4. Handshake (per connection)

The first frame a client sends after connecting:

```
Handshake := { tenant: string, credential: string }
```

Authenticates the whole connection and binds it to one tenant (replacing v1's shared cleartext password
+ unauthenticated startup-packet tenant string). The node replies `Response::Ready` or
`Response::Error` and closes on failure. There is **no per-operation signing** in this format; that is
a federation-edge concern (V2-DESIGN §5), forbidden in the node protocol.

---

## 5. Commands and responses

### 5.1 Command: the complete verb set

The logical schema (field names are normative; encoding is per §3). All ids are `Ulid` (128-bit,
lexicographically sortable, client-supplied). All times are `Instant` (i64 µs).

**Resources** (the bookable tree; `parent_id` forms the hierarchy):
- `CreateResource { id, parent_id?, name?, capacity: u32, buffer_after?: i64 }`
- `UpdateResource { id, name?, capacity: u32, buffer_after?: i64 }`
- `DeleteResource { id }`

**Rules** (open/close regions, `NonBlocking`/`Blocking`):
- `AddRule { id, resource_id, span, blocking: bool }`
- `UpdateRule { id, span, blocking: bool }`
- `RemoveRule { id }`

**Holds** (self-expiring tentative claims):
- `PlaceHold { id, resource_id, span, ttl?: i64 }`: **`ttl` is a duration; the node assigns
  `expires_at` from its own clock and returns it opaque.** (v1's client-supplied absolute `expires_at`
  is removed, V2-DESIGN §2 Bug 2.)
- `ReleaseHold { id }`
- `CommitHold { hold_id, booking_id, label? }`: **atomic** hold→booking under one lock, excluding the
  named hold from the conflict check. The single most important addition over v1 (Bug 1). Idempotent:
  re-committing an already-committed `booking_id` is a success echo.

**Bookings** (permanent claims):
- `ConfirmBooking { id, resource_id, span, label? }`: direct booking without a prior hold.
- `BatchConfirmBookings { bookings: [{ id, resource_id, span, label? }] }`: all-or-nothing.
- `CancelBooking { id }`

**Schedules:** *(removed from the kernel; recurrence is expanded into `AddRule` segments at the edge
via `expandRecurrence`; there is no kernel Schedule command. See REQUIREMENTS `MODEL-11`/`EDGE-03`/`GAP-08`.)*

**Queries** (read-only):
- `GetResources { parent_id?: Option<Option<Ulid>> }` *(absent = all; null = roots; id = children of id)*
- `GetRules { resource_id }` · `GetBookings { resource_id }` · `GetHolds { resource_id }`
- `GetAvailability { resource_id, start, end, min_duration?: i64 }`
- `GetMultiAvailability { resource_ids: [Ulid], start, end, min_available: usize, min_duration?: i64 }`

**Subscriptions** (real-time, replacing v1 `LISTEN/NOTIFY`):
- `Subscribe { resource_id }` · `Unsubscribe { resource_id }` · `UnsubscribeAll`

> Naming note: v2 renames v1's `Insert*`/`Select*`/`Listen` to verb-first (`Create*`/`Get*`/`Subscribe`)
> to drop the SQL inheritance. The *shapes* are identical; this is a rename, not a redesign.

### 5.2 Response

```
Response :=
  | Ready                                  // handshake accepted
  | Rows([Row])                            // query results
  | Affected { count: u64, expires_at?: Instant }  // mutation ack; PlaceHold returns assigned expiry
  | Subscribed
  | Event(Event)                           // pushed on a subscribed connection (§6), native, no JSON-in-NOTIFY hop
  | Error { code: string, message: string }
```

`Row` is a tagged union over the query result types (resource / rule / booking / hold / schedule /
availability-slot). Field names mirror §5.1.

---

## 6. Event: the storage (WAL) format

Events are the append-only log. The same `Event` stream drives both crash-replay and live subscription
push (the single `apply_event` funnel, the invariant that makes event-sourcing trustworthy). The WAL
record framing is independent of the wire framing:

```
wal_record := LEN(u32) PAYLOAD[LEN] CRC32(u32)   // bad CRC / short tail → safe truncation on replay
```

`Event` variants (the durable vocabulary; **each gets a permanently-frozen discriminant**, §7):

```
ResourceCreated { id, parent_id?, name?, capacity, buffer_after? }
ResourceUpdated { id, name?, capacity, buffer_after? }
ResourceDeleted { id }
RuleAdded       { id, resource_id, span, blocking }
RuleUpdated     { id, resource_id, span, blocking }
RuleRemoved     { id, resource_id }
HoldPlaced      { id, resource_id, span, expires_at }     // shape UNCHANGED from v1 (on-disk stable)
HoldReleased    { id, resource_id }
HoldCommitted   { hold_id, booking_id, resource_id, span, label? }   // NEW: the atomic transfer
BookingConfirmed{ id, resource_id, span, label? }
BookingCancelled{ id, resource_id }
```
*(There is no `ScheduleSet`/`ScheduleRemoved` event: kernel Schedule was removed; recurrence is edge
rules. The current engine has exactly the 10 variants above; `HoldCommitted` is the one planned addition.)*

**A per-resource monotonic sequence number** must be derivable from (or cheaply addable to) this stream
*before v1.0 freezes*, so federation (V2-DESIGN §5: ownership epoch + per-resource seq + per-op nonce)
is a seam and not a re-format flag-day.

---

## 7. Evolution rules (what governs every change after v1.0)

These are the rules that make "100 years" credible. They are the distilled discipline of Protocol
Buffers, SQLite's format promise, and the IETF "must-ignore-unknown" culture.

1. **Never reuse or renumber a discriminant.** `Command`, `Response`, and `Event` variant tags are frozen
   once shipped. A removed variant's tag is marked **reserved**, never recycled. (This is why HoldPlaced
   keeps its exact shape and a new HoldCommitted is *added* rather than confirm_booking being mutated.)
2. **Additive-only.** New capability = a new optional field or a new variant. Never change the type or
   meaning of an existing field.
3. **Must-ignore-unknown.** A reader skips unknown optional fields and reserved flag bits without error,
   so a newer writer never breaks an older reader for additive changes.
4. **Version is negotiated, not assumed.** `VERSION` mismatch the reader can't satisfy → hard reject. A
   breaking change is a new major `VERSION`, supported alongside the old for a deprecation window.
5. **The `Instant` semantics never change.** i64 µs, Unix epoch, UTC. Sub-second granularity finer than
   µs, or a second frame, arrives via the §3.1 seam, never by redefining the existing field.
6. **The four invariants are frozen** (V2-DESIGN §13): the instant primitive; the unified
   `Interval{id, span, kind}` model; availability is *derived, never stored*; one authoritative home per
   resource (single-writer). These are not fields; they are the conceptual contract.
7. **No external dependency leaks into the spec.** The format is self-contained: no SQL grammar, no
   Postgres types, no library-specific encoding assumptions beyond "NDJSON or postcard of this schema."

---

## 8. Conformance: the actual durability mechanism

A format is kept alive by a **test suite, not a document** (this is the SQLite lesson). Ship and maintain:

- **A cross-language round-trip corpus**: a directory of canonical `(Command|Response|Event)` examples,
  each with its NDJSON and postcard encodings as golden files. Rust **and** TypeScript implementations
  MUST encode→decode→re-encode every example to byte-identical output. This gates CI and is the wall
  against encoder drift (the v1 risk of three representations: SQL-in, bincode-in-WAL, JSON-out).
- **A backward-compat corpus**: golden WAL segments from each shipped version; every newer binary MUST
  replay them to identical state. This is what makes a storage-engine rewrite safe.
- **A negative corpus**: malformed frames (bad magic, oversize `LEN`, unknown `VERSION`, truncated WAL
  tail) that every implementation MUST reject/recover identically.

When v1.0 freezes, this corpus *is* the spec; this document is its prose companion.

---

## 9. Open items before v1.0 freeze

- NDJSON vs postcard hot-path default: decide by one batch-booking benchmark (do not freeze two encoders).
- Confirm the per-resource monotonic sequence number is derivable now (§6) so federation stays a seam.
- Confirm no external consumer has byte-persisted v1 data before the ms→µs widening (the one breaking
  change; the audit found no released wire clients).
- Decide the `ttl` clamp bounds (`MIN`/`MAX`/`DEFAULT` hold lifetime) and whether `Affected.expires_at`
  is the right channel for returning assigned expiry, or a dedicated `HoldPlaced` response variant.
- Ratify the hold-capability model (V2-DESIGN §9 Q1): does `CommitHold` authorize on possession of
  `hold_id`, and if so does the format need a capability secret field reserved now?
- **`booking_group: Option<Ulid>`** on the booking events: without it a multi-resource booking ("table
  + my calendar", or "2 seats") is N *unlinked* bookings: can't cancel/query the set as a unit, and
  cross-node sagas have nothing to coordinate around. Additive, defaults None; adding it post-freeze is
  a major-version flag day. **Decide before freeze.** (Found while building the Meet demo.)
- **`label: String` → `external_ref: Option<Ulid>`**: `label` is a second free-text field (contradicts
  V2-DESIGN §11's "one grandfathered String") and will collect PII into an append-only, signed,
  replicated WAL (GDPR right-to-be-forgotten tension). Make it an opaque id into the business-data layer.
- **Visibility/ACL is NOT a kernel field** (fails the admission rule), but `GetAvailability`/`GetBookings`
  etc. must be authorization-gated per `(tenant, resource_id)`. Reword "everything searchable" to
  "everything a publisher chose to publish, via the discovery edge, never via direct kernel query."
- **Open-ended / variable-duration** stays (`park until I leave`) vs the frozen `start < end` invariant.
  Decide whether to model an open right end (sentinel / separate kind) before freezing the Span rule.
- **Engine silently truncates multi-row `INSERT`** (only bookings batch). Worked around in the SDK; the
  framed protocol must make batch *explicit and typed* (and reject, not truncate, anything it can't apply).
