# deltat: the demo that proves it

> Ideation doc. The goal: one demo that makes a non-technical person *feel* why deltat does
> something every booking system on earth structurally cannot. Grounded in real primitives
> (spec IDs in parentheses); the "north star" section is clearly marked as not-yet-built.

---

## TL;DR

Build **one** demo, with **one** escalation. Skip auto-rebooking.

**Headline: "One Room, Four Channels."**
A single hotel room is sold at the same time on **Airbnb, Booking.com, Expedia, and the hotel's own site.** Two modes, side by side:

- **Today (a channel manager):** each site keeps its *own* calendar and re-syncs on a timer. A visible countdown ticks `next sync 3:47…`. Two guests book the same night during that gap → **double-booked** → cancel, refund, 1-star review. Show the cost.
- **deltat:** there is *one* calendar. The instant anyone books, or even *starts* checkout, every other channel updates in **real time**, with a visible signal pulse fanning out from the core. The double-booking counter stays at **0**, forever, because the gap doesn't exist.

The "wow" is purely visual: you watch the deltat signal reach all four channels and grey out the date *while the old way's sync timer is still counting down.*

**Escalation: "The Impossible Bundle."**
Book a thing that needs **several independent resources at once**, all-or-nothing: room **+** parking spot **+** spa slot for the same night. Either you get the whole bundle or nothing is taken. No half-booked trips. (AVAIL-06)

**The one-sentence pitch:**
> Every booking system on earth syncs on a timer and double-books in the gap. **deltat has no gap.**

---

## The villain: the sync interval

Nobody connects Airbnb, Booking.com and Expedia directly. They connect through a **channel manager** that **polls** each platform and **pushes** updates on an interval: every few minutes at best. That interval *is* the double-booking window. Two guests, two channels, same 90 seconds → both succeed → the host eats a cancellation.

Everyone who has ever run a rental, a clinic schedule, or a restaurant floor has lived this. It's not a bug in any one product; it's the **architecture**: N independent calendars reconciled *after the fact*. You cannot poll your way out of a race.

deltat removes the architecture, not the bug. There is **one source of truth**, it **pushes** changes the instant they happen (LISTEN/NOTIFY, PROTO-12), and it is **mathematically incapable of double-booking**: every instant is checked against capacity before commit (INV-01, property-proven against a brute-force reference over thousands of generated cases). There is nothing to reconcile because nothing ever diverged.

---

## The headline demo, in detail

### What's on screen
A central **deltat core**, a single calm node showing the live WAL tick and a readout like `availability recomputed · 0.4 ms`. Around it, **four channel panels** (Airbnb / Booking.com / Expedia / Your Site), each showing the *same* room and the *same* October calendar. Thin **wires** connect each panel to the core. These are the live subscriptions (every channel is `LISTEN`-ing).

### Interaction 1: a booking propagates
Click **"Book Oct 14"** on the Airbnb panel:
1. A pulse travels **Airbnb → core** (the booking command).
2. The core flashes, the WAL ticks, the readout shows the recompute time.
3. Pulses fan **out from the core → all four panels at once.**
4. Oct 14 greys out **everywhere, simultaneously.**

No panel "owns" the calendar; they're all windows onto the same truth.

### Interaction 2: the hold (the part nobody else can do)
Start checkout on **Booking.com** but *don't* confirm. deltat places a **hold**, a segment with a self-destruct timer (AVAIL-11). Watch:
- A hold pulse → core → **all four** panels show Oct 14 as `⏳ reserved · 4:58…` and count down together.
- **Abandon the cart** (or let it expire) → the hold auto-releases → pulses fan out → Oct 14 **reappears everywhere.**

This is the killer beat. In the old world, "a guest is mid-checkout on one channel" is **invisible** to the others: only *confirmed* bookings sync, and only on the timer. deltat makes "reserved-in-progress" a real, real-time, auto-expiring, cross-channel state. Nobody else has this.

### Interaction 3: flip to "Today"
A toggle switches all four panels to **channel-manager mode**: each grows its own little calendar and a `next sync 3:47` timer. Drop two ghost-guests onto Oct 14 (one on Airbnb, one on Expedia) *inside the same sync window.* At the next tick: **collision.** One booking turns red: `OVERBOOKED: cancel + refund + 1★`. A scoreboard reads:

```
double-bookings   today: 3      deltat: 0
```

Run synthetic traffic for 30 seconds and let the gap rack up overbookings on one side and zero on the other. That single number is the whole argument.

---

## The escalation: The Impossible Bundle

Same engine, one step crazier, and still something everyone immediately understands.

Some things only make sense **booked together.** deltat can validate availability across **several independent resources at once** and commit them as a **single all-or-nothing transaction** (AVAIL-06: sorted+deduped locks, two-phase validate-then-commit). It can also answer *"when are at least k of these n free at the same time?"* in one query (AVAIL-04, `min_available`).

Framings, pick the audience:
- **Hotel:** the suite **+** an adjoining room **+** a parking spot **+** a 7pm spa slot: for the same night, or none of it.
- **Wedding:** venue **+** caterer **+** photographer **+** band: one date, all-or-nothing. (Today you book each separately and pray the photographer doesn't fall through after you've paid the venue.)
- **Operating room:** surgeon **+** theatre **+** anaesthetist **+** a recovery bed: find the window where *all* align (`min_available = all`), then book the set atomically.
- **Travel:** flight seat **+** hotel night **+** rental car: no more "hotel booked, but no car."

The visual: the bundle items light up across separate timelines, a single sweep finds the window where they overlap, and **one** commit pulse locks them all, or, if one is taken, the whole thing flashes red and **nothing** is touched. The all-or-nothing snap-back is the thing people remember.

---

## Why *not* auto-rebooking

It's the obvious "AI magic" idea, and it's a trap. Software silently **moving** someone's confirmed reservation is creepy, hard to trust, and the first wrong move destroys the demo's credibility. Your instinct is right. Don't build it.

The human-practical magic is the opposite of automation: it's **perfect real-time truth + soft-locks + atomic bundles**, with the human always in control. If you want a "smart" beat, make it a **suggestion**, never an action: *"Oct 14 just got taken: here are 3 windows where your whole bundle is still free,"* and a person clicks. deltat's job is to make the answer **instant and always correct**; the decision stays human.

---

## The north star (the genuinely crazy version: vision, not the demo)

The same property that lets one host coordinate four channels lets **independent companies** coordinate too: deltat's availability composition is **topology-free** (FED-09): combining timelines doesn't care whether they belong to one operator or twenty. The endgame is a shared **availability fabric** where you book *flight + hotel + car across three different companies* as one atomic transaction, with every provider's other channels updating in real time.

Be honest in the room: cross-**organization** federation and atomic hold→commit (`CommitHold`, AVAIL-07) are **not built yet** (NOT-05 defers federation; today's hold→confirm is non-atomic, PROTO-15), and cross-region is speed-of-light bound, not sub-millisecond (SCALE-04). Show it as the trajectory the single-operator demo is already standing on, not as something to claim it does today.

---

## Why deltat can do this (and the rest can't)

| What the demo shows | deltat primitive | Built? |
|---|---|---|
| Can't double-book, ever | capacity check at every instant (INV-01, property-proven) | ✅ |
| Every channel updates the instant anything changes | real-time push, no polling (LISTEN/NOTIFY, PROTO-12) | ✅ |
| "Reserved mid-checkout" locks across all channels, auto-frees | hold = segment with self-destruct timer; counts only while live (AVAIL-11) | ✅ |
| Book a bundle all-or-nothing | atomic multi-resource commit (AVAIL-06) | ✅ |
| "When are k of these n free together?" | multi-resource sweep + `min_available` (AVAIL-04) | ✅ |
| Same room, many capacities / turnaround gaps | capacity sweep (AVAIL-05) + buffer_after (MODEL-04) | ✅ |
| Instant answers | sub-ms availability reads, in-region (SCALE-04) | ✅ (reads) |
| Atomic *hold → booking* in one lock | `CommitHold` (AVAIL-07) | ◻ near-term |
| Cross-**company** atomic bundle | topology-free composition / federation (FED-09, NOT-05) | ◻ vision |

The load-bearing idea (VIS-00): **scheduling is 1-D collision detection**: N timelines on the number line of time, bound together by **atomicity.** Everything above is one primitive wearing different clothes. That's *why* deltat can compose across channels and resources in one query while the incumbent stack needs a whole reconciliation subsystem to fake it badly.

---

## Build notes

Most of the pieces already exist in `tap/demo`:
- a **hotel** example and a **live** example,
- the real-time path (`useWebSocket` → `/ws` → deltat `LISTEN/NOTIFY`),
- holds, capacity, buffers, multi-resource availability in the SDK.

What's new is mostly **presentation**, not engine work:
1. the **four-panel "same room, four channels"** layout over one resource,
2. the **signal-pulse choreography** (command in → fan-out), driven by the existing NOTIFY stream,
3. the **"Today" toggle** with a fake sync timer and a scripted collision in the gap,
4. the **scoreboard** (`double-bookings: today N · deltat 0`).

The escalation reuses the existing multi-resource + batch-booking SDK calls; it's a second screen, not a second backend.

---

## The 20-second version (for the room)

> "Here's a hotel room on four booking sites. Watch. I book it on Airbnb… and it's gone on the other three before you can blink. Now watch the *old* way: two people, two sites, same night, and there's the double-booking. Every booking platform alive has that gap. deltat doesn't have a gap. There's one calendar, and everyone's looking at it live."

---

# More demo ideas

The hotel demo leads with **real-time + holds**. These others each spotlight a *different* primitive, so a deck of them shows range. Ranked by demo power; pick by audience.

### 1. The Drop: "your cart expired" (holds + capacity + contention)
*The Ticketmaster meltdown, fixed.* 50,000 people race for the same seat map the instant tickets go onsale. A seat map flickers live: **yellow = held (cart timer 2:59…)** → **red = sold** → **green = released** the moment a cart is abandoned (the hold self-destructs, AVAIL-11). Toggle the "old way": two fans both reach payment for seat 14F, and one gets *"sorry, no longer available"* **after** entering their card. deltat's hold makes "in someone's cart, for now" a real, cross-everyone, auto-expiring state, so the same seat is never promised twice, and abandoned carts free themselves instantly.
- **Why impossible today:** carts and inventory live in different systems reconciled late; the famous failure is exactly this race. Everyone has felt it.
- **Visual:** a stadium/theatre map breathing in real time + a "ghost carts" counter + `oversold: today 11 · deltat 0`.
- *Possibly the strongest after the hotel one: the failure is culturally infamous.*

### 2. The Cascade: "close the floor" (hierarchy ripples in real time)
A coworking building: **building → floor → room → desk** as a live tree on the left, the bookable grid on the right. Flip **"Floor 3: fire inspection"** and watch a red wave sweep *down* the tree as every Floor-3 desk vanishes from the grid, on every channel, in the same instant. Flip it back; they all return. One toggle, hundreds of leaves, zero manual updates.
- **Why impossible today:** flat calendars have no parent. Closing a floor means hunting down and editing every desk by hand, in every system.
- **Visual:** the tree-to-grid ripple is the whole show; it *looks* like the availability is alive.

### 3. The Turnaround: "the time nobody schedules" (buffer_after)
An operating room (or an EV charger, or a barber's chair). After every booking, a striped **cleaning / turnaround** ghost-block *appears by itself* (buffer_after, MODEL-04) and the next slot literally can't be offered until the room is ready. Toggle the buffer off (the "naïve scheduler") and watch back-to-back bookings collapse into a cascade of delays, a patient parked in a room still being sterilised.
- **Why impossible today:** turnaround is invisible to most calendars, so humans forget it and overbook reality. deltat bakes it into availability so the mistake can't be made.
- **Visual:** ghost cleaning-blocks auto-growing after each booking; a "real-world delay" meter on the naïve side.

### 4. The Assembly Line: "book the whole chain or none of it" (atomic sequence)
A part needs **Mill (9-10) → Lathe (10-11) → QA bench (11-11:30)**: three machines, three consecutive windows, booked as **one atomic unit** (AVAIL-06). If any link is taken, the whole job rolls back and nothing is reserved. Bump one machine and the chain re-places or fails as a unit. The serious, industrial cousin of the bundle: job shops, film shoots (actor + location + crew + gear for a shoot day), clinical pathways (consult → scan → results).
- **Why impossible today:** you book each station separately and pray; a mid-chain failure strands the rest. deltat commits the dependency chain all-or-nothing.
- **Honest note:** deltat gives the fast per-machine availability + the atomic commit; the *placement search* is thin edge logic on top (greedy over availability), not magic.

### 5. The Runway: "this is literally what deltat is" (the on-the-nose one)
One runway, planes as segments on its timeline, **wake-turbulence separation = the buffer between them**, a weather closure = a blocking rule on the parent airport that cascades to every gate. It's VIS-00 made physical: *scheduling is 1-D collision detection*, here at 500 knots. Not a pitch to run real air traffic; a jaw-drop that the same tiny primitive set models a hotel night, a concert seat, *and* a runway.
- **Why it lands:** it reframes deltat from "a booking thing" to "the geometry under all scheduling."

### 6. The Time Machine: "rewind your schedule" (append-only log; leans into the name)
A scrubber under any of the demos: drag back in time and the calendar shows its **exact past state**: *who held 14F at 14:03:02, when did it release, who booked it.* Every change is an immutable logged event (the WAL), so nothing is lost to an overwrite. Plays straight into the name (deltat = change over time).
- **Why impossible today:** mutable calendars overwrite history; "what did availability look like at 2:01pm?" is unanswerable.
- **Honest note:** the append-only log makes this *possible*; a "state as of T" read is glue to build on top of replay, not a shipped API today. The most build-it flourish of the set.

---

## Picking from the deck
- **Lead with the hotel** (universal pain, real-time + holds).
- **Add The Drop** for emotional punch (everyone's been burned by it).
- **Add The Cascade** for the prettiest "it's alive" moment.
- Keep **The Runway** as the one-liner that recontextualises everything: *it's all 1-D collision detection.*
- **Turnaround / Assembly Line / Time Machine** are the depth picks for technical or industry-specific audiences.

---

# Reality check & scaling (researched)

Investigated the code + how Ticketmaster and One Million Checkboxes actually work. Verdict on *"would the 200k drop break live?"*: **yes, on today's stack, and that's fine, because the hard part was never deltat's job.**

## A high-demand onsale is *survived, not served*: three layers, only one is deltat

| Layer | Job | Scales like | deltat? |
|---|---|---|---|
| 1. Queue / admission | Cap concurrency, absorb the mob, FIFO/raffle, bot filtering, tokens | CDN / ∞ | No (edge) |
| 2. **Allocation / inventory** | Authoritative seat state, holds + auto-expiry, atomic confirm, conflict, never-double-sell | one node/tenant, fsync-bound writes | **Yes, this is deltat** |
| 2a. Read fan-out | Push the live seat-map to thousands of watchers | horizontal WS workers | No (wraps NOTIFY) |
| 3. Payment | Charge card, then confirm the held seat | provider | No |

The queue is **load-shedding in front of the fragile thing**: SeatGeek sizes a leaky bucket = the protected zone's capacity, full → HTTP 429. Ticketmaster admits *"only a few hundred shop at once,"* in waves; holds run 5-10 min; **~50% of inventory sells by the first cart timeout and expired carts re-flood** (the seat-map churns *both* ways). Eras Tour: 3.5B requests, 4× prior peak, and the failure was a *cross-layer* dependency (queue throttle × verified-fan codes), not the allocator. **deltat is Layer 2 and only Layer 2:** the fast, correct, conflict-detecting allocator that *assumes a queue already bounded the load.* The mob is somebody else's leaky bucket.

## Why 800, and how to blow past it
`MAX_WS_TOTAL=800` is a **config default in the demo's Node server** (`process.env.MAX_WS_TOTAL`, `0` disables). Nothing in deltat uses it. The real ceiling is architectural: one Node process funnels every browser onto **one** deltat connection (porsager multiplexes all `LISTEN`s onto a single pgwire connection).
- A single Node `ws` process realistically holds ~50-100k connections (the FD limit bites first: raise `ulimit -n`); a C++ stack (uWebSockets) did **1M on an 8 GB laptop**. The 65k-port limit is a myth (a connection is a 4-tuple, not a port).
- **The scale-out ("spin up another worker") works because of how deltat broadcasts:** run **K Node workers, each with its own deltat connection, each `LISTEN`ing the hot resources, and deltat already delivers every event to all K.** So **deltat's NOTIFY *is* the cross-worker backplane: no Redis/NATS needed for fan-out.**
- Two real caveats: (1) the broadcast ring is **256 events**: a subscriber that lags gets its stream *silently terminated*, so the correct pattern is "any event → re-read the truth," never "apply deltas blindly" (the demo already does this). (2) The hard write ceiling is **single-node fsync** (one durable writer per tenant); past it you **shard by tenant/resource** across nodes. deltat is single-node-durable, **not HA**. Flag that for prod.

## One Million Checkboxes is the fan-out blueprint
Nolen Royalty's OMCB held a live shared state of 1,000,000 boxes for tens of thousands of users on ~$850 of infra. It maps 1:1 to a live seat-map:
- **State = a bitset.** 1M bits = 125 KB; a 100k-seat venue = 12.5 KB (or 25 KB for free/held/sold). The whole "DB" is a tiny blob.
- **Never one message per change.** Coalesce changes on a ~10 Hz tick; emit **compact index-diffs** (`[[nowUnavailable],[nowFree]]`, 5× smaller than per-event); push a **full snapshot every ~30 s + a version stamp** so missed-diff / late clients re-sync; render only visible seats.
- This **wraps deltat's NOTIFY** *and fixes the 256 lag-drop*: if the worker's subscription lags, it doesn't replay; it **re-snapshots from deltat and rebroadcasts the truth.** deltat = the authoritative bitset (with capacity/holds/conflict OMCB never had); the OMCB layer = the read fan-out.

## The stadium at scale: two plans
**Stage demo (laptop, convincing):** a *real* queue UI (leaky-bucket admission ~200 wide, signed token, 429 over-cap, FIFO/raffle toggle) **+** a **simulated bot swarm** (500-5,000 bots that queue, pick, hold, ~50% abandon, *labeled simulated*) **+** the OMCB compact-diff fan-out **+** **2 Node workers** (proves the deltat-as-backplane property at K=2) **+** deltat unchanged **+** stubbed payment. Money shots: two bots race one seat → loser flips red in one tick (real conflict); 5,000 bots vs 200 seats behind a 200-wide queue → **deltat's booking count never exceeds capacity** (the headline).

**Production:** edge waiting room (Queue-it-class) **+** **CDN-fronted snapshot** (the millions "just looking" never hit the WS layer) **+** edge fan-out with passive-watcher suppression (Discord: ~90% of a big room is passive → ~90% cheaper) **+** K sticky-sharded workers **+** **sharded deltat nodes** **+** real payment inside the protected zone.

**The one kernel change worth making: `CommitHold` (AVAIL-07).** Atomic hold→book under one lock, excluding your own hold from the conflict check. Today it's **release-then-book**: a real TOCTOU window where a competitor can snipe the seat between release and confirm. Smallest kernel change, biggest payoff; "never double-sell on a single hot seat" depends on it. Decide the hold-capability model first (SEC-03: a guessable `hold_id` = slot hijack once CommitHold exists).

---

# Flagship: "Δt Live Airport Operations"

The boss-level demo: the only scenario that needs **every primitive at once, coupled, under one atomic commit**, and where getting it wrong (a plane parked with no fuel crew) is *viscerally* wrong.

**The thesis, on screen:** a flight isn't a thing: it's **a batch of segments on coupled timelines that commits all-or-nothing.** That's AVAIL-06 (atomic multi-resource booking) made physical, and it's **real today** (multi-row booking INSERT hits the real batch path).

### A flight = one atomic bundle
```
Flight DL482 = ONE atomic batch of:
  Runway 09L  [14:05-14:09)            arrival slot
  Gate A3     [14:12-15:50) +45m buf   turnaround (buffer_after)
  Tug Pool    [15:48-15:58)            pushback (1 of 4)
  Fuel Pool   [14:30-15:10)            refuel  (1 of 3)
  Runway 09L  [15:55-15:59)            departure slot
```
Either all five commit or **zero rows are written.** *"There is no half-flight."*

### Resource model (all real deltat)
- **Airport → Terminal → Gate** hierarchy; operating hours = one inherited non-blocking rule on the root (MODEL-06, EDGE-03).
- **Gate** = capacity-1 + `buffer_after: 45min`, the turnaround gap that pushes the next plane's earliest time out (MODEL-04 / AVAIL-12).
- **Runway** = capacity-1 + buffer = **wake-turbulence separation** (the dark gap between segments on the timeline, VIS-00 drawn to scale).
- **Ground pools** (tugs cap 4, fuel cap 3, catering 2, de-ice 2, baggage 6) = capacity-N shared resources, contended across flights (AVAIL-05 sweep).

### The visual
A dark airport map (reuses the stadium canvas engine): planes fly the approach, land on a runway strip (lit segments + hatched wake-gaps), taxi a bezier path to a gate, sit a turnaround (amber ghost block), depart. Gate pads fill/clear; **ground-pool utilization bars** breathe emerald→amber→rose as they saturate. Three **airline towers** along the bottom are independent live mirrors (each its own `/ws` subscription). A 60× clock drives it.

### The killer interaction: the cascade
Tap **`Close 09L: weather`** = **one real blocking rule** on the parent runway. Because blocking accumulates down the tree and availability subtracts it, every 09L slot in the window goes unavailable instantly, and the event bubbles to the root, so all three towers get the NOTIFY and **re-read** (they're never told what to draw). Then, as a staggered amber wave:
- a held inbound banks into a hold pattern;
- displaced flights **re-plan** via the multi-resource sweep (find a window where runway + gate + tug + fuel are *all* free) and **atomically re-commit**;
- a gate's turnaround ghost slides right; the Tug bar flashes **4/4 rose**; the contention propagates one more hop;
- a flight that can't re-fit a *complete* bundle **fails its batch entirely → DIVERTED** (rose), never half-booked.

One cause → a wave of "thinking" pulses → settle to emerald (re-found) or rose (failed). Nothing snaps; every ghost and taxi path *eases* so the eye follows the dependency chain.

### Honest: planes are cartoons, every constraint is deltat
Simulated: all plane motion, the map, the clock's advance, the choreography. **Real deltat:** the resource tree, inherited hours, availability (every lit/dark pixel), conflict detection, buffers, capacity sweeps, the atomic 5-leg commit, the cascade recompute, and the NOTIFY propagation. On-screen line: *"Planes are cartoons. Every reason a plane can or can't be where it is (runway free, gate buffered, crew available, the whole flight committed as one) is computed by deltat, live."*

**~70% reuse** (the live/NOTIFY loop, the stadium canvas, the Stage shell, seed helpers, multi-read fan-out); **~30% new** (the airport seed, the runway-strip / pool-bar / plane-sprite render, a flight-resolver, the sim clock, the two cascade buttons).

**Two caveats baked into the design (don't fake them):**
- **AVAIL-16:** blocking rules subtract from *availability* but aren't enforced in the booking *conflict check*, so the cascade is driven by availability-recompute + re-plan, not the kernel refusing to land a plane in a storm. Claim *"no runway is available in the storm,"* not *"deltat refuses the booking."*
- **MODEL-10:** the 5 legs share no kernel-level group id (correlated only by the demo's flight label), so "cancel the whole flight" is a client-side fan-out today: the natural next kernel feature this demo motivates.

---

# The builder & the AI-first bet (VIS-10/11/12)

The demos above *prove the engine*. This section is the other half of adoption: the demos must also be **self-deployable** and **buildable by the operator**, because the real bet is that **nobody hand-assembles a booking system in the future. They ask an AI, and the AI reaches for the free, open, self-hostable thing with a schema it can emit.** That thing is deltat+tap. Make that path beautiful and the examples become the training surface that makes us the default.

## 1. Self-deployable, free, one per vertical
Each demo (restaurant, hotel, clinic, parking, stadium, coworking) ships as a **one-command self-deploy** to any cloud provider: a `€5 VPS hosts a whole city` because the engine is a single binary with RAM-reads + amortized writes (VIS-06). Free for everybody; the operator owns their node (VIS-02). The deploy *is* the demo: "click → your own live booking system in 60s." This is also exactly what an AI agent does when asked to "set one up."

## 2. The builder: draw your space, get resources
A GUI where the operator **draws their space** instead of configuring a database:
- **Restaurant:** drop tables onto a canvas in the actual shape of the room; each table → a deltat resource (a 4-top = capacity-1 named "T7"; the bar = one capacity-N pool). Bar/booth/patio = tree nodes.
- **Parking garage:** lay out bays/levels; each bay → capacity-1, each level → a parent node; EV bays carry a `buffer_after` for charge-cycle turnaround.
- **Clinic:** rooms + practitioners as resources; "≥1 of N doctors free" falls out of `min_available` (AVAIL-04).
- **Coworking:** desks as a grid, rooms as nodes, building→floor→desk as the tree (reuses The Cascade).
- **Rental car / equipment / anything bookable:** the same primitives wearing different clothes (VIS-00).

Shapes are **SVG** (or grid, or free-form): the drawing is just a view; the truth is the resource tree it compiles to.

### Multiple layouts per space
A space holds several named **layouts** activated as resource sets: *"normal," "Christmas: +6 tables," "patio closed (rain)."* Switching a layout adds/removes resources; bookings against still-present resources are untouched. (Motivates a kernel/edge "layout = named resource set" concept; today it's an edge orchestration over create/delete.)

## 3. The four building blocks (each already part-demoed)
1. **Create your space**: the tree (building/floor/room, or restaurant/section/table).
2. **Place bookable resources**: leaves with capacity + buffer (`tap/demo` rules + resources actions).
3. **Rules of availability**: open/closed/recurring hours (the availability demo + `expandRecurrence`; note the EDGE-04 DST fix owed).
4. **Live availability**: derived, real-time, never stored (the live demo + `/ws`).

## 4. The schema is the product (human == AI)
The builder's output is a **declarative `space → resources → rules → availability` schema** that compiles to a deltat migration. The non-negotiable design rule (VIS-10/11): **a human drawing a floor plan and an AI answering "set up bookings for my garage" must emit the *same* artifact.** So:
- Define the schema as a typed, documented, example-rich format (the few-shot surface models learn from).
- The builder is one emitter; an LLM is another; both feed one `applySchema → migration` path.
- Publish worked examples (restaurant, parking, clinic, rental, salon) as canonical schema docs; these *are* the AI-discovery/adoption surface (VIS-10).

## 5. Why this sidesteps the wedge problem
Converting an incumbent restaurant off its POS-integrated booking system is hard (billing lock-in). The AI-first bet **doesn't fight incumbents**: it targets the long tail of *bookable things with no system yet*, provisioned by an AI for free, published into the federation, and monetized via discovery/search (VIS-12), not per-seat fees. The engine being free + open + self-hostable isn't generosity; it's the precondition for being what the AI reaches for.
