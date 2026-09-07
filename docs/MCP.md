# MCP & agent access (PROTO-04): plan, requirements, sequencing

> Companion to [`REQUIREMENTS.md`](REQUIREMENTS.md) (kernel truth), [`AUTH-ARCHITECTURE.md`](AUTH-ARCHITECTURE.md) (authorization model), [`ADAPTERS.md`](ADAPTERS.md) (calendar edge). This doc is authoritative on **the agent-facing surface**: how an agent addresses a tenant, what tools exist, how two agents book across two calendars, and what travel time does to the model. Same convention: stable IDs, statuses ✅ 🟡 📋 ⏸ ❌.
>
> Research date 2026-08-26. Spec floor: **MCP 2026-07-28**. Everything below is edge work: `src/engine` changes in exactly one place (MCP-K1), and that change is a kernel primitive, not an auth concept.

---

## 0. Verdict

Build **one hosted multi-tenant remote MCP server** (`@open-deltat/mcp`) that is a thin skin over an HTTP/JSON adapter (PROTO-03), exposing **six tools with no `book` verb**. The propose-hold-commit loop is the product; the tool list is deliberately a tenth the size of the incumbent's.

Three claims that came out of the research and drive every decision below:

1. **The incumbent has the primitive and does not expose it to agents.** Cal.com ships `POST /v2/slots/reservations` with a `reservationDuration` TTL in REST, and its MCP server exposes 34 tools, none of which reserve. Their agents check availability, then book, and race. The gap is not capability, it is surface.
2. **MCP went stateless on 2026-07-28, which is a gift.** The spec now tells servers to "mint explicit handles within tools rather than rely on transport session state". A deltat hold *is* that handle: durable, server-expiring, and safe to abandon. deltat's model became the recommended pattern.
3. **The coordination layer is the named missing piece in the market.** The unified-calendar vendors describe the failure exactly ("two agents check, both write, no arbitration; the missing piece is holds with TTL, conditional writes, and an audit trail") and then do not build it.

The abandonment property is the pitch, in one line: **the only durable state a negotiation leaves behind is a set of expiring holds, so when an agent crashes mid-negotiation the calendar heals itself.**

---

## 1. What the market actually does (findings, 2026-08-26)

| Who | Surface | Concurrency story | What it means for us |
|---|---|---|---|
| **Cal.com MCP** (hosted `mcp.cal.com`) | 34 tools, Streamable HTTP, OAuth 2.1; self-host variant is stdio + API key | `get_availability` then `create_booking`. No reserve tool in MCP. Reservation TTL exists in REST only | An API-shaped MCP, not an agent-shaped one. 34 tools is a context tax and 34 permissions. **Watch item:** the day `reserve_slot` enters their tool list, our surface advantage narrows to atomicity and multi-party |
| **Nylas / Cronofy** | REST + push notifications | Re-validate at write time, reject the loser. Nylas Agent Accounts give each agent a calendar identity but explicitly **no arbitration between agents** | They solved "don't corrupt the calendar", not "let two agents converge". Their own docs frame the gap for us |
| **Skej / Clara** | Email, Slack, SMS personas | Negotiation happens in **prose between humans**, settled by a normal calendar write at the end | The negotiation layer already has demand. It is running on the least reliable transport available. This is our joint-hold customer |
| **Cal.ai** | Voice agents making bookings | Phone call to booking, same check-then-book | The mid-call double-book story lives here |
| **OpenActive Open Booking API** | C1/C2/B flow with anonymous and named **Leases** | Two-phase with optional lease; if the lease expired the booking proceeds anyway if inventory allows | Prior art that our hold model is a real pattern, and a warning: an *advisory* lease is not an invariant. Ours must be load-bearing |

**Nobody researched exposes hold-then-commit to agents, and nobody does multi-party atomic anything.**

---

## 2. The 2026-07-28 spec changed the design (read this before writing code)

| Spec change | Consequence for deltat |
|---|---|
| **Stateless core.** No `initialize` handshake, no `Mcp-Session-Id`. Each request carries protocol version, client identity, capabilities in `_meta`. State goes in tool-minted handles | We were already right. `hold_id` + server-assigned `expires_at` is the handle. No session store, so the MCP tier is horizontally scalable behind a round-robin LB while deltat stays the single writer |
| **MRTR** (`resultType: "input_required"`, client retries with `inputResponses`) replaces held-open elicitation | This is the human confirm step. `commit_hold` can demand confirmation mid-call without a stateful stream. **Design rule:** the hold TTL must outlive a human MRTR round trip (default 5 min, not 30 s) |
| **Header-based routing** (`Mcp-Method`, `Mcp-Name`) so gateways meter without parsing bodies | Rate-limit `hold_slot` and `commit_hold` on separate buckets from `find_slots` at the CDN. **MCP-S4:** these headers are metering only, never authorization, and the server MUST reject a request whose header disagrees with the parsed body, else a writer gets the reader's cheap bucket |
| **Cacheable list results** (`ttlMs`, `cacheScope`) | `tools/list` is per-tenant and near-static: long `ttlMs`. Anonymous availability gets a short `ttlMs` aligned to the VIS-13b 15-minute grid, which is the same key-space bound DOS-01 already demands |
| **Auth hardening**: RFC 9728 PRM mandatory, RFC 8707 resource indicators, RFC 9207 `iss` validation, Client ID Metadata Documents preferred over DCR (DCR deprecated, 12-month runway) | Fixes the tenant addressing question. See §3 |
| **Tasks extension** `io.modelcontextprotocol/tasks`, poll-based `tasks/get` / `tasks/update` | The container for a multi-round negotiation and for reactive-availability watches. Negotiation is a Task; the holds are the durable part |
| **Subscriptions** move to `subscriptions/listen` streams | The mapping target for deltat NOTIFY. Gated on AUTHZ-07: today the forwarder ships the full `Event` including `label` and raw hold `Ulid` |
| **Sampling, roots, logging deprecated** (12-month support) | Do not design around sampling. Never rely on the client's model |
| **2026 roadmap:** `.well-known` server metadata, Tasks productionisation, gateway/enterprise concerns | Our discovery story rides the standard rather than inventing one |

---

## 3. Addressing: how "anybody can use a tenant's MCP" works

**MCP-A1 📋 DECISION: one process, per-tenant canonical URI on a path.**

```
https://mcp.delt.at/t/{tenant}/mcp                                    <- the MCP endpoint
https://mcp.delt.at/.well-known/oauth-protected-resource/t/{tenant}/mcp   <- RFC 9728 PRM for it
```

Why the path and not a header or a tool argument:

- RFC 8707 requires the client to send `resource=<canonical URI>` and requires the server to reject tokens whose audience is a different resource. Tenancy therefore **must** be in the URI, or every tenant shares one audience and one stolen token opens all of them.
- The spec explicitly blesses `https://mcp.example.com/server/mcp` "when path component is necessary to identify individual MCP server".
- A tenant taken from a header repeats `wire.rs`'s `metadata['database']` mistake one layer up. **The tenant is whatever the validated token says it is, and the path must match it, or 403.**

Subdomains (`{tenant}.mcp.delt.at`) are strictly better for origin isolation and are the v2 move. Paths ship first because they need no wildcard DNS or per-tenant certificate ops.

**MCP-A2 📋 Self-host parity.** The same npm package runs stdio, single-tenant, API-key, no OAuth, for operators running their own deltat. Two transports, one policy module. If policy forks between hosted and self-hosted, the self-hosted path becomes the vulnerable one.

**MCP-A3 📋 Discovery.** Serve `/.well-known/mcp/server-card.json` per SEP-1649 and list in the official registry. Add `llms.txt`. Distribution channels are the Claude connector directory and the ChatGPT/Codex plugin directory (both consume remote MCP over HTTPS; ChatGPT has never supported stdio).

### The three rings

The user-facing question was "anybody should be able to get on and use a tenant's MCP". That is three different access levels, and only the middle one is interesting.

| Ring | Who | Auth | Scope | Tools |
|---|---|---|---|---|
| **R0 PUBLIC READ** | any agent, no account | none, plus Web Bot Auth signature preferred | `avail:read` | `find_slots` only, coarse: 15-min buckets, ≤7-day horizon for anonymous, booleanised, no capacity counts (VIS-13b) |
| **R1 GUEST WRITE** | any human's agent, acting for a person who is not a tenant member | OAuth 2.1 at the tenant's AS, `act` claim = agent, `sub` = the human (MCP-OBO-02) | `hold:write`, `booking:commit`, own bookings only | `hold_slot`, `commit_hold`, `release_hold`, `list_bookings`, `cancel_booking` |
| **R2 OWNER** | tenant members and their agents | same, plus a tenant-membership claim | `+ tenant:admin` | full surface, plus resource and rule management |

**MCP-A4 📋 R1 is the answer to "anybody".** A stranger's agent does not need an account *with the tenant*. It needs a verified identity from anywhere (social OIDC or magic link, IDENT-02), which the tenant's booking policy then evaluates. Signing up per tenant is the friction that kills agent bookings; identity federation is not.

**MCP-A5 ⚠️ Standing tension, decided but worth restating.** IDENT-01 says every write is authenticated: there is no anonymous write path. That is the correct call (holds are the DoS surface and a hold is free to create and expensive to squat), but it does put one OAuth hop between a fresh agent and its first hold. The two escape hatches, both already in the model: a T1 unlisted share link as a read capability, and payment-as-authorization, where a successful card authorisation *is* the write permission (PAY principle 3). An agent with a funding mandate never needs an identity ceremony.

**MCP-A6 📋 HARD GATE.** None of R1 or R2 ships before **PROTO-AUTH-08**. Status is honest and partial: `auth.rs` now supports `DELTAT_TENANT_PASSWORDS` so a named tenant accepts only its own password, but `resolve_engine` still derives the tenant from the connection's database name and still calls `get_or_create`, so any caller holding the global password can mint or enter any unnamed tenant. Shared-secret-per-tenant is not principal-binding. A multi-tenant MCP over that is a cross-tenant breach with a nicer interface.

---

## 4. The tool surface

**MCP-T0 📋 Six tools, no `book`.** Cal.com has 34. Every tool is context the model pays for on every turn and a permission the user must grant. The absence of a direct book verb is the safety pitch: an agent that hallucinates a time cannot commit it, because commit takes a hold capability the server issued for a span the server verified.

| Tool | Ring | Annotations | Args | Returns |
|---|---|---|---|---|
| `find_slots` | R0 | `readOnlyHint`, `idempotentHint` | `resource_ids[]`, `start`, `end`, `timezone`, `min_duration?`, `min_available?` | free spans, `ttlMs` on the result |
| `hold_slot` | R1 | write, `idempotentHint` via key | `resource_id`, `start`, `end`, `idempotency_key` | hold capability + **server-assigned** `expires_at` |
| `commit_hold` | R1 | write, MRTR confirm | hold capability, `external_ref?` | booking id + receipt |
| `release_hold` | R1 | write, `idempotentHint` | hold capability | ok |
| `list_bookings` | R1 | `readOnlyHint` | `resource_ids[]?` | caller's own bookings only (SHR-21) |
| `cancel_booking` | R1 | **`destructiveHint`** | booking id | ok |

**MCP-T1 📋 The hold is a capability, never a bare id.** `hold_slot` returns a signed, audience-bound, short-TTL token over `(hold_id, resource, span, expires_at, action=commit)` per MCP-OBO-03. SEC-03 already says the raw `hold_id` authorises nothing. This matters more on MCP than anywhere else, because hold ids will end up in model context, in logs, and in other agents' transcripts.

**MCP-T2 📋 `min_available` is the multi-party read, and it already exists.** `compute_multi_availability(ids, start, end, min_available, min_duration)`: `min_available = N` is the intersection of N calendars, `= 1` is a pool, `= k` is quorum. One tool argument covers "when are all five of us free", "any free room", and "at least three panellists". No new engine work.

**MCP-T3 📋 Timezones live here and nowhere else.** NOT-01 keeps them out of the kernel; the MCP edge is where they enter. Accept an IANA `timezone` argument, return RFC 3339 instants **and** a rendered local string. Never make a model do timezone arithmetic and never hand it bare epoch milliseconds: both are reliable hallucination sources.

**MCP-T4 📋 The error contract is a control-flow signal, not prose.** Every failure returns a typed `code` so the model knows which of three different things to do:

| `code` | Cause | Correct agent behaviour |
|---|---|---|
| `RETRY` | SQLSTATE **40001** serialisation failure | Retry the **same** call, bounded backoff. Never surface to the user |
| `CONFLICT` | Someone else holds or booked the span | Do **not** retry. Call `find_slots` again and pick another span |
| `EXPIRED` | Hold TTL elapsed before commit | Re-hold, then re-confirm with the human |
| `FORBIDDEN` | Insufficient scope | Step-up authorisation (403 + `WWW-Authenticate: scope=`) |
| `INVALID` | Bad span, out of window, over limits | Fix arguments. Include the actual limit in the message |

Conflating `RETRY` and `CONFLICT` is how an agent either gives up on a transient blip or hammers a genuinely taken slot. This is the observability error-taxonomy work reaching its actual consumer.

**MCP-T5 📋 Terminology hazard.** MCP has "resources" and so does deltat, and they mean different things. In all agent-facing text call deltat resources **bookables** and never expose the word "resource" in a tool description. Ship the tenant catalogue as MCP *resources* (documents) so the model can read the bookable list without spending a tool call.

---

## 5. Two agents booking across two calendars

This is the destination the whole plan is aimed at, so here is the honest decomposition.

### 5.1 What is already true

**FED-09 is the load-bearing fact: availability composition is topology-free.** Intersecting free-interval lists is commutative and associative, so the same sweep gives the same answer whether it runs inside one tenant, chained agent to agent, or gathered by a per-request coordinator over lists from twenty servers. The read is portable data.

**The write is not.** No-double-book is not invariant-confluent (Bailis), so commit-time coordination at the resource's single home is unavoidable (FED-02, Helland). No amount of protocol design removes that.

### 5.2 The coordinator, and why it is not a broker

**MCP-N1 📋** The coordinator is **per request and stateless**. Any participant's agent can be it. It is not a service, not a registry, not an indexer, and building any of those before a real second operator is forbidden by NOT-05.

```
1. GATHER   find_slots on each home            -> free/busy at D0 only, never calendars
2. INTERSECT  run the same sweep locally        -> candidate spans, ranked
3. AGREE    propose to humans / peer agents     -> MCP Task, MRTR for confirmation
4. HOLD     hold_slot on every home             -> N expiring capabilities
5. COMMIT   commit_hold on every home           -> in a fixed deterministic order
6. HEAL     any failure: release the rest       -> or let TTL do it
```

**MCP-N2 📋 Privacy is a projection, not a promise.** Step 1 exchanges the D0 free/busy projection, never bookings, labels, holder identity, or capacity counts. Two people's agents converge on a time without either learning what the other is doing. The projection already exists as a ladder rung; the work is enforcing it on the NOTIFY push path too (AUTHZ-07).

**MCP-N3 📋 Negotiation state is a Task; the only durable state is holds.** Rounds, preferences, and counter-proposals live in the MCP Task and in the agents. deltat learns nothing about intent. If both agents die at step 4, every hold expires and the calendars are exactly as they were. Design for agents that crash, because they do.

### 5.3 The atomicity ladder (state the gap, do not fake it)

| Case | Today | Correct mechanism | Work |
|---|---|---|---|
| **One bookable, one home** | 🟡 cannot double-book, but not one event | `commit_hold` holds a single write guard across the conflict check and both appends, which share one fsync (`WalCommand::AppendAtomic`) | `engine/mutations.rs:284`. The in-memory release-then-book TOCTOU is **closed**. AVAIL-07's single `HoldCommitted` event is **not built**: still two WAL records, so a torn write between them loses the booking. That fails safe (a freed slot, never a live hold *and* booking, INV-01 holds) but it is not crash atomicity |
| **N bookables, one tenant** (two colleagues on one instance; also appointment **plus** its travel interval) | ❌ **no primitive**. `check_batch_capacity` takes one `&ResourceState`, so batch writes are single-resource | **MCP-K1**: `CommitHolds(&[hold_id])`, sorted multi-resource lock acquisition, verify all live, one event, all or nothing | small, real kernel work. Lock ordering already exists (store.rs sorted batch locks, ABBA-safe). **Needs a WAL `FORMAT_VERSION` bump** for the new event variant |
| **N bookables, N homes** (two strangers, two operators) | ⏸ | TCC saga with compensation. Not atomic and cannot be made atomic without consensus | FED-07, explicitly unsolved. **Document it, never claim otherwise** |

**MCP-N4 📋 Saga discipline for the cross-home case.** Commit in a fixed order (sort by home URI) so concurrent coordinators queue rather than deadlock. Size hold TTL to comfortably exceed the whole fan-out (a 5-minute hold against a sub-second fan-out is four orders of magnitude of headroom). On partial failure, cancel the committed bookings and tell the human plainly. A visible cancellation is an acceptable outcome; a silent double-book is not.

**MCP-N5 📋 Cheat the hard case where you legitimately can.** Two parties who share one instance need no saga: model both as bookables in one tenant and MCP-K1 makes the joint booking atomic. The "co-locate a booker's calendar with what they book" mitigation is already written down (FED-07), and for the front-desk-agent scenario, where one operator holds many people's calendars, it covers most real demand.

**MCP-N6 📋 Joint tools (v0.3), three of them:** `find_joint_slots` (thin wrapper over `min_available = N`), `hold_joint` (fan-out, returns a bundle capability), `commit_joint` (atomic within a tenant via MCP-K1, saga across homes, and the return value **says which one it did**). Never let an agent believe it got atomicity it did not get.

### 5.4 Reactive availability is what makes this more than a wrapper

Standing gap-queries with atomic grant: "fire the instant a two-hour gap opens Tuesday to Thursday, and place a hold for exactly one subscriber in the same event". Every waitlist product notifies and races. Nobody grants.

**MCP-N7 📋** Over MCP this is `watch_slots` returning a Task, with `on_match: notify | hold`, backed by `subscriptions/listen`. For agent-to-agent this is the piece that turns a negotiation from polling into standing intent: both agents register interest, and the first mutually valid gap is granted to them rather than announced to a stampede. Prerequisite: AUTHZ-07, because a subscription that leaks hold ids hands out slot-hijack capabilities.

---

## 6. Travel time

The requirement: the system knows where an appointment is and how long it takes to get there, and factors that into what it offers.

**MCP-G1 📋 Travel is an interval, not a field.** The edge computes `[depart, arrive)` and books it as an ordinary interval on the mobile bookable's timeline. The kernel needs nothing: no location, no geo, no distance matrix, no new type. NOT-02 stays intact and travel becomes a first-class thing you can see, move, and conflict against.

**MCP-G2 📋 `buffer_after` is not travel and must not be stretched into it.** It is a static per-bookable turnaround (clean the room, sanitise the chair). Travel is dynamic and depends on the *pair* of adjacent appointments. Keep both concepts; do not overload one.

**MCP-G3 📋 `TravelProvider` port at the edge**, `estimate(from, to, depart_at) -> duration`, with the same factory shape as `SettlementAdapter`. Implementations: fixed matrix, then a routing API, then traffic-aware. Locations live in Tier 3 (app DB), keyed by bookable `Ulid`, never in the WAL.

**MCP-G4 📋 Travel makes MCP-K1 mandatory, not optional.** An appointment and its drive must be held and committed together, or an agent wins the appointment and loses the road to it. The travel case and the two-party case need the identical primitive, which is a strong signal it is the right one to build.

**MCP-G5 📋 The availability query gains one optional argument**, `origin`, so `find_slots` can subtract travel from each candidate gap before returning it. This is edge post-processing over the kernel sweep: the kernel returns gaps, the edge shrinks them. For a single agent this is convenience; for a front-desk agent scheduling a route it is the whole feature ("who has an opening near this one, and can I get there").

---

## 7. Other access surfaces

MCP is one skin. It should be the thinnest of several over a single policy core, because the alternative is re-implementing scope checks per surface, which is how surfaces drift.

| Surface | Verdict | Why |
|---|---|---|
| **HTTP/JSON + OpenAPI** (PROTO-03) | **build first** | MCP wraps it. Also the surface for agents with no MCP client and for plain webhooks. Building MCP first means writing the policy layer twice |
| **MCP over Streamable HTTP** | **build now** (PROTO-04) | The distribution channel: Claude connectors, ChatGPT plugins, Cursor, and the rest all consume remote MCP |
| **`llms.txt` + a machine-readable quickstart** | **build now**, hours of work | Cheapest possible discovery. Currently nonexistent |
| **`.well-known/mcp/server-card.json` + registry listing** | **build now** | Standard discovery, rides the 2026 roadmap |
| **Web Bot Auth** (RFC 9421 signatures, `Signature-Agent`, JWKS directory) | **adopt at the edge** | The right anonymous-read abuse control: cryptographic agent identity instead of IP allowlists, already enforced by Cloudflare, AWS WAF, Vercel, Akamai. Lets R0 stay open while still being accountable |
| **`subscriptions/listen`** over deltat NOTIFY | **v0.4**, gated on AUTHZ-07 | Where real-time stops being a claim and becomes a demo |
| **A2A agent card** (`/.well-known/agent-card.json`) | **later**, at the second-operator trigger | A2A v1.0, Linux Foundation, 150+ orgs. When deltat is a *participant* in someone's agent mesh rather than a tool, this is the wrapper. Same crypto as the planned signed manifest, so one trust fabric |
| **`.well-known/bookable.json`** signed manifest (VC over a schema.org graph) | **later**, FED-08 gated | Already designed in V2-DESIGN §12. Do not build before a real aggregator exists |
| **ICS / CalDAV free-busy export** | **later** | Human interop, not agent interop. One read-only endpoint buys "it shows up in my calendar" |
| **pgwire** | **keep, never promote** | Operator and developer surface. Never the agent surface: no OAuth, no scopes, no cacheability |
| **gRPC** | ❌ | PROTO-06 |

---

## 8. Requirements list

| ID | Requirement | Priority |
|---|---|---|
| **MCP-A1** | Per-tenant canonical URI `mcp.delt.at/t/{tenant}/mcp`; RFC 9728 PRM per tenant; tenant from validated token, path must agree or 403 | P0 |
| **MCP-A2** | One policy module, two transports (hosted HTTP OAuth, self-host stdio API key) | P0 |
| **MCP-A3** | `.well-known/mcp/server-card.json`, registry listing, `llms.txt` | P1 |
| **MCP-A4** | R1 guest write: federated identity, no per-tenant signup | P0 |
| **MCP-A5** | Alternate doors: T1 share link (read), payment-as-authorization (write) | P2 |
| **MCP-A6** | **Gate:** PROTO-AUTH-08 principal-bound tenancy before any multi-tenant ring ships | **P0 blocker** |
| **MCP-S1** | OAuth 2.1 RS: RFC 8707 `resource`, audience validation on every request, RFC 9207 `iss`, CIMD preferred over DCR | P0 |
| **MCP-S2** | Scopes `avail:read` / `hold:write` / `booking:commit` / `booking:write` / `tenant:admin`; 403 + `WWW-Authenticate: scope=` for step-up | P0 |
| **MCP-S3** | Bearer stops at the edge (PROTO-AUTH-06), structural test that no token constructor reaches `Command` | P0 |
| **MCP-S4** | `Mcp-Method` / `Mcp-Name` for metering only; reject header/body disagreement | P1 |
| **MCP-S5** | Per-identity concurrent-hold cap and hold-creation token bucket (squatting is the cheapest attack on us) | P0 |
| **MCP-S6** | `external_ref: Ulid` instead of free-text `label` on every agent-visible surface (**GAP-02 is now a prompt-injection control**, see §9) | P0 |
| **MCP-T0** | Six tools, no direct `book` | P0 |
| **MCP-T1** | Hold returns a signed capability, never a bare id | P0 |
| **MCP-T2** | `min_available` exposed as the multi-party read | P1 |
| **MCP-T3** | IANA timezone in, RFC 3339 plus rendered local out; no epoch ms in the model's context | P0 |
| **MCP-T4** | Typed error codes: `RETRY` (40001) / `CONFLICT` / `EXPIRED` / `FORBIDDEN` / `INVALID` | P0 |
| **MCP-T5** | "Bookable" in all agent-facing text; catalogue served as MCP resources | P1 |
| **MCP-T6** | Idempotency key on every write; replay returns the original result | P0 |
| **MCP-T7** | Server-assigned hold TTL, default 5 min, sized to outlive an MRTR human round trip | P0 |
| **MCP-C1** | Cacheable list results: long `ttlMs` on `tools/list`, short and grid-aligned on anonymous availability | P1 |
| **MCP-C2** | Anonymous reads snapped to a 15-min grid, non-aligned windows rejected (DOS-01 key-space bound) | P0 |
| **MCP-N1** | Per-request stateless coordinator, no broker, no registry | P1 |
| **MCP-N2** | Cross-party exchange is D0 free/busy only | P1 |
| **MCP-N3** | Negotiation state in an MCP Task; only holds are durable | P1 |
| **MCP-K1** | **Kernel:** `CommitHolds(&[hold_id])`, sorted multi-resource locks, all-or-nothing, WAL `FORMAT_VERSION` bump | P1, the one engine change |
| **MCP-N4** | Cross-home saga: fixed commit order, TTL ≫ fan-out, compensate visibly | P2 |
| **MCP-N6** | `find_joint_slots` / `hold_joint` / `commit_joint`; return value states atomic vs saga | P2 |
| **MCP-N7** | `watch_slots` Task with `on_match: notify \| hold` (reactive availability) | P2, the differentiator |
| **MCP-G1** | Travel modelled as a booked interval, kernel unchanged | P2 |
| **MCP-G3** | `TravelProvider` port, locations in Tier 3 | P2 |
| **MCP-G5** | Optional `origin` on `find_slots`, edge-side gap shrinking | P3 |
| **MCP-O1** | Per-tool RED metrics, hold-to-commit conversion rate, abandoned-hold rate, `RETRY` frequency | P1 |
| **MCP-O2** | Every commit records agent and principal (delegation not impersonation, FED-AUTH-03) | P0 |

---

## 9. Risks

**Prompt injection through booking labels is the sharpest new risk MCP adds.** Today `label` is free text in the kernel (`model.rs:61/189`). Under MCP, one tenant's booking label lands in another party's agent context during a joint availability read or a NOTIFY push. That is attacker-controlled text entering an agent's instruction stream. GAP-02 (`label` becomes an opaque `external_ref: Ulid`) was hygiene; it is now a security control, and it should be enforced by a test that no free-text byte crosses an agent-visible surface.

**Hold squatting is the cheapest attack.** Creating a hold costs an attacker nothing and costs the tenant a slot. Idempotency collapses retries, not fresh-Ulid floods. The bounds are per-identity concurrent-hold caps, TTL, and cost (MCP-S5, L6).

**Over-trusting the agent's clock.** Agents will send stale spans confidently. Server-assigned expiry (shipped) and span re-validation at commit are what keep that harmless.

**Spec churn.** MCP moved from stateful sessions to a stateless core in about a year, and deprecated sampling, roots, logging, and HTTP+SSE on 12-month runways. Pin to 2026-07-28, keep the protocol adapter one thin module, and never let spec types leak into policy.

**Positioning risk.** If Cal.com adds `reserve_slot` to its MCP tool list, the surface advantage shrinks to atomicity, multi-party, and reactive availability. Those are the defensible three. Weight the roadmap toward MCP-K1 and MCP-N7 rather than toward tool-count parity.

---

## 10. Sequencing

Each phase ends in something externally demonstrable.

| Phase | Contents | Gate to start |
|---|---|---|
| **P0 unblock** | PROTO-AUTH-08 principal-bound tenancy, delete `get_or_create` on unauthenticated names | none, this is the blocker |
| **v0.1 read** | HTTP/JSON adapter, `find_slots`, R0 anonymous coarse read, `llms.txt`, server card, registry listing, typed errors, cacheable lists | P0 |
| **v0.2 write** | OAuth RS (MCP-S1/S2/S3), R1 guest ring, `hold_slot` / `commit_hold` / `release_hold` as capabilities, idempotency, MRTR confirm, hold caps and metrics | v0.1 |
| **v0.3 joint** | MCP-K1 kernel multi-hold commit + WAL version bump, `find_joint_slots` / `hold_joint` / `commit_joint`, coordinator, cross-home saga with honest labelling | v0.2 |
| **v0.4 reactive** | AUTHZ-07 NOTIFY projection, `subscriptions/listen`, `watch_slots` with atomic grant | v0.3 |
| **v0.5 travel** | `TravelProvider`, travel intervals, `origin` on `find_slots` | v0.3 (needs MCP-K1) |

The demo that sells this is v0.3 plus v0.4: two agents, two calendars, converging on a time neither party disclosed, holding both sides, committing atomically, and healing themselves when one agent is killed mid-negotiation.

---

## 11. Open decisions

1. **Hosted or self-host first.** Recommendation: build the package so both work from day one (MCP-A2), operate a reference instance because connector directories only list remote HTTPS servers, and keep the self-host path first-class because it is the OSS story.
2. **Whose authorization server.** deltat is never an issuer (IDENT-03). Delegate to WorkOS, Auth0, Clerk, or Keycloak. This is a cost and time decision, not an architectural one, but it blocks v0.2.
3. **How hard to push R1's identity requirement.** IDENT-01 (no anonymous writes) is decided and correct. The question is whether payment-as-authorization ships as the alternate door in v0.2 or waits. It removes the identity hop entirely for commercial bookings.
4. **Whether `commit_joint` ships before the cross-home case is solvable.** Recommendation: yes, atomic within a tenant, with the return value stating which mode ran. The single-operator front-desk case is real demand and does not need federation.
