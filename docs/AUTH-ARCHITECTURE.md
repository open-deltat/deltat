# Auth Architecture — SWFCA (Single-Write-Front, Capability-At-Edge)

> Companion to [`REQUIREMENTS.md`](REQUIREMENTS.md) and [`AUTH-AND-PAYMENTS.md`](AUTH-AND-PAYMENTS.md). Authoritative for **who needs auth and when, how visibility broadens, how delegation works, where every datum lives, and how the public surface survives a flood.** Same legend as the forever spec: each line is a checkable requirement with a stable ID; statuses use ✅ 🟡 📋 ⏸ ❌ ❓. Where this doc and REQUIREMENTS conflict, REQUIREMENTS wins on **kernel** facts; AUTH-AND-PAYMENTS wins on the **payment edge**; this doc is authoritative on the **authorization model** (visibility, request-identity, delegation, storage tiers, DoS).
>
> **Fact-checked against HEAD (`feat/clock-seam`).** Verified directly: `command.rs` depends only on `model` + `ulid`; `model.rs:59` `IntervalKind::Hold` carries only `expires_at`; `model.rs:61/189` `label: Option<String>` is free text in both `IntervalKind::Booking` and `Event::BookingConfirmed`; `auth.rs:18` returns `DELTAT_PASSWORD` for any user (`auth_ignores_username`); `wire.rs:72-85` `resolve_engine` derives the tenant from unauthenticated `client.metadata()['database']` with an unconditional `get_or_create("default")` fallback; `wire.rs:176/188` is `release_hold` then `confirm_booking` (a real TOCTOU, no atomic `CommitHold`); `clock.rs:24` `SystemClock` reads steppable `CLOCK_REALTIME`; **`wire.rs:778` the LISTEN/NOTIFY forwarder does `serde_json::to_string(&event)` and pushes the full `Event` — `label` and the raw hold `Ulid` included — to any subscriber.**

## 0. The spine (PROTO-AUTH-00, already principle)

The kernel takes `(Command, idempotency Ulid, integer now)` and **nothing else**. No secret, token, identity, price, ACL principal, or PII crosses into `src/engine`. **Every** authorization concept — visibility, identity, grants, capabilities, fencing epochs, OAuth, payment, confirmation, customers — lives **above** `Command` dispatch. This doc specifies that edge. `src/engine/*` needs zero change for the entire story.

**AUTHZ-00** 📋 **SWFCA.** One mechanism, applied four times (MCP auth, friend-share, cross-calendar, payment): (1) identity strength matched to the action; (2) a read/write ACL match on the `Command` variant **before** dispatch (PROTO-AUTH-10); (3) for commits, a verify-then-lock check against in-process edge caches **before** the per-resource write lock (FED-AUTH-06). The raw `hold_id` is a non-authorizer (SEC-03); the OAuth bearer is structurally dropped at the edge (PROTO-AUTH-06).

## 1. The three hard gates (SHR-90 — prerequisite or nothing)

Nothing capability-gated ships before these land, **in order**. Until then the blast radius of any grant is the entire multi-tenant database.

| Gate | ID | What | Why it gates everything |
|---|---|---|---|
| P0a | **PROTO-AUTH-08** 📋 | Bind tenant to the authenticated principal; **delete** the `wire.rs:72` `metadata['database']` path; missing binding = reject, never `get_or_create("default")`. | Today any caller mints/enters any tenant by naming it. No isolation, no identity to key any other control on. |
| P0b | **AVAIL-07** 📋 | Atomic single-lock `HoldCommitted` (one event, no release-then-reinsert gap), excluding only that hold from the conflict check. | Today's `release_hold`→`confirm_booking` (`wire.rs:176/188`) is a real TOCTOU. A commit-capability over it makes replay+race a **double-book** and a **double-capture**, not an idempotent echo. |
| P0c | **HW-01 / HW-20** 📋 | Monotonic-floored wall expiry; adversarial backward/stalled-clock tests. | `clock.rs:24` reads `CLOCK_REALTIME`; a backward NTP step makes an expired hold **and** an expired short-TTL capability read as **live**, voiding the leaked-token-dies-in-120s guarantee. |

**Adjacent open breach: GAP-02 / PROTO-AUTH-14** 📋 — `label` is still free-text in the kernel (`model.rs:61/189`). Until `label → external_ref: Ulid` lands, every cross-calendar / grant / confirmation adapter **MUST** pass `None` / an opaque `Ulid`, enforced by a test (the documented clippy guard does not exist).

## 2. Who needs auth, and when (the matrix)

Reads MAY be anonymous **only** through the cached, cost-bounded, publisher-opted-in path. Every **write** is always authenticated and attributable for every actor — there is **no anonymous write path** (PROTO-AUTH-05). Identity strength: `none` < lightweight social OIDC / magic-link < OAuth 2.1 bearer < home-signed capability. `SelectHolds` returns the bare hold `Ulid` (`queries.rs:253`) — owner/operator data, scoped off the open surface, filtered to the caller's **own** holds (**SHR-21**); resource-wide is `mayAdmin`-only.

| Actor | Read free/busy | Read full detail | REQUEST (hold/book) | CONFIRM (CommitHold) | Book-on-behalf | Manage / publish | Pay deposit |
|---|---|---|---|---|---|---|---|
| **Owner / operator** | lightweight (own tenant) | lightweight (own) | lightweight (write) | capability | lightweight (own resource-set) | lightweight; destructive ops = elevated role | receives (Stripe Connect MoR) |
| **Friend / known person** | verified id + owner grant | verified id + **higher** grant | verified id (write) | capability (only a hold THEY placed) | bounded delegation cap | denied | verified id + instrument |
| **Anonymous stranger** | opted-in coarse only, rate-bounded | never | must acquire a verified id first | capability (raw hold_id never authorizes — SEC-03) | denied | **payment-as-authorization** or never |
| **Returning customer** | verified (iss,sub) | own history only | verified id (+ returning policy) | capability | bounded delegation cap | denied | policy: waive / hold / strict |
| **AI agent / MCP** | inward scoped cap (bearer dropped) | cap inheriting user's detail scope | inward write cap (records agent + user U) | commit-cap, re-verified **every** commit | attenuated cap, depth-capped | only if owner-delegated | cap states price; edge picks rail |
| **Federated peer / reseller** | topology-free portable read (FED-09) | never across the boundary | Try-Confirm-Cancel escrow; fresh cap per home | home-signed cap, aud == that home | fresh-minted per hop (depth-0) | denied | reversible (capture after commit) |

**IDENT-01** 📋 Request-requires-auth: `book`/`hold`/`CommitHold` are always authenticated. The stranger must cease being anonymous before requesting. **HONEST STATUS:** none of this matrix is enforced at HEAD — `execute_command_inner` (`wire.rs:120-382`) dispatches every Command with no principal check; the effective default is **T4/D2 world-readable-full-detail** with a forgeable tenant. P0a is the diff that makes any cell true.

## 3. Visibility — private by default, opt-in to broaden

**VIS-13** 📋 **Default-off.** Every resource defaults to `(T0, D0)`. Two orthogonal axes (the universal Google/Outlook/CalDAV/JMAP model), each a per-resource **edge** flag the kernel never sees (SEC-02, FED-09 — the read is topology-free).

**Audience ladder:**

| Rung | Meaning | Mechanism |
|---|---|---|
| **T0 PRIVATE** (default) | owning tenant's authenticated principal only | per-tenant Engine isolation (SEC-07) + read ACL |
| **T1 UNLISTED** | holder of an unguessable ≥128-bit token reads free/busy; not indexed | link == capability; token in the URL **# fragment** (out of logs/Referer); per-link epoch to revoke |
| **T2 NAMED-GRANT** | a registry row grants a specific principal/DID (the friend case) | Tier-2 grant row; JMAP rule — cannot grant a right you do not hold |
| **T3 ORG/REALM** | all principals in the publisher's tenant | tenant-scoped flag |
| **T4 PUBLICLY-DISCOVERABLE** | anonymous geo/time search (VIS-03) | pushed to the indexer; **last** rung, most-explicit opt-in, never implied by T1-T3 |

**Detail ladder (independent, default D0):** `D0` free-busy-only (boolean bookable/not, coarsened on public; no labels, no `external_ref`, no holder identity, no capacity counts, **never hold ids**) → `D1` limited (publisher-marked metadata, no PII) → `D2` full (booking detail; NAMED-GRANT/ORG only, never the public default).

**VIS-13a EXISTENCE-HIDING** 📋 (JMAP rule): when only free/busy is granted the resource MUST behave as though it does not exist — identical responses for "does not exist" and "exists but you can't see it" (no timing/error distinction), and ULID enumeration MUST NOT reveal an unpublished resource (PROTO-AUTH-05). Unenforceable on the current read path (`SelectResources` lists all, `SelectAvailability` computes for any id) — gated on the PROTO-AUTH-10 read ACL.

**VIS-14 NO-CASCADE** 📋 (tested invariant, before any publish path): a child defaults to `T0` **regardless of parent tier** — creating a child under a published parent leaves it private and out of the index. A bulk "publish subtree" must enumerate and **name** each node, never inherit. Test: create→publish parent→create child→assert child not discoverable, not indexed.

### 3.1 The NOTIFY push-path leak — the ladder's missing half (CRITICAL)

**AUTHZ-07** 📋 The (audience, detail) projection **MUST** govern the LISTEN/NOTIFY payload, not just `SELECT`. The five-rung ladder, D0 coarsening, existence-hiding, and every DON'T below govern the read path **only**; the push path is silent. At `wire.rs:778` the forwarder serializes the full `Event` (`model.rs:142` derives `Serialize`) and pushes it to any subscriber on `resource_{rid}` — including `BookingConfirmed.label` (free-text PII) and the raw `HoldPlaced.id` (a slot-hijack capability once `CommitHold` ships, SEC-03/T-06) — gated by nothing but the shared password and `MAX_SUBSCRIPTIONS_PER_CONNECTION`. **This is D2-full at T4-public.** Fix: the forwarder must project each Event through the subscriber's grant **before** `serde_json::to_string`: at D0 emit a contentless "capacity changed / k-free-now" tick, never the Event struct; strip `label` unconditionally on any non-owner channel; **never** emit the hold `Ulid` on any channel (HoldPlaced → a contentless tick). Test: assert no hold `Ulid` and no `label` byte ever crosses the NOTIFY sink for a non-owner subscriber (mirror the PROTO-AUTH-06 structural test).

### 3.2 Discovery-vs-private reconciliation (T-09) and the coarse feed

Four opt-in mechanisms resolve the tension: (1) **opt-in indexing** — absence is the default; "searchable" == "what a publisher chose to publish"; (2) **free-busy-only public** — anonymous reads get coarse busy/free, never detail/PII/holds; (3) **per-resource flags, no cascade** (VIS-14); (4) **publisher-opt-in indexer** holding a **stale AP hint** (T-08), never a commit point — every booking re-validates at the single home (FED-02).

**VIS-13b COARSE FEED** 📋 (anti-scrape/anti-inference): quantize public slots to 15/30-min buckets, not second-precision; publish the booleanized sweep output ("k slots free") not the underlying booking/hold/capacity-N segments; bound the horizon (30-90d); **rate-limit AND cap sampling frequency per identity** (a scraper polls — window-width caps alone do not stop it). **Honest limit:** coarse-graining raises inference cost, it does not defeat it — for a single-capacity resource, k-count temporal sampling near-reconstructs the schedule; for high-sensitivity single-capacity resources, suppress `k` and publish only "any free slot in bucket" booleans.

**DON'Ts:** never default-publish; never cascade parent visibility onto children; never put labels/holder-identity/PII on a public or anonymous feed; never expose hold **identifiers** on **any** read **or push** surface; never publish exact second-precision spans or capacity counts publicly; never let the indexer be a commit point; never run an unauthenticated endpoint richer than coarse free/busy.

## 4. Request identity (IDENT-*)

**IDENT-02** 📋 **Default = social OIDC + magic-link.** Sign-in-with-Google/Apple and email magic-link, equal status. Lowest friction, a verified deliverable channel (`email_verified`) for the confirmation, and a stable `(iss, sub)` for returning-customer recognition + ban-by-identity. This is **IAL1 account-control, not legal-identity proofing** — "Google is the ultimate auth" is true operationally, false in the assurance sense; never let it leak into KYC/regulated-deposit contexts (IAL2). Always offer magic-link so no single IdP is a hard dependency.

**IDENT-03** 📋 **Validate a tap-audienced ACCESS token, key on `(iss, sub)`.** As an OAuth 2.1 Resource Server (PROTO-AUTH-02), validate a token minted **for tap** (`aud == tap`'s canonical URI, RFC 8707), **NOT** the raw Google ID token (whose `aud` is the front-end `client_id` — the classic confused-deputy). Validate signature + audience offline against the IdP's JWKS on **every** write. Key the customer on `(iss, sub)`, never email. tap runs no password DB and is never an issuer — delegate issuance to WorkOS/Auth0/Clerk/Keycloak. Audience ≠ tenant (PROTO-AUTH-04): the tenant comes from a separate claim bound via PROTO-AUTH-08.

**IDENT-04** 📋 **Sybil resistance ≠ OIDC.** Social OIDC gives **attribution** and ban-persistence, not sybil **resistance** — minting `(iss,sub)` pairs happens at Google/Apple, outside tap's control. The real anti-sybil levers for write floods are L5 (PoW/Turnstile/Privacy-Pass at account-first-touch), the L7 admission queue, and — strongest — the **step-up card-hold**: payment-as-authorization makes minting cost real money.

**Trust ladder:** anonymous read (lightly-keyed) → Tier-1 write (social/magic-link, the floor) → Tier-2 step-up (SMS OTP — NIST-deprecated, step-up only — and/or a Stripe card hold) → returning customer (bind a passkey after first booking for 3-5s phishing-resistant re-auth).

## 5. MCP on-behalf-of (MCP-OBO-*)

**THE rule:** the user's token is **never** handed to the MCP server; the home **mints** the agent a fresh, scoped, attenuable capability from the registry row, and **that** is what gets transferred off.

1. **MCP-OBO-01 CONSENT once.** The user grants the assistant scoped access at the home's OAuth AS. The MCP server is a Resource Server (PROTO-AUTH-02), the agent is its own client with its own `client_id`, the user stays the distinct `sub`. The home decides at consent whether the grant is attenuable (flat single-hop by default; multi-block only with an explicit `mayShare` grant — FED-AUTH-07).
2. **MCP-OBO-02 EDGE EXCHANGE.** The inbound bearer is validated and **stops** at the edge (PROTO-AUTH-06 structural no-passthrough — no constructor into `Command`, a property with a test). The home mints a fresh Biscuit scoped to the consented grant. In OAuth terms this is RFC 8693 token-exchange: a new token bound to tap's audience carrying `actor=agent` + `user=U` (the `act` claim).
3. **MCP-OBO-03 THE CAPABILITY.** `place_hold` returns a capability, not a bare `hold_id` (SEC-03): a home-signed, audience-bound, short-TTL, attenuable Biscuit `sign(hold_id, resource, span, expires_at, action=commit, [price], audience)`, **Ed25519, never HMAC** (FED-AUTH-01; alg fixed by format kills JWT alg-confusion).
4. **MCP-OBO-04 ATTENUATION.** A sub-agent gets a strictly narrower slice by appending a Datalog caveat block **offline**, no round-trip; Biscuit can only restrict (FED-AUTH-12 monotonic). Bounded: depth-capped from measurement (start at 3), the verifier rejects `block-count > MAX` **before** running Datalog (deep-chain CPU-DoS guard, SHR-31).
5. **MCP-OBO-05 COMMIT (two cadences).** Verify-then-lock (FED-AUTH-06): one `verify_strict`, `typ==commit-cap`, `aud ==` this node's URI, `action==commit`, resource-set contains target, nonce/epoch == the edge fencing store's stored truth, a hard tenant compare against the PROTO-AUTH-08 binding, monotonic-floored wall expiry — **all before** the per-resource write lock — then the unchanged kernel `CommitHold(hold_id)`. The bearer is verified **once per connection** (session authz); the commit-capability is re-verified **every commit** — encode the two as separate code paths with separate tests; never use the session/connection as the commit principal (PROTO-AUTH-03).
6. **MCP-OBO-06 CONSENT LEGIBILITY.** A `mayShare` consent **MUST** surface the concrete caps the user authorizes (price ceiling, daily/total count, commit-TTL ≤120s, window, depth). Pin an absolute block-count ceiling as a fixed constant enforced before any d-block verify, and benchmark the real caveat ruleset on the deploy target before exposing `mayShare`.

**Delegation not impersonation** (FED-AUTH-03): every commit records both agent and user U via a signed receipt. Hop depth (cross-home) and delegation depth (user→agent→sub-agent) are orthogonal; each cross-home hop mints fresh, so the cross-home path is depth-0 by construction (FED-AUTH-02 — a cap for node A is rejected by node B on audience). **Revocation** kills the agent without touching the user: delete the registry row + bump the per-grant fencing epoch (SHR-40) + short TTL backstop; the user's credentials and co-grantees are untouched.

**HONEST STATUS:** zero of this is enforced at HEAD — no bearer, no capability, no actor claim, no exchange boundary exist in `src/`. The no-passthrough test (SHR-91) cannot exist because the PROTO-AUTH-11 runner it guards does not exist. These properties are **📋 planned**, not "verified/structural."

## 6. Storage layering (AUTHZ-01..03) — the fork, decided

Three durable tiers; the kernel `Ulid` is the only cross-tier key. **deltat GRANTS live in Tier 2** (deltat-node-adjacent), business AUTH/identity/money in Tier 3.

| Tier | Home | Holds | Keyed by | Properties |
|---|---|---|---|---|
| **1 KERNEL** | append-only WAL (`src/engine`, zero change) | resources, rules, holds (`span`+`expires_at` only — `model.rs:59`), bookings, the authoritative `BookingConfirmed` fact (FED-09 portable truth) | — | identity/money/secret-blind; 10-variant MODEL-11 vocabulary frozen |
| **2 NODE-ADJACENT** (**AUTHZ-01**) | its own append-only log, co-located with but **outside** the WAL | the CCAP **grant-registry row** (the legible truth, SELECT-enumerable, revocable by delete — friend-grant, agent delegation, reseller cap **all** live here; the Biscuit is minted **from** the row, the row is the only mint source); the **fencing nonce/epoch store** (the authoritative truth the verifier reads — FED-AUTH-08); stateful-cap accumulators (reserved at hold time, SHR-25); signed booking receipts | kernel `Ulid` | reconstructed on restart, **fail-closed** on kernel disagreement; per-tenant isolated; read on the hot commit path as in-process integer/string compares, **zero network I/O before the write lock** |
| **3 APP DB** | tap/web's Postgres/Drizzle | `(iss,sub)` customer identity + returning-customer history; OAuth refresh tokens (AES-GCM, KMS, rotation + reuse-detection — PROTO-AUTH-12); payment-intent state (`hold_id → payment_intent_id, status`); external-calendar `Credential` + `BookingReference`; confirmation/notification state; operator roles; discovery/publish index | stores kernel `Ulid`s downward only | the wrong latency/trust domain for a per-commit bind-check — which is exactly why grants are **not** here |

**Why Tier 2 not the app DB:** the per-commit bind-check must be an in-process compare with zero network I/O before the write lock (FED-AUTH-06) — the app DB serializes commits and breaks the sub-ms claim; the kernel is the wrong purity domain. Co-location is the payoff: grants travel with the node, revocation is one epoch bump, audit is one SELECT — exactly what registry-less object-capability throws away (no table = no enumerate, no delete-to-revoke).

**AUTHZ-03 SHR-41 (crash direction, pinned + tested):** persist the epoch bump / row delete / accumulator reservation **before** the action it gates. On restart the kernel WAL replays to truth first, then edge stores reconcile (a hold the fencing store knows but the kernel doesn't ⇒ revoked; a committed hold the store missed ⇒ kernel wins, idempotent). Test: crash after epoch bump but before its fsync ⇒ token stays revoked (HW-20 style). Untested today re-opens the revocation hole.

## 7. Payment & confirmation (PAY-06..10)

Pure edge, zero kernel change — the kernel hold **already is** authorize-and-hold, glued to money by the hold `Ulid` as idempotency key.

**Core mapping (PAY-01):** `place-hold` → `authorize()` (Stripe `PaymentIntent(capture_method=manual)` → `requires_capture`); `CommitHold` → `capture()`; expiry/cancel → `void()`; no-show → partial capture (`amount_to_capture = fee`, remainder auto-released — PAY-03). **Capture fires on the first-time `HoldCommitted` WAL append** (FED-AUTH-11), never on "verify succeeded" — a within-TTL replay double-captures nothing. Gated on AVAIL-07.

**PAY-06 ADAPTER FACTORY.** One `SettlementAdapter` port `{capabilities()→RailCaps, authorize, capture, void, charge, refund}`, factory-selected per-resource, every call idempotency-keyed on the hold `Ulid`. The capability is **rail-agnostic** — it states price + audience + action; which adapter settles is an edge decision keyed by the same `Ulid`. **PAY-04 is structural in the port:** every Stripe call runs on the resource's connected account as merchant of record via Connect **direct** charges (not destination — those custody); the platform takes only `application_fee_amount`; there is **no adapter method that lands principal in a platform balance**.

**PAY-07 `RailCaps` `{holds, partial_capture, void, refund, immediate_only}`** lets the booking-policy layer refuse a deposit-requiring flow on an immediate-only rail at **config time** — making "x402 is not the deposit engine" a type-level fact. **x402** is one **deferred** adapter for pay-to-**use** the federation (search metering, VIS-12, PAY-x402-01), never deposits — its EIP-3009 push scheme is immediate/irreversible (Stripe's own x402 runs immediate-capture), the opposite of a manual-capture hold; kept off any refund path; nonce/EIP-712 domain bound to the hold capability.

**Payment-as-authorization (PRINCIPLE 3):** for commercial bookings a successful `authorize()` **is** the write-permission to hold — the inverse of the capability path (there a signed capability proves *who* may commit; here a settled payment proves the caller is *committed enough* to occupy the slot). Payment and identity are independent composable gates.

**PAY-08 CONFIRMATION = edge policy over the kernel hold TTL** (not a kernel feature): hold = pending-confirmation-with-TTL; confirm-click → `CommitHold`; no-confirm → reaper expires → slot frees. Two flavors, both edge config: payment-backed (authorize at hold, capture at confirm) and personal/restaurant ("hold your table 15 min" = a hold with a 15-min `expires_at` + a confirm webhook). The confirm token is bound to the hold `Ulid` and never enters `src/engine`.

**PAY-09 RETURNING-CUSTOMER = Tier-3 policy lookup against the edge-VERIFIED identity** (never caller-supplied, else the deposit waiver is forgeable — PROTO-AUTH-04): `customer{verified_identity_ref(iss,sub), psp_customer_id, booking_count, no_show_count, saved_pm}`; first-timer → full card hold, returning-good-standing → saved-card off-session or no-deposit, prior no-shows → strictest. The kernel is identity- and money-blind throughout (MODEL-09/PAY-05).

**PAY-10** 📋 Cap deposit-backed hold TTL to the rail's `RailCaps` auth window (Stripe ~7d / 5d Visa MIT) or use extended authorizations; a hold whose `expires_at` outlives the window has its `PaymentIntent` silently void while the kernel hold reads live — define capture-failure-on-commit as an explicit refund/retry path.

## 8. DoS (DOS-*) — layered, edge-first, request-requires-auth is the primary lever

Asymmetry: gate at the cheapest layer that sees the attack. Four scarce resources (cheapest first): wire bytes → read CPU → the Node fan-out loop → the fsync write path. **A fifth (DOS-04):** tenant creation — `resolve_engine`'s lazy `get_or_create` on an attacker-named database (`wire.rs:78`) amplifies a new WAL file + reaper per name; bound it to authenticated principals (PROTO-AUTH-08).

| Layer | Where | What | Folded caveat |
|---|---|---|---|
| **L0 CDN/WAF** | edge | absorbs volumetric L3/L4/L7 floods | — |
| **L1 cached availability snapshots** | edge (deltat fills) | derived (AVAIL-01), SWR, NOTIFY purges per Event; reads served at the rate of **changes** | **DOS-01:** SWR only bounds read-DoS if the **key space** is bounded — snap anonymous `[start,end]` to a 15-min grid and reject non-aligned windows, or a 1ms cache-buster collapses L1 to origin-fill |
| **L2 OMCB compact-diff fan-out** | edge (Node) | coalesce ~10Hz, compact diffs, re-snapshot on lag (never replay) | **DOS-02:** forwarders spawn per subscription (`wire.rs:775`) — cap **total forwarders per principal**, not just per connection |
| **L3 per-query cost bounds** | kernel + edge | kernel caps in `limits.rs` (90d window, 1000 IN-list/batch) + MODEL-04 saturating arithmetic | edge anonymous caps (window ≤7d, IN-list ≤25) are **not assumed** — `limits.rs` has no per-tier cap, this is work |
| **L4 request-requires-auth** | edge (primary lever) | ban by `(iss,sub)` not IP; write ACL split before dispatch (PROTO-AUTH-10) shipped with the open-read limit | — |
| **L5 anonymous-but-accountable** | edge | Turnstile / PoW / Privacy-Pass | must sit behind a real per-identity limit, never the sole gate |
| **L6 write-amplification limits** | edge + kernel backstop | per-identity write token-bucket (abandoned holds count), auto-expiring holds (AVAIL-11), idempotency collapse; INV-01 capacity backstop | idempotency collapses **retries**, not adversarial **fresh-Ulid** floods; INV-01 bounds **overbooking**, not write-**rate** (a churner saturates fsync without overbooking) — only L6+L7 bound that |
| **L7 admission queue** | edge (decoupled) | leaky bucket, 429 over-cap, FIFO/raffle; deltat is Layer-2-only | keep strictly decoupled (the Eras meltdown was cross-layer coupling) |
| **L8 connection cap + drain** | deltat (shipped) | ENG-24 Semaphore(256) + 10s drain; per-tenant isolation | coarse last-resort, not a strategy |

**DOS-03 (new surface the auth layer adds):** capability verify **is** the auth, so you cannot rate-limit by identity **before** paying the verify cost — a well-formed-but-invalid-signature flood is a pre-auth CPU-DoS. Gate capability-verify behind the cheaper once-per-connection bearer/connection identity, and drop the connection after N failed verifies. Plus FED-AUTH-06 verify-then-lock (no synchronous network I/O, never under the lock), reject `block-count > MAX` before Datalog (SHR-31), fail-closed-on-kid-miss with **background** refill.

**The dividing line (PROTO-AUTH-00):** everything identity/rate/auth/queue/cache is **edge**, zero `src/engine` change; deltat contributes only ENG-16 cost caps, ENG-24 connection cap, MODEL-04 saturating arithmetic, and the INV-01 capacity invariant.

**HONEST STATUS:** at HEAD the only write-amplification bounds are the kernel cost caps + the 256-connection Semaphore. L0-L7 are unbuilt 📋; L4 (the primary lever) provides zero sybil resistance because `auth.rs:18` is a shared password with no identity to key on. Until PROTO-AUTH-08, this is a roadmap, not a defense.

## 9. Why this is both powerful and efficient

**Powerful:** SWFCA is the only model simultaneously **legible** (read rows, revoke by deletion/epoch, enumerate with a SELECT) and forge-proof + federated + attenuable. The Tier-2 registry buys legibility — "show everyone with access" is a SELECT, "remove access NOW" is a row delete + epoch bump — exactly what registry-less object-capability throws away. The orthogonal boolean rights-map expresses splits a calendar ACL ladder cannot (`mayReadAvailability` without `mayReadBookings`). Because the FED-09 read is topology-free, the same sweep composes across one home or twenty.

**Efficient:** the hot commit path is one `verify_strict` (~28-41µs) + in-process integer/string compares against the co-located Tier-2 caches — zero DB lookup, zero network I/O, all before the per-resource write lock (verify-then-lock). The registry table is touched only at **mint** (cold path). The read path is removed from deltat entirely by the L1 snapshot cache. The verify cost is a ~20-35% tax on a group-commit-amortized durable commit — **benchmark it on the deploy target with the depth cap set from measurement** (start at 3), not assumed.

**The deep reason both hold:** by exiling all auth to the edge, the kernel is freed to be a dumb-fast correct allocator (never overbook, instantly), while the edge is freed to be arbitrarily expressive about identity/grants/money without touching the latency- or correctness-critical core. Purity is what makes both the power and the speed achievable.

## 10. Build sequencing (NOT-05 — record now, build on trigger)

The model is recorded here for free (drawing the boundary). Crypto/federation/registry code is built only on a concrete trigger. The **next sprint is grant-model-independent**: P0a (PROTO-AUTH-08) + HW-01/02/03/20 + AVAIL-07 + AVAIL-08 + the AUTHZ-07 NOTIFY-projection fix + GAP-02. For the single-operator demo the Tier-2 grant store can be a trivial in-process map with only the durable log + revocation epoch deferred (local PASETO key, edge fencing store, no `.well-known`, no Biscuit). Federation, the durable grant registry, the indexer, and x402 wait for a real second operator / multi-hop trigger (FED-08).

**deltat is single-node-durable and not HA.** A DoS saturating legitimate write demand is indistinguishable from organic flash-sale demand at the fsync writer; the admission queue (L7) is the only thing bounding absolute concurrent writes, per-tenant isolation the only blast-radius container. Flag "not HA" for prod; shard by tenant past the ceiling.