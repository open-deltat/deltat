# deltat

A database for time. Every booking is a half-open span on one timeline per resource; a conflict is an
overlap; availability is the gaps, swept on every read. Speaks the PostgreSQL wire protocol with no
Postgres underneath.

This file is the router, not the documentation. `docs/` holds ~2,800 lines of specification and you
should not read it all. Read the rows below that match what you are touching.

## The invariant

**INV-01: never double-book.** For every resource, at every instant, the count of active overlapping
allocations is less than or equal to capacity. Everything else is negotiable; this is not.

When a change has a failure mode, choose the direction that shows **less** availability. A reader
briefly seeing an allocation that a later crash loses is acceptable. A reader seeing a slot as free
when it is held is not. State the direction explicitly in the PR.

## Read this before touching that

| If the change touches | Read first |
|---|---|
| auth, identity, tenancy, principals | `docs/AUTH-ARCHITECTURE.md`, `docs/AUDIT-2026-08-26.md` (ship-blockers + one-way doors) |
| the WAL format, any `Event` variant or field | `src/wal.rs:28-39` (the safe/breaking classification), `docs/FORMAT.md` §7 |
| hold expiry, TTLs, any `now` comparison | `docs/AUTH-AND-PAYMENTS.md` FED-AUTH-09, `docs/REQUIREMENTS.md` HW-01..HW-03, HW-20 |
| NOTIFY, LISTEN, streams, subscriptions | `docs/AUDIT-2026-08-26.md` one-way door 1, `docs/MCP.md` §7 |
| the MCP surface, agent-facing tools | `docs/MCP.md` (requirement IDs `MCP-*`) |
| availability, conflict, capacity, buffers | `docs/REQUIREMENTS.md` AVAIL-*, `src/engine/availability.rs` mod `spec` |
| federation, multi-node, peer trust | `docs/REQUIREMENTS.md` FED-08 and NOT-05 (both gate on a real second operator) |
| payments, deposits, capture | `docs/AUTH-AND-PAYMENTS.md` §PAY |
| anything with a requirement ID in the issue | that ID in `docs/REQUIREMENTS.md`; the ID is the contract, the issue is a snapshot |

`docs/REQUIREMENTS.md` is authoritative where docs conflict. It says so, and it is right.

## Enforced principles

These are checked by a machine. They cannot be argued with, and that is the point: a principle that
is only written down decays, and this repo has the evidence for that.

| Principle | Enforced by |
|---|---|
| Wall-clock reads go through the injected `Clock` (`src/clock.rs`) | `scripts/check-no-ambient-time.sh` **and** the `clippy.toml` `disallowed-methods` rule |
| Requirement IDs are unique and every referenced ID exists | `scripts/check-requirements.sh` |
| No new `✅` requirement without a verifying symbol | `scripts/check-requirements.sh` (ratchet against `scripts/requirements-baseline.txt`) |

Run all of them: `sh scripts/check-all.sh`. CI runs them before the test suite.

**When you establish a new invariant, add a check in the same PR.** The clock seam is the template:
state it in the module doc, enforce it two ways (a path-based lint and a spelling-proof grep, because
each catches what the other misses), wire it into CI, and make the failure message name the fix.

`docs/MAINTENANCE.md` explains why this exists, how to introduce a check to a corpus that predates it
without watering the rule down, and what is worth enforcing next.

## Closed decisions

Reopening these has cost real time more than once. If you think one is wrong, say so explicitly and
cite new evidence; do not relitigate by accident.

- **Instants are milliseconds.** `Ms = i64`, end to end. The microsecond widening in `FORMAT.md` is
  superseded. Agents never see the unit (MCP-T3 mandates RFC 3339 output), so it binds only
  wire, SDK and WAL, and `@open-deltat/client` is published speaking ms.
- **`DELTAT_TENANT_PASSWORDS` does not mitigate the tenant problem.** It fails open at
  `auth.rs:100`.
- **PROTO-AUTH-08 is not regression-free.** It changes a released public config surface and inverts
  a currently-passing test that asserts the anti-property.
- **Write-side controls cannot be "added later".** They are the prerequisite for the read ACL's
  principal.
- **A stale or hallucinated read cannot become a booking is NOT a kernel property.** `INSERT INTO
  bookings` takes the caller's span with no hold. Safety currently lives in the *absence* of an MCP
  tool.
- **Payment-as-authorization is not the strongest lever.** Retracted; P2 at the earliest.
- **Adding a new `Event` variant is the safe class; adding or reordering a field inside an existing
  variant is the breaking class.** Only the second needs a `FORMAT_VERSION` bump, and batching the
  two together delays the safe change behind the dangerous one.

The full list with reasoning is in `docs/AUDIT-2026-08-26.md` under "Do not re-litigate".

## Conventions

- **Requirement IDs are permanent.** Add them, edit them, never delete one; mark it `WITHDRAWN`.
  Status legend: ✅ done and verified · 🟡 partial · 📋 planned · ⏸ deferred by design · ❌ excluded
  (anti-requirement) · ❓ open decision.
- **`✅` means running code verifies it** (PRIN-10). Name the verifying test in the requirement line,
  in backticks, so the link is machine-checkable.
- **Test-first, red then green** (PRIN-12). For a bug fix, write the failing test that reproduces it
  and confirm it goes red before the fix. A fix without a test that would have caught it is
  incomplete. Verify the test is not hollow: it must fail when the code is mutated.
- **Comment the why, never the what** (PRIN-07).
- **Build only what is needed now** (PRIN-05). This repo's specification describes a federation; the
  code should not.
- **When a doc's claim about the code goes stale, fix the doc in the same PR as the code.** Several
  docs currently carry fact-check headers naming a commit or branch that has moved on, which is how
  a spec stops being trustworthy.

## Commands

```sh
cargo test --lib                   # unit suite
cargo test --test listen_notify    # plus extended_query, auth_isolation
cargo clippy --all-targets -- -D warnings
sh scripts/check-all.sh            # every enforced principle
cargo bench --bench stress         # not gated in CI (TEST-11); see #25 before trusting it
```

CI additionally runs the lib suite under `--release`, because the release-only `overflow-checks`
guard in `Cargo.toml` is what exercises boundary arithmetic.
