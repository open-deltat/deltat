# Session handoff — deltat + tap booking-protocol work

**Purpose:** pick this up from a *fresh* conversation without the old chat history. Read this + `docs/REQUIREMENTS.md` (the authoritative spec) and you have full context.

_Last updated: 2026-06-18. All work below is UNCOMMITTED on disk (survives a reboot; only the running dev servers die)._

## Repos & state
- **deltat** (`~/repositories/deltat`) — Rust time-allocation DB. Branch **`feat/clock-seam`**, ~24 uncommitted files. The engine. Authoritative spec: `docs/REQUIREMENTS.md`.
- **tap** (`~/repositories/tap`) — `@open-tap/client` SDK + Next.js demos. Branch **`main`**, ~40 uncommitted files. SDK resolves to `packages/client/dist` (rebuild with `cd packages/client && bun run build` after editing SDK source).
- **Nothing committed or pushed this session.** Consider committing before major new work.

## How to restart the dev environment after reboot
```bash
# 1. deltat engine on :5434 (password "secret" so the frontend connects)
cd ~/repositories/deltat
DELTAT_PORT=5434 DELTAT_PASSWORD=secret DELTAT_DATA_DIR=/tmp/deltat_frontend_data ./target/release/deltat &
# (rebuild first if needed: cargo build --release --bin deltat)

# 2. seed all demo data (idempotent)
cd ~/repositories/tap/demo
DELTAT_PORT=5434 DELTAT_PASSWORD=secret bun scripts/seed-all.ts

# 3. Next.js demos on :3000
DELTAT_PORT=5434 DELTAT_PASSWORD=secret PORT=3000 bun run server.ts &
# open http://localhost:3000  (redirects to /demos/airline)
```
Note: `/tmp/deltat_frontend_data` may be wiped on reboot (it's in /tmp) — if so, deltat starts empty and the per-page seeds (or seed-all) repopulate. The seed password is `secret`; the `smoke-two-schedules.ts` script hardcodes the SDK default `deltat` and will fail auth against this server (harness quirk, not a real bug — frontend is unaffected).

## DONE this session (verified)

### deltat engine (correctness + tests)
- **GAP-12 fixed** — `availability()` read path now scans a buffer-expanded window so it agrees with `check_no_conflict` (a slot in a booking's buffer tail no longer reads "free" yet rejects). Regression: `buffer_straddling_query_start_blocks_availability`.
- **GAP-13 fixed** — `compute_multi_availability` now `merge_overlapping`s before the `min_duration` filter, so continuous coverage handed off between resources at a shared boundary isn't fragmented and dropped. Regression: `multi_avail_merges_adjacent_coverage_before_min_duration`; the bug-codifying `multi_avail_exact_boundary_touch` was corrected.
- **Executable spec added** — `src/engine/availability.rs` mod `spec` (property test vs independent brute-force reference) + `src/engine/verify.rs` (read-path == write-path, and write-path vs brute force). Mutation-tested (a real boundary blind spot in the generator was found + fixed).
- **`substitute_params` (wire.rs) hardened** — single-pass UTF-8-safe; killed the global-replace placeholder-clobber bug.
- **Coverage measured**: 82.76% region / 83.87% line (`cargo llvm-cov --lib`). Core (availability/conflict/model/queries/store/mutations/wal) ~93–100%; the gap is the transitional pgwire transport (wire 51%, tls 0%) slated for replacement.
- **Spec updated** (`docs/REQUIREMENTS.md`): added PRIN-12 (test-first/red-green), TEST-16 (coverage measured, not gated), TEST-17 (every fix ships its test); GAP-12/GAP-13 RESOLVED; GAP-AUDIT note (adversarial pass confirmed INV-03 batch atomicity + INV-05 byte-identical replay hold by construction, and multi-tenant isolation is sound).
- State: `cargo test --lib` ~310 green, `clippy --all-targets -D warnings` clean, ambient-time grep gate clean.

### tap demos
- **Phase 0 bug fixes**: calendar week/day grids use full **containment** not overlap (the "13:00/16:30 can't book back-to-back" bug); restaurant floor plan refreshes on every WS event (the "doesn't update" bug); seat hold socket reverts the optimistic green on a `{type:"error"}` rejection (phantom-green bug).
- **Seat hold→book fixed** — `demo/lib/booking-flow.ts` `releaseHoldsThenBook` releases holds (awaited) BEFORE the atomic booking; the personal-calendar mirror is now a **separate best-effort** booking so an occupied calendar can't roll back the seat booking. Guard test: `demo/scripts/smoke-seat-hold-booking.ts`.
- **Phase 1 redesign (Stage-on-void)**: `demo/components/stage.tsx` (dark-void single pane + primitive header + floating tray), `demo/components/booking-confirmed-modal.tsx` (shared success modal showing the verbatim deltat row). Removed the right `PersonalCalendarSidebar`; trimmed nav (dropped Scheduling + Bookings). **Airline / Theater / Stadium** converted to single-pane Stage (one shared `SeatBookingPage`), no date/slot pre-step, slots as header pills, success → modal.
- **Audit fixes**: SDK `availability.getCombined` parameterized; typed catches; modal optional-chain; onWsEvent mid-booking race guard. Demo typecheck clean.

## NEXT — the demo redesign (tasks #20, #21). Decisions LOCKED:
- **Visual**: Stage-on-void hybrid — single luminous pane on dark canvas, thin top demo-switcher strip, per-demo "primitive" header, shared success modal. **dark-first** theme.
- **Catalog**: keep all booking demos, **reframe & trim** — recast Calendar + Availability as pure bookers (delete their admin/owner panels), **remove Scheduling** (its multi-resource trick is shown by Meet).
- **Model**: **fungible = one capacity-N resource** (stadium GA tiers, hotel room-types, restaurant bar, parking zones); **assigned = capacity-1 resource per seat** (plane/theater/cinema/premium). "Block a table/seat/spot tonight" = a pre-seeded **blocking rule**.
- **Stadium** = ~90 oval **capacity-pool sections** summing to ~80,000 (NOT 80k rows) + zoom/pan + a few assigned premium sections. **Airline** ~300 assigned seats. **Hotel** = TWO-PANE (manage + book) — confirmed override of single-pane for hotel only. **Calendar** = polished custom week-grid single-resource booker (no new dep). **Restaurant/Parking** = hardcoded grids (no library).

### Build order
1. Realistic seeds + scale (airline ~300, stadium ~80k capacity, parking garage, restaurant floor) + reseed + dynamic-blocking examples.
2. Stadium oval+zoom, Hotel two-pane, Restaurant/Parking grids → Stage + modal.
3. Calendar week-grid booker + Availability booker-only (delete admin/owner panels).
4. Meet three-lane intersection (Alice / Bob / Both-free on one ruler) + Cinema route (`/demos/cinema`, wire `seed-cinema`) + global dark theme + delete dead components + seat-color dark polish.

## Open backlog (deltat)
- #7 commit the clock-seam + Dockerfile fix; #8 engine silently drops multi-row INSERT rows (rules; bookings OK); #9 remove dead kernel-Schedule path; #10 hold-expiry on a monotonic clock (HW-01/GAP-11); #13 curated keeper tests for INV-03 + INV-05; #15 atomic CommitHold (AVAIL-07) — the race-free hold→booking; #16 WS protocol shared-contract + god-component split + centralize time constants.
- Audit deferrals (local-demo, non-blocking): unauthenticated `/ws` + WS input validation; deltat negative `min_available` / inverted-window guards; demo-default passwords.

**Authority when docs conflict: `docs/REQUIREMENTS.md`.**
