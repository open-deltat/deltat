# Contributing to deltat

Thanks for your interest. deltat is a time-allocation database built to last; contributions are
held to a high correctness and clarity bar.

## Ground rules

- **`docs/REQUIREMENTS.md` is authoritative.** It is the spec, with stable requirement IDs. When code
  and other docs disagree, REQUIREMENTS wins. Reference the relevant ID in your PR.
- **Principles (non-negotiable):** first-principles, KISS/Occam, DRY without premature abstraction,
  SOLID, small composable functions, no over-engineering, comment the *why* not the *what*.
- **Kernel purity:** the kernel never gains timezones, calendars, recurrence, display, or
  business/identity data. All of that lives at the edge. A field enters the kernel only if computing
  single-resource availability is impossible without it (MODEL-07).
- **No panics on reachable paths.** Errors are values. Validate untrusted input at the boundary; use
  checked/saturating arithmetic.

## Tests

- **Test-first (red then green).** A bug fix lands with the test that reproduces it; a feature extends
  the executable spec. A fix without a test that would have caught it is incomplete.
- Property and mutation testing matter more than a coverage percentage: a test must *fail* when the
  code is mutated. The availability contract is property-tested against an independent reference.

## Local checks (must pass before a PR)

```bash
sh scripts/check-no-ambient-time.sh          # wall-clock reads confined to src/clock.rs
cargo clippy --all-targets -- -D warnings    # must be clean
cargo test --lib                              # full unit suite
cargo test --test listen_notify               # pgwire integration tests
```

CI runs the same gates plus the suite under the release profile (to exercise overflow-checks) and a
Docker build. Mutation testing (`cargo mutants`) is run periodically.

## Pull requests

- One logical change per PR; keep the diff focused.
- Match the surrounding code: naming, comment density, idiom.
- Update `docs/REQUIREMENTS.md` status (and the test named after the ID) when behavior changes.
