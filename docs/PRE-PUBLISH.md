# deltat + tap — Pre-Publish Plan, Lessons, and Go/No-Go

> What to do before going fully public, distilled from landmark software projects and successful
> open-source repos, tailored to deltat (already PUBLIC, AGPL-3.0) and tap (PRIVATE, MIT).
>
> Note: the multi-agent research pass for this was blocked by a sustained API overload; this is
> synthesized directly and can be re-verified with a web-research pass later if desired.

## Status going in (verified)

- deltat: 342 lib + 19 integration tests pass (no skips), clippy clean, release build OK; graded all
  A with three S (Correctness, Security, Test-rigor). Already public.
- tap: SDK 18 + calendar 7 tests pass, both apps typecheck clean; graded all A. Currently private.
- git history of both repos is clean: no secrets, keys, or data files ever committed.

## Lessons worth internalizing (and how they apply here)

1. **Correctness culture is the moat (SQLite, TigerBeetle, FoundationDB).** SQLite's reputation is
   built on extreme testing (branch-complete coverage, billions of cases), not features. TigerBeetle
   and FoundationDB made *deterministic simulation testing* both their correctness method and their
   marketing story. deltat already has the seam for this: the injected clock (ENG-05) makes the engine
   deterministically simulatable, and the spec names it (TEST-03). **This is deltat's single highest-
   leverage future investment** and a credibility story no competitor in "booking software" has.

2. **The format outlives the binary (SQLite file format, Linux "do not break userspace", the C ABI,
   TCP/IP).** Things that lasted decades did so by freezing a simple, versioned interface and never
   breaking it. deltat's own thesis ("the FORMAT/spec is the 100-year artifact", SCALE-07/08) is
   exactly right. Concretely: before broad adoption, freeze the wire/WAL format with magic + version
   byte + a cross-language conformance corpus (SCALE-08). Today the WAL has no magic/version (ENG-11);
   that is fine pre-1.0 but must be settled before people depend on it.

3. **Do one thing well (Unix philosophy).** deltat's kernel-purity rule (MODEL-07: a field enters the
   kernel only if single-resource availability is impossible without it) is the discipline that keeps
   it small and durable. Keep resisting scope creep; the restraint is the product.

4. **A great README and honest distribution win adoption (ripgrep, fd).** ripgrep's rise was a sharp
   README (a 30-second "why", honest benchmarks, copy-paste quickstart) plus being installable
   everywhere (crates.io + every package manager + prebuilt binaries). deltat's README has the idea;
   it needs badges, a crates.io install, and benchmarks reported honestly (ns/query + cache-misses,
   per HW-16, never an unqualified "sub-millisecond" claim, per VIS-06/SCALE-04).

5. **Supply-chain and maintainer trust (the xz/liblzma backdoor, event-stream, left-pad).** A public
   repo is a target. The cheap, high-value defenses: GitHub secret scanning + push protection,
   committed lockfiles (deltat has Cargo.lock, tap has bun.lock, good), minimal trusted dependencies,
   and signed/tagged releases. Do not hand out commit access loosely.

6. **License is a strategic, hard-to-reverse choice.** SQLite chose public domain for maximal
   adoption. AGPL (deltat today) is strong copyleft: great for keeping a network-served protocol open,
   but it deters many companies from building on it, which is in tension with the "universal,
   widely-adopted protocol" vision (VIS-01/02). MIT/Apache-2.0 maximizes adoption (Apache adds a
   patent grant). Decide deliberately; see §Decisions.

## Pre-publish checklist

### deltat (already public; these are merge / release-time)
- [ ] Merge `fix/oss-hardening` -> `main` (the security/hardening pass).
- [ ] Add `SECURITY.md` with a private vulnerability-disclosure path (important for a public database).
- [ ] Add `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md`.
- [ ] Add a `CHANGELOG.md` and cut a tagged `v0.1.0` GitHub release.
- [ ] Enable on GitHub: secret scanning + push protection, Dependabot, branch protection on `main`
      (require the CI to pass).
- [ ] CI: the release-profile test step is added; consider adding `cargo llvm-cov` (report-only,
      TEST-16) and a periodic `cargo-mutants` job (TEST-04). The honest mutation baseline today is
      103 mutants, 82 caught, 5 equivalent/defensive survivors on conflict.rs + queries.rs.
- [ ] README: add CI / crates.io / license badges.
- [ ] (If publishing the crate) confirm the `deltat` name on crates.io; the package metadata is in
      place. Decide whether to publish now or after the format freeze.

### tap (private; these GATE making it public)
- [ ] **Finish the calendar B1**: harden `calendar/server.ts` (the live `/book/[slug]` WebSocket path)
      the way `demo/server.ts` already is (Zod validation, no `any`/`!`, release-before-book), and
      reconcile the dead `createPublicBooking`. This is your active WIP and the main public-safety gate.
- [ ] **Remove the dead `Schedules` SDK surface** once your `setup.ts` migration off `dt.schedules`
      lands (the stale event variants are already gone).
- [ ] Decide whether to make the WHOLE repo public or publish only the SDK + demo and keep the
      calendar app private until it is product-ready (see §Decisions).
- [ ] Merge `fix/oss-hardening` -> `main` (after the above).
- [ ] (SDK) Publish `@open-tap/client` to npm (with provenance); it is publish-ready now (metadata +
      README + LICENSE shipped, verified via `npm pack`).
- [ ] Same GitHub hygiene as deltat (secret scanning, Dependabot, branch protection).
- [ ] Final pre-flip check: re-confirm no secrets in history immediately before flipping to public
      (history is forever once public; it is clean today).

## Decisions (genuinely yours to make)

1. **License strategy.** Keep AGPL (deltat) + MIT (tap)? Or move deltat to MIT/Apache-2.0 for
   adoption (at the cost of copyleft protection)? Or, given you said "public domain," CC0 like SQLite?
   This shapes who will build on the protocol and is hard to reverse once others contribute.
2. **deltat: merge `fix/oss-hardening` -> main now?** It is green and graded S/A. I can open the PR;
   you click merge (or I merge if you say go).
3. **tap public timing.** Recommended: hold the full repo private until the calendar B1 + Schedules
   work is done; optionally publish `@open-tap/client` to npm now (it is ready and low-risk).
4. **Publishing packages (crates.io / npm) now vs after a format freeze + 1.0.** Publishing signals
   stability; doing it before the format is frozen invites churn for early adopters.
