# deltat + tap — Quality-Improvement Review

> Forward-looking craftsmanship review: concrete refinements to take already-strong, well-tested code
> to exceptional, built-to-last quality. Every item respects the project principles (KISS, DRY without
> premature abstraction, no over-engineering). Synthesized from a full read of the `fix/oss-hardening`
> worktrees (deltat @ 85539633, tap @ 33bc6d5). This is not a bug list; it is a polish list.
>
> Honest framing: the code is already good. These are net-positive refinements, ordered by leverage.

## High-leverage (do soon)

1. **Audit the kernel's `unwrap()/expect()` (PRIN-08).** There are ~141 `unwrap/expect` in non-test
   `src/`. Most are in the transitional pgwire layer (`wire.rs`/`sql.rs`) and are pgwire-shaped, but
   any in `src/engine/**` are the ones that matter. Action: grep `src/engine` specifically; for each,
   either prove it unreachable and add a one-line `// safe: <why>` or convert it to errors-as-values.
   This session already closed the live ones (delete_resource, compaction, gc); this is the
   finish-the-sweep pass that makes "no panics in the kernel" auditable, not just believed.

2. **Collapse the 13 demo route shims into one registry-driven dynamic route (tap).** `demo/app/demos/`
   has 13 byte-identical `page.tsx` files differing by one id literal; `app/embed/[example]/page.tsx`
   already proves the collapse pattern. One `app/demos/[example]/page.tsx` + an id->component map +
   `generateStaticParams = enabledExampleIds()` removes ~100 lines and a per-example edit tax. This is
   pure simplification (fewer files, one source of truth), not added abstraction. Verify with a
   `next build` since it touches routing.

3. **Split `engine/tests.rs` (~4900 lines) into focused modules.** One giant file hides intent. Split
   by concern (`availability_tests.rs`, `conflict_tests.rs`, `mutation_tests.rs`, `hardening_tests.rs`,
   `concurrency_tests.rs`) behind a `mod tests`. Zero behavior change, large navigability win for a
   public repo where tests are also documentation. Keep all coverage; only reorganize.

## Polish (cheap, clear wins)

4. **Verify the 11 numeric `as` casts are each provably-safe (deltat).** A handful (`as usize`,
   `as i32` in the multi-availability sweep). Each is reachable from parsed input; confirm bounds and
   add a `// bounded by <guard>` note, or switch to `try_into()` with a clean error where the bound is
   not obvious. Small, removes a class of silent-truncation doubt for reviewers.

5. **Apply the crypto/credentials extraction pattern once more (tap).** The `crypto.ts` /
   `credentials.ts` / `config.ts` split made security logic dependency-free and unit-testable. The
   session sign/verify in `calendar/lib/auth.ts` is still entangled with `next/headers`; extracting
   the pure sign/verify (token <-> payload) into a deps-free module would let it be unit-tested too
   (round-trip + tamper cases). Do it only because it unlocks tests, not for its own sake.

6. **Funnel the demo's two metadata writes through one helper (tap).** `demo/lib/store.ts` plus the
   resource-create action write resource meta in two places (PRIN-06, duplicated state). One
   `createResourceWithMeta` helper makes the pairing atomic-in-intent. Cosmetic blast radius, but it
   is the kind of single-source-of-truth tidy that ages well.

7. **README badges + a 30-second "why" (deltat).** The README is already strong on the idea. For a
   public repo, add CI / crates.io / license badges and keep the opening "scheduling is 1-D collision
   detection" hook tight. First impressions drive adoption (see PRE-PUBLISH.md).

## Architectural (bigger, deliberate, your call)

8. **Finish the `Span::new` -> `Span::try_new` migration (TIME-05).** The two-constructor split
   (panicking `new` for engine-internal spans, fallible `try_new` for untrusted input) is a real
   footgun: a reviewer cannot tell at a glance which call can panic. Migrating all construction to
   fallible removes the panicking constructor from the kernel entirely and makes "no kernel panics"
   structural. This is a deliberate, spec-tracked change touching many call sites, NOT urgent, and the
   internal asserts are defensible today, so do it as one focused PR when you choose, with tests.

9. **Do NOT refactor `sql.rs` / `wire.rs` for their own sake.** They are large (1411 + 1272) but are
   the transitional transport (PROTO-02), slated for replacement by the framed protocol (PROTO-01/03).
   The right investment is building PROTO-03 against the already-extracted `Command` (command.rs), not
   polishing code that will be deleted. Resisting this refactor is the KISS-correct choice.

10. **Converge on one vocabulary at the v2 boundary (VOCAB-02 / PROTO-09).** The engine uses lifecycle
    verbs (create/add/place/confirm) while SQL uses CRUD and the SDK uses its own names. Aligning them
    is a v2-protocol concern, not a churn-now item; flag it so the framed protocol lands with one name
    per concept rather than inheriting three.

## Deliberately rejected (would violate KISS / no-over-engineering)

- Introducing a trait/generic abstraction over the SDK resource/rule/booking/hold modules: they are
  short and similar, and three similar files beat a premature abstraction. Leave them concrete.
- A config-knob framework or plugin system for the engine: nothing today needs it (NOT-06).
- Splitting `model.rs` into many files: it is cohesive at ~518 lines; one types module is correct.
