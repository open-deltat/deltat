# Maintenance: how this project stays true over time

> Companion to [`REQUIREMENTS.md`](REQUIREMENTS.md). That doc says what the project must be. This one
> says how it stays that way once more than one person, and more than one agent, is working on it.

## The thesis

**A principle survives only as far down this ladder as you push it.**

| Rung | Form | Decay rate |
|---|---|---|
| 1 | Stated in a design doc | Decays silently. Nobody re-reads 2,800 lines. |
| 2 | Stated in `CLAUDE.md` | Read every session, but an agent under pressure still drifts. |
| 3 | Enforced by a machine | Cannot drift. Fails loudly, at the moment of the mistake. |

This repo already proved rung 3 works. The clock seam (all wall-clock reads go through
`src/clock.rs`) is enforced twice, by `scripts/check-no-ambient-time.sh` and by a `clippy.toml`
`disallowed-methods` rule, and it is the only principle here that has never regressed. The comment in
that script explains why it is belt-and-suspenders: a path-based lint can be fooled by how a call is
spelled, and a grep cannot. That pairing is the template.

Everything else was on rung 1. Here is what that cost:

- Both auth docs' fact-check headers still say "there is no `commit_hold` symbol". It shipped at
  `engine/mutations.rs:284`.
- `REQUIREMENTS.md`'s own header says "Fact-checked against HEAD (`feat/clock-seam`) on 2026-06-18".
  That branch is a stale pre-history-rewrite ref and the date is three months old.
- The header goes on to name four docs it knows are stale (GAP-08), and has carried that note rather
  than fixing them.
- `AUDIT-2026-08-26.md`'s one-way door 5 describes a root cause that PR #33 closed.
- Issue #24 still carries a "silently divides by 1000" framing that `FORMAT.md` §9 now formally
  contradicts.
- `FORMAT.md` evolution rule 3 is unimplementable under bincode and contradicts rule 4.
- `benches/stress.rs:298` queries a 365-day window against a 90-day cap, so phase 3 panics and phase
  4 has never run. Nobody noticed, because the bench is not in CI (TEST-11).
- The audit needed a section titled "Do not re-litigate". That section is a symptom: decisions were
  made and not captured where the next decider would see them.

None of that is carelessness. It is the expected half-life of prose.

## The three layers

**Layer 1: `CLAUDE.md` is a router, not documentation.** It is read on every session, so every line
competes for attention with the actual work. It carries the invariant, a table of which doc to read
for which kind of change, the enforced principles, the closed decisions, and the commands. It does
not carry the spec. Directory-scoped files narrow this further: `src/engine/CLAUDE.md` holds
PROTO-AUTH-00, because the moment you are editing the kernel is the moment that rule matters.

**Layer 2: `scripts/check-*.sh`, run by `scripts/check-all.sh`.** CI runs it before the test suite,
and a Claude Code `PreToolUse` hook (`.claude/settings.json`) runs it before any `git push`, so a
regression is caught while the context is still in your head rather than in a CI log you have to go
and look at.

The contract for adding one: **when you establish a new invariant, add its check in the same PR.**
Each check must print what failed *and* what to do about it, because whoever trips it is usually not
whoever wrote it.

**Layer 3: decisions live in the repo, not in a person's head or a local tool's memory.** A decision
that only exists in one machine's assistant memory is invisible to a contributor, unreviewable in a
PR, and gone if the machine is. Closed decisions belong in `CLAUDE.md` with their reasoning in a
dated doc. Personal working context (how a given person likes to work, what happened in a session)
is the only thing that belongs outside the repo.

## Introducing a check to a corpus that predates it

Most checks worth adding will fail on day one, because the corpus was written before the rule
existed. Do not fix everything first, and do not water the rule down. **Ratchet it.**

`scripts/check-requirements.sh` is the worked example. It counts three things, compares each against
`scripts/requirements-baseline.txt`, and fails only when a number goes *up*. Today those numbers are
22 duplicate declarations, 10 dangling references, and 79 `✅` requirements naming no verifying test.
The rule is live immediately; the backlog burns down when someone touches that area anyway. The one
discipline that makes this work: **never raise a baseline to make a build pass.**

## The convention this corpus still needs

Writing that check surfaced the reason the docs cannot be checked exactly: **there is no marker that
declares an ID.** The corpus declares them three ways, all legitimate:

```
- **AVAIL-07** ✅ ...        list item        (REQUIREMENTS.md, MCP.md prose)
**VIS-13** 📋 ...            line-start bold  (AUTH-ARCHITECTURE.md, AUTH-AND-PAYMENTS.md)
| **MCP-K1** | ... |         table cell       (MCP.md summary table)
```

and bold prose (`**Non-binding**`, `**Two-phase**`) is indistinguishable from an ID by shape alone.
That is why two entries in the dangling list are false positives and why MCP.md shows 22 "duplicates"
that are really one prose declaration plus one table restatement.

Pick one declaration form, convert the corpus once, and the heuristics above become exact rather than
ratcheted. Until then the check is honest about being best-effort.

## What to enforce next, ranked by what it would have caught

1. **WAL `Event` schema fingerprint.** A golden test over the encoded shape of every `Event` variant
   that fails when a field is added, removed or reordered without a `FORMAT_VERSION` bump. This is
   one-way door 5, described in the audit as the most dangerous item in the repo, and it is currently
   guarded by a comment in `wal.rs:28-39` and nothing else.
2. **No entity id or free text on any read or push surface.** One-way door 1. A test asserting the
   NOTIFY payload carries no `Ulid` and no label, plus a grep that `serde_json::to_string(&event)`
   does not reappear on the notify path. The audit confirms no existing test asserts payload content,
   so this is cheap to add and would have prevented the leak that is live today.
3. **No bearer or token type reachable from `Command`.** PROTO-AUTH-06 and MCP-S3 both require it
   structurally rather than by convention. A dependency-graph assertion over `command.rs` makes
   "token passthrough is impossible" a fact instead of a promise.
4. **Doc fact-check headers name a live ref and a recent date.** Would have caught all four stale
   headers above.
5. **The stress bench runs in CI.** Not gated on a latency threshold, just *runs*, so a bench that
   panics in phase 3 cannot go unnoticed for months (TEST-11).

## Doc hygiene

- When a doc's claim about the code goes stale, fix it in the **same PR** as the code. A spec that is
  usually right is worse than one that is known to be partial, because it is trusted.
- `REQUIREMENTS.md` is authoritative where docs conflict. It says so. Keep it that way, and when a
  companion doc disagrees, change the companion.
- Never delete a requirement ID. Mark it `WITHDRAWN`.
- A `✅` means running code verifies it (PRIN-10). Name the test in backticks on the requirement line
  so the link is machine-checkable, and so the ratchet above can fall.
