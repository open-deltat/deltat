# src/engine: the kernel

Read `../../CLAUDE.md` first. This file adds the rules that apply only inside the kernel.

## PROTO-AUTH-00, the one invariant of this directory

**The kernel takes `(Command, idempotency Ulid, integer now)` and nothing else.**

No secret, token, identity, price, ACL principal, or PII crosses into `src/engine`. Auth is a gate
*above* `Command` dispatch. Payment is a side-channel keyed by the kernel idempotency `Ulid`.

This is why the entire auth, capability, payment and federation story needs **zero** change in here,
and it is the reason the kernel can later be carved into its own crate. If a change to this directory
needs to know *who* is asking or *what it costs*, the change belongs at the edge instead. Say so and
move it rather than threading a parameter through.

Related anti-requirements: **NOT-01** keeps timezones out of the kernel (they enter at the MCP layer
and nowhere else). **NOT-02** keeps the kernel pure to time.

## Time

Never call `SystemTime::now()`. Every wall-clock read goes through the injected `Clock`
(`../clock.rs`), reached as `self.now_ms()` or `engine.now_ms()`. This is enforced two ways
(`scripts/check-no-ambient-time.sh` and a `clippy.toml` `disallowed-methods` rule) and it is what
makes a run reproducible from a seed.

Expiry and elapsed-time comparisons are a separate, still-open problem: today they compare against
`CLOCK_REALTIME`, which can step backward. Before changing anything in that area read FED-AUTH-09 in
`../../docs/AUTH-AND-PAYMENTS.md`, which has the worked design. Do not invent a monotonic-delta
scheme; `CLOCK_MONOTONIC` does not survive a restart and this is a replay system.

## The WAL is append-only and versioned

`../wal.rs:28-39` classifies every change:

- **Safe**: adding a new `Event` *variant* at the end. bincode indexes variants in declaration order.
- **Breaking**: adding, removing or reordering a field inside an existing variant, or changing what a
  field's value means. Records before and after are indistinguishable by length and CRC, so nothing
  but `FORMAT_VERSION` catches the difference.

A breaking change without a version bump is data-destroying, not merely wrong: mid-log the node will
not start, and at the tail `Wal::open` truncates and fsyncs, destroying an acknowledged booking.

## Locks

Lock ordering already exists and is ABBA-safe (sorted batch locks in `store.rs`). Never hold a
resource write guard while awaiting another resource's lock; collect what you need from ancestors
*before* taking the guard, which is what `collect_inherited_rules` is for.

Note the open issue here: `persist_and_apply` awaits the WAL fsync while holding the write guard, so
reads serialise behind commits. See issue #25 before optimising anything on the read path, because
the fix changes the shape of this function.

## Failure direction

When a change has a failure mode, choose the direction that shows **less** availability. A reader
briefly seeing an allocation that a later crash loses is safe. A reader seeing a held slot as free is
INV-01 violated, and INV-01 is the product.
