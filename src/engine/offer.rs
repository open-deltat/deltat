//! Counter-offers: the spans a refused caller could take instead.
//!
//! A refusal that says only "no" costs an agent a whole round trip to find out what it should have
//! asked for, and on a live phone call that round trip is silence. Every refusal here happens at a
//! point where the state needed to answer "then when?" is already in hand, so the answer rides back
//! with the refusal.
//!
//! Nothing in this module reserves anything. An offered span is free at `as_of` and is still free
//! for anyone else; taking one means placing a hold like any other caller. That is deliberate:
//! handing out a commitment nobody asked for is worse than the refusal it replaces.

use crate::limits::MAX_VALID_TIMESTAMP_MS;
use crate::model::{Ms, ResourceState, Span};

use super::availability::availability;
use super::EngineError;

/// Rule context already collected at a refusal point.
///
/// Borrowed rather than gathered here, and that is the whole safety argument. Collecting inherited
/// rules walks ancestors and takes their locks; doing that while holding a descendant's write guard
/// is the ABBA half of the documented C1 deadlock with `batch_confirm_bookings`. The caller
/// collects before it takes its guard, exactly as it already does for admission, and lends the
/// result here.
pub(crate) struct RuleCtx<'a> {
    pub inherited_non_blocking: &'a [Span],
    pub inherited_blocking: &'a [Span],
    pub ancestor_has_schedule: bool,
}

/// Spans the caller could take instead of the one that was refused.
///
/// There is no `reserved` field because nothing is ever reserved. The wire payload states that
/// explicitly for readers who cannot see this type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterOffer {
    /// The span that was refused, echoed so a caller can match an offer to its request.
    pub requested: Span,
    /// The engine clock the sweep ran against. An offer is a statement about this instant.
    pub as_of: Ms,
    pub alternatives: Vec<Span>,
    /// The resource publishes no opening hours, so there are no windows to enumerate.
    ///
    /// Worth its own flag: such a resource still accepts bookings (it is a pure collision
    /// detector), so an empty `alternatives` here means "nothing to enumerate", never "nothing is
    /// free". Collapsing the two would have the caller tell a customer the calendar is full.
    pub unscheduled: bool,
}

impl CounterOffer {
    /// Whether there is anything worth putting on the wire. A scheduled resource with nothing free
    /// says nothing at all, rather than sending an empty list that reads like a claim.
    pub fn has_content(&self) -> bool {
        !self.alternatives.is_empty() || self.unscheduled
    }
}

/// An engine refusal, plus whatever could be offered instead.
///
/// Lossy into `EngineError` on purpose: every existing caller keeps the error it has today and the
/// offer is additive.
#[derive(Debug)]
pub struct Refused {
    pub error: EngineError,
    pub offer: Option<CounterOffer>,
}

impl From<Refused> for EngineError {
    fn from(r: Refused) -> Self {
        r.error
    }
}

impl From<EngineError> for Refused {
    fn from(error: EngineError) -> Self {
        Refused { error, offer: None }
    }
}

/// Rank the spans a refused caller could take instead.
///
/// Pure, synchronous and infallible by construction. It takes `&ResourceState`, which is precisely
/// what a write guard derefs to, so calling it at a refusal point acquires no lock and introduces
/// no await point. That is the property that makes it safe under a guard, and
/// `offer_rank_is_a_pure_function_of_resource_state` in the engine tests is the lock on it: a
/// future refactor that makes this async or lock-taking fails to compile.
///
/// A candidate must fit `requested.duration_ms() + buffer_after`, because a booking's effective
/// footprint includes its turnaround tail. That is the same antecedent `verify.rs` already proves
/// the write path admits, so an offer being acceptable is a corollary of an existing property test
/// rather than a fresh claim.
pub(crate) fn rank(
    rs: &ResourceState,
    requested: &Span,
    ctx: &RuleCtx<'_>,
    now: Ms,
    window_end: Ms,
    limit: usize,
) -> CounterOffer {
    let empty = |unscheduled| CounterOffer {
        requested: *requested,
        as_of: now,
        alternatives: Vec::new(),
        unscheduled,
    };

    if limit == 0 {
        return empty(false);
    }

    // No schedule anywhere in the chain means the sweep has no base to subtract from and would
    // enumerate nothing, while the write path happily admits any non-colliding span. Report the
    // distinction rather than an empty list that reads as "full".
    if !rs.has_non_blocking_rule() && !ctx.ancestor_has_schedule {
        return empty(true);
    }

    // try_new, never new: `requested` arrives from the wire and a past-dated or inverted span must
    // not panic the connection task on the failure path of all places.
    let Ok(window) = Span::try_new(requested.start, window_end.min(MAX_VALID_TIMESTAMP_MS)) else {
        return empty(false);
    };

    let needed = requested
        .duration_ms()
        .saturating_add(rs.buffer_after.unwrap_or(0));

    let free = availability(
        rs,
        &window,
        ctx.inherited_non_blocking,
        ctx.inherited_blocking,
        now,
    );

    // One offer per free run, not several stacked inside one. Three alternatives spread across the
    // week is a useful answer; three consecutive half-hours in the same morning is one answer
    // repeated, and a caller whose customer cannot do that morning is back where it started.
    let alternatives = free
        .iter()
        .filter(|run| run.duration_ms() >= needed)
        .filter_map(|run| Span::try_new(run.start, run.start.saturating_add(requested.duration_ms())).ok())
        .take(limit)
        .collect();

    CounterOffer {
        requested: *requested,
        as_of: now,
        alternatives,
        unscheduled: false,
    }
}
