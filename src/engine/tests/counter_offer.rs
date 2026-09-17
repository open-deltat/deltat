//! Counter-offers: what a refusal says instead of just "no".
//!
//! The contract under test is narrow and total: every span the engine offers must be a span the
//! write path would actually accept. An offer that is itself refused is worse than no offer, because
//! the caller burns a round trip and a customer hears two different wrong answers.

use crate::clock::{now_ms, TestClock};
use crate::engine::*;
use crate::limits::COUNTER_OFFER_MAX;

use super::helpers::*;

/// The load-bearing test. Force a refusal, then feed every offered span straight back into the
/// write path against the same state and require acceptance.
///
/// Written before `offer.rs` existed. Its red failure was:
///   `place_hold_offering returned Refused { offer: None } for a refused span on a resource with
///    free runs in the next 7 days`
#[tokio::test]
async fn every_offered_span_is_one_the_write_path_accepts() {
    let path = test_wal_path("offer_accepted.wal");
    let notify = Arc::new(NotifyHub::new());
    let clock = Arc::new(TestClock::new(0));
    let engine = Engine::with_clock(path, notify, clock).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    // Open 09:00-17:00 on each of three consecutive days.
    for day in 0..3 {
        let base = day * 24 * H;
        engine
            .add_rule(Ulid::new(), rid, Span::new(base + 9 * H, base + 17 * H), false)
            .await
            .unwrap();
    }
    // Fill the whole of day 0 so a request there must be refused.
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(9 * H, 17 * H), None)
        .await
        .unwrap();

    let wanted = Span::new(10 * H, 11 * H);
    let refused = engine
        .place_hold_offering(Ulid::new(), rid, wanted, 60_000, COUNTER_OFFER_MAX)
        .await
        .expect_err("day 0 is fully booked, so this must be refused");

    let offer = refused.offer.expect("a refusal on a scheduled resource with free days must offer");
    assert!(
        !offer.alternatives.is_empty(),
        "days 1 and 2 are wide open, so there was something to offer"
    );
    assert!(!offer.unscheduled);

    // The contract: each offered span is bookable. Booked one at a time against the live engine,
    // which is the strongest form of the claim: later offers survive earlier ones being taken.
    for (i, alt) in offer.alternatives.iter().enumerate() {
        engine
            .confirm_booking(Ulid::new(), rid, *alt, None)
            .await
            .unwrap_or_else(|e| {
                panic!("offered alternative {i} [{}, {}) was refused by the write path: {e}",
                    alt.start, alt.end)
            });
    }
}

/// The offer is computed under the guard that refused, at the same `now`, so it can never hand back
/// the span it just refused. Without that, a hold expiring between refusal and sweep would produce
/// "2pm is taken, I can do 2pm", which destroys trust in the whole feature.
#[tokio::test]
async fn an_offer_never_contains_the_span_that_was_refused() {
    let path = test_wal_path("offer_not_requested.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let base = now_ms();
    engine
        .add_rule(Ulid::new(), rid, Span::new(base, base + 48 * H), false)
        .await
        .unwrap();
    let wanted = Span::new(base + H, base + 2 * H);
    engine.confirm_booking(Ulid::new(), rid, wanted, None).await.unwrap();

    let refused = engine
        .place_hold_offering(Ulid::new(), rid, wanted, base + 60_000, COUNTER_OFFER_MAX)
        .await
        .expect_err("the span is taken");
    let offer = refused.offer.expect("should offer");

    assert!(
        !offer.alternatives.contains(&wanted),
        "the refused span came back as its own alternative"
    );
    assert_eq!(offer.requested, wanted, "the offer echoes what was asked for");
}

/// A resource with no rules anywhere in its chain accepts anything that does not collide, so the
/// sweep has no windows to enumerate. Reporting an empty list would read as "the calendar is full",
/// which is the opposite of the truth.
#[tokio::test]
async fn an_unscheduled_resource_reports_unscheduled_rather_than_empty() {
    let path = test_wal_path("offer_unscheduled.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let base = now_ms();
    let wanted = Span::new(base + H, base + 2 * H);
    engine.confirm_booking(Ulid::new(), rid, wanted, None).await.unwrap();

    let refused = engine
        .place_hold_offering(Ulid::new(), rid, wanted, base + 60_000, COUNTER_OFFER_MAX)
        .await
        .expect_err("the span is taken");
    let offer = refused.offer.expect("should offer");

    assert!(offer.unscheduled, "no rules anywhere means unscheduled, not full");
    assert!(offer.alternatives.is_empty());
}

/// A run exactly as long as the request is NOT offerable on a buffered resource: the booking's
/// effective footprint is duration + buffer_after, so offering it would hand back a span the write
/// path then refuses for turnaround.
#[tokio::test]
async fn an_offer_respects_buffer_after() {
    let path = test_wal_path("offer_buffer.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    // 15 minutes of turnaround after every booking.
    engine.create_resource(rid, None, None, 1, Some(15 * M)).await.unwrap();
    let base = now_ms();
    engine
        .add_rule(Ulid::new(), rid, Span::new(base, base + 24 * H), false)
        .await
        .unwrap();

    // Leave exactly one free hour between two bookings. A 1-hour request needs 1h15m of room, so
    // that gap must not be offered.
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(base + H, base + 2 * H), None)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(base + 3 * H + 15 * M, base + 4 * H), None)
        .await
        .unwrap();

    let wanted = Span::new(base + H, base + 2 * H);
    let refused = engine
        .place_hold_offering(Ulid::new(), rid, wanted, base + 60_000, COUNTER_OFFER_MAX)
        .await
        .expect_err("the span is taken");
    let offer = refused.offer.expect("should offer");

    let exact_gap = Span::new(base + 2 * H + 15 * M, base + 3 * H + 15 * M);
    assert!(
        !offer.alternatives.contains(&exact_gap),
        "a gap with no room for the turnaround tail was offered: {:?}",
        offer.alternatives
    );
    // Whatever it did offer must still be bookable, which is the general contract.
    for alt in &offer.alternatives {
        engine.confirm_booking(Ulid::new(), rid, *alt, None).await.unwrap();
    }
}

/// `offer_limit: 0` is the kill switch, and it must produce byte-identical behaviour to a kernel
/// without this feature. The delegating `place_hold` passes 0, so this also proves the delegation.
#[tokio::test]
async fn the_kill_switch_produces_no_offer_at_all() {
    let path = test_wal_path("offer_killswitch.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let base = now_ms();
    engine
        .add_rule(Ulid::new(), rid, Span::new(base, base + 48 * H), false)
        .await
        .unwrap();
    let wanted = Span::new(base + H, base + 2 * H);
    engine.confirm_booking(Ulid::new(), rid, wanted, None).await.unwrap();

    let refused = engine
        .place_hold_offering(Ulid::new(), rid, wanted, base + 60_000, 0)
        .await
        .expect_err("the span is taken");
    assert!(refused.offer.is_none(), "offer_limit 0 must offer nothing");

    // And the plain method, which delegates with 0, still returns the same error it always did.
    let plain = engine
        .place_hold(Ulid::new(), rid, wanted, base + 60_000)
        .await;
    assert!(matches!(plain, Err(EngineError::Conflict(_))));
}

/// A refusal that is not about the span (a bad id, a limit) carries nothing. Offering alternatives
/// for "that id is already in use" would be noise at best and misleading at worst.
#[tokio::test]
async fn a_refusal_that_is_not_about_the_span_carries_no_offer() {
    let path = test_wal_path("offer_not_offerable.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let base = now_ms();
    engine
        .add_rule(Ulid::new(), rid, Span::new(base, base + 48 * H), false)
        .await
        .unwrap();

    let reused = Ulid::new();
    engine
        .place_hold(reused, rid, Span::new(base + H, base + 2 * H), base + 60_000)
        .await
        .unwrap();

    let refused = engine
        .place_hold_offering(
            reused,
            rid,
            Span::new(base + 5 * H, base + 6 * H),
            base + 60_000,
            COUNTER_OFFER_MAX,
        )
        .await
        .expect_err("the id is already in use");
    assert!(matches!(refused.error, EngineError::AlreadyExists(_)));
    assert!(refused.offer.is_none(), "an id clash is not a question about time");
}

/// Widening the rule-collection window to the offer horizon must not change who gets admitted.
/// `check_rules_admit` intersects the open windows with the candidate span, so extra coverage
/// outside the candidate is inert; this pins that, because the alternative is a feature flag
/// silently altering admission.
#[tokio::test]
async fn widening_the_rule_window_does_not_change_admission() {
    let base = now_ms();
    let wanted = Span::new(base + 10 * H, base + 11 * H);

    for (label, limit) in [("narrow", 0usize), ("wide", COUNTER_OFFER_MAX)] {
        let path = test_wal_path(&format!("offer_admission_{label}.wal"));
        let notify = Arc::new(NotifyHub::new());
        let engine = Engine::new(path, notify).unwrap();

        let parent = Ulid::new();
        engine.create_resource(parent, None, None, 1, None).await.unwrap();
        engine
            .add_rule(Ulid::new(), parent, Span::new(base + 9 * H, base + 12 * H), false)
            .await
            .unwrap();
        let child = Ulid::new();
        engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

        // Inside the parent's window: admitted either way.
        let ok = engine
            .confirm_booking_offering(Ulid::new(), child, wanted, None, limit)
            .await;
        assert!(ok.is_ok(), "{label}: a span inside the parent window must be admitted");

        // Outside it: refused either way, and the wider window must not have opened it.
        let outside = Span::new(base + 20 * H, base + 21 * H);
        let refused = engine
            .confirm_booking_offering(Ulid::new(), child, outside, None, limit)
            .await;
        assert!(
            matches!(refused, Err(ref r) if matches!(r.error, EngineError::ClosedBySchedule { .. })),
            "{label}: a span outside the parent window must stay closed, got {refused:?}"
        );
    }
}

/// A past-dated or otherwise degenerate request reaches this path from a plain
/// `INSERT INTO bookings`. The offer code must not panic on the failure path of all places.
#[tokio::test]
async fn an_offer_on_a_past_dated_request_does_not_panic() {
    let path = test_wal_path("offer_past_dated.wal");
    let notify = Arc::new(NotifyHub::new());
    let clock = Arc::new(TestClock::new(1_000_000_000_000));
    let engine = Engine::with_clock(path, notify, clock).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    // A window straddling the clock, so the request below sits inside the schedule but behind now.
    engine
        .add_rule(
            Ulid::new(),
            rid,
            Span::new(1_000_000_000_000 - 10 * 24 * H, 1_000_000_000_000 + 10 * 24 * H),
            false,
        )
        .await
        .unwrap();

    // Well in the past relative to the clock.
    let past = Span::new(1_000_000_000_000 - 5 * 24 * H, 1_000_000_000_000 - 5 * 24 * H + H);
    engine.confirm_booking(Ulid::new(), rid, past, None).await.unwrap();
    let refused = engine
        .place_hold_offering(Ulid::new(), rid, past, 1_000_000_060_000, COUNTER_OFFER_MAX)
        .await
        .expect_err("the span is taken");
    // No assertion on content; the point is that it returned rather than panicked.
    let _ = refused.offer;
}

/// `offer::rank` must stay a pure, synchronous function of resource state. A refactor that made it
/// async or lock-taking would reintroduce the C1 deadlock hazard and would still pass a wall-clock
/// benchmark on an idle box, so the type system is the only reliable guard.
#[test]
fn offer_rank_is_a_pure_function_of_resource_state() {
    const _: fn(&ResourceState, &Span, &crate::engine::offer::RuleCtx<'_>, Ms, Ms, usize) -> CounterOffer =
        crate::engine::offer::rank;
}
