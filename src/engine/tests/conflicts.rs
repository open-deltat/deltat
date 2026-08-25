use crate::engine::*;
use crate::clock::now_ms;
use super::helpers::*;

// ══════════════════════════════════════════════════════════════
// Conflict detection edge cases
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn engine_adjacent_allocations_no_conflict() {
    // [100,200) and [200,300) are adjacent, NOT overlapping, should succeed
    let path = test_wal_path("adjacent_no_conflict.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let far_future = now_ms() + H;
    engine
        .place_hold(Ulid::new(), rid, Span::new(100, 200), far_future)
        .await
        .unwrap();
    // Adjacent, should NOT conflict
    engine
        .place_hold(Ulid::new(), rid, Span::new(200, 300), far_future)
        .await
        .unwrap();
}

#[tokio::test]
async fn engine_booking_booking_conflict() {
    let path = test_wal_path("booking_booking_conflict.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();

    let result = engine
        .confirm_booking(Ulid::new(), rid, Span::new(1500, 2500), None)
        .await;
    assert!(matches!(result, Err(EngineError::Conflict(_))));
}

#[tokio::test]
async fn engine_expired_hold_allows_booking() {
    let path = test_wal_path("expired_hold_booking.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    // Place a hold that's already expired
    let past = now_ms() - 10_000;
    engine
        .place_hold(Ulid::new(), rid, Span::new(1000, 2000), past)
        .await
        .unwrap();

    // Booking should succeed because hold is expired
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn engine_hold_expires_at_exact_now() {
    // Hold expires_at == now → considered expired (expires_at <= now)
    let path = test_wal_path("hold_exact_now.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let now = now_ms();
    engine
        .place_hold(Ulid::new(), rid, Span::new(1000, 2000), now)
        .await
        .unwrap();

    // Should succeed: hold at exact `now` is expired
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();
}

// ══════════════════════════════════════════════════════════════
// Projection validation edge cases
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn engine_projection_exact_boundary() {
    // Rule at exact parent boundary edges, should pass
    let path = test_wal_path("projection_exact.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), parent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    // Exactly at parent boundaries, should succeed
    engine
        .add_rule(Ulid::new(), child, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn engine_projection_one_ms_outside() {
    // Rule extends 1ms beyond parent availability, should be rejected
    let path = test_wal_path("projection_1ms_outside.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), parent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    // 1ms before parent start
    let result = engine
        .add_rule(Ulid::new(), child, Span::new(9 * H - 1, 10 * H), false)
        .await;
    assert!(matches!(
        result,
        Err(EngineError::NotCoveredByParent { .. })
    ));
}

#[tokio::test]
async fn engine_projection_validated_against_parent_not_grandparent() {
    // Child validated against immediate parent, not grandparent
    let path = test_wal_path("projection_parent_only.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let grandparent = Ulid::new();
    engine.create_resource(grandparent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), grandparent, Span::new(0, 24 * H), false)
        .await
        .unwrap();

    let parent = Ulid::new();
    engine
        .create_resource(parent, Some(grandparent), None, 1, None)
        .await
        .unwrap();
    // Parent narrows to 9-17
    engine
        .add_rule(Ulid::new(), parent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let child = Ulid::new();
    engine
        .create_resource(child, Some(parent), None, 1, None)
        .await
        .unwrap();

    // Child tries [8,10), grandparent allows it but parent doesn't
    let result = engine
        .add_rule(Ulid::new(), child, Span::new(8 * H, 10 * H), false)
        .await;
    assert!(matches!(
        result,
        Err(EngineError::NotCoveredByParent { .. })
    ));

    // Child [10, 12) is within parent's 9-17 → OK
    engine
        .add_rule(Ulid::new(), child, Span::new(10 * H, 12 * H), false)
        .await
        .unwrap();
}

// ══════════════════════════════════════════════════════════════
// Boundary conditions
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn engine_resource_with_many_intervals() {
    // 1000 bookings, query a narrow window, binary search should handle this
    let path = test_wal_path("many_intervals.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    // One big availability rule
    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 1_000_000), false)
        .await
        .unwrap();

    // Place 100 bookings, each 1ms long, spaced 1000ms apart
    for i in 0..100 {
        let start = (i * 1000) + 100;
        engine
            .confirm_booking(Ulid::new(), rid, Span::new(start, start + 1), None)
            .await
            .unwrap();
    }

    // Query a narrow window that contains exactly 1 booking
    // Booking at i=50: [50100, 50101)
    let avail = engine
        .compute_availability(rid, 50_000, 51_000, None)
        .await
        .unwrap();
    // Within [50000, 51000): booking at [50100, 50101)
    // Free: [50000, 50100) + [50101, 51000)
    assert_eq!(avail.len(), 2);
    assert_eq!(avail[0], Span::new(50_000, 50_100));
    assert_eq!(avail[1], Span::new(50_101, 51_000));
}

#[tokio::test]
async fn engine_availability_past_query() {
    // Query entirely in the past, should still return correctly
    let path = test_wal_path("past_query.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), rid, Span::new(100, 200), false)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(150, 175), None)
        .await
        .unwrap();

    let avail = engine
        .compute_availability(rid, 100, 200, None)
        .await
        .unwrap();
    assert_eq!(
        avail,
        vec![Span::new(100, 150), Span::new(175, 200)]
    );
}

#[tokio::test]
async fn engine_min_duration_larger_than_all_gaps() {
    // min_duration filters out all remaining gaps
    let path = test_wal_path("min_dur_all_filtered.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), rid, Span::new(0, H), false)
        .await
        .unwrap();
    // Two bookings leave only 20-min gaps
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(20 * M, 40 * M), None)
        .await
        .unwrap();

    // Ask for min 30 minutes
    let avail = engine
        .compute_availability(rid, 0, H, Some(30 * M))
        .await
        .unwrap();
    // [0, 20min) = 20min → too short.  [40min, 60min) = 20min → too short.
    assert!(avail.is_empty());
}

// ══════════════════════════════════════════════════════════════
// Full hold → book → cancel lifecycle
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn engine_hold_to_booking_flow() {
    let path = test_wal_path("hold_to_book.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), rid, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let hold_id = Ulid::new();
    let far_future = now_ms() + H;

    // Step 1: Place hold
    engine
        .place_hold(hold_id, rid, Span::new(10 * H, 11 * H), far_future)
        .await
        .unwrap();

    // Verify slot is blocked
    let avail = engine
        .compute_availability(rid, 9 * H, 17 * H, None)
        .await
        .unwrap();
    assert_eq!(
        avail,
        vec![Span::new(9 * H, 10 * H), Span::new(11 * H, 17 * H)]
    );

    // Step 2: Release hold
    engine.release_hold(hold_id).await.unwrap();

    // Step 3: Confirm booking at same slot
    let booking_id = Ulid::new();
    engine
        .confirm_booking(booking_id, rid, Span::new(10 * H, 11 * H), None)
        .await
        .unwrap();

    // Verify still blocked (now by booking)
    let avail2 = engine
        .compute_availability(rid, 9 * H, 17 * H, None)
        .await
        .unwrap();
    assert_eq!(
        avail2,
        vec![Span::new(9 * H, 10 * H), Span::new(11 * H, 17 * H)]
    );

    // Step 4: Cancel booking → slot reopens
    engine.cancel_booking(booking_id).await.unwrap();

    let avail3 = engine
        .compute_availability(rid, 9 * H, 17 * H, None)
        .await
        .unwrap();
    assert_eq!(avail3, vec![Span::new(9 * H, 17 * H)]);
}
