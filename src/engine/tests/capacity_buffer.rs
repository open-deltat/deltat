use crate::engine::*;
use crate::clock::now_ms;
use super::helpers::*;

// ── Capacity tests ───────────────────────────────────────────

#[tokio::test]
async fn capacity_two_bookings_same_slot() {
    let path = test_wal_path("cap_two_same.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 2, None).await.unwrap();

    // Add availability
    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 10000), false)
        .await
        .unwrap();

    // Two bookings on the same span: capacity=2, both should succeed
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn capacity_third_booking_conflicts() {
    let path = test_wal_path("cap_third.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 2, None).await.unwrap();

    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 10000), false)
        .await
        .unwrap();

    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();

    // Third booking should fail, capacity exceeded
    let result = engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn capacity_expired_hold_not_counted() {
    let path = test_wal_path("cap_expired.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 10000), false)
        .await
        .unwrap();

    // Place a hold that expires in the past
    let now = now_ms();
    engine
        .place_hold(Ulid::new(), rid, Span::new(1000, 2000), now - 1000)
        .await
        .unwrap();

    // Booking should succeed because the hold is expired
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn capacity_one_is_default_behavior() {
    let path = test_wal_path("cap_one_default.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 10000), false)
        .await
        .unwrap();

    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();

    // Second booking should fail, capacity=1
    let result = engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await;
    assert!(matches!(result, Err(EngineError::Conflict(_))));
}

#[tokio::test]
async fn batch_capacity_books_n_units_same_span_atomically() {
    // A capacity-N pool (e.g. a stadium GA section) must accept N simultaneous bookings for
    // the SAME span in one atomic batch, the "buy N GA tickets at once" path.
    let path = test_wal_path("batch_cap_n_ok.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 4, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10000), false).await.unwrap();

    let batch: Vec<_> = (0..4)
        .map(|_| (Ulid::new(), rid, Span::new(1000, 2000), None))
        .collect();
    engine.batch_confirm_bookings(batch).await.unwrap();

    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 4);
}

#[tokio::test]
async fn batch_capacity_rejects_over_capacity_atomically() {
    // N+1 simultaneous units on a capacity-N pool must fail as a whole, none committed.
    let path = test_wal_path("batch_cap_over.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 4, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10000), false).await.unwrap();

    let batch: Vec<_> = (0..5)
        .map(|_| (Ulid::new(), rid, Span::new(1000, 2000), None))
        .collect();
    assert!(matches!(
        engine.batch_confirm_bookings(batch).await,
        Err(EngineError::CapacityExceeded(4))
    ));

    // Atomic: the failed batch left nothing behind.
    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 0);
}

#[tokio::test]
async fn batch_capacity_accounts_for_committed_load() {
    // Committed bookings count against the batch: 1 existing + 3 batch == capacity 4 (ok),
    // but 1 existing + 4 batch exceeds it (rejected, atomically).
    let path = test_wal_path("batch_cap_committed.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 4, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10000), false).await.unwrap();

    engine.confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None).await.unwrap();

    // 1 committed + 4 same-span batch members = 5 > capacity 4 → reject, nothing added.
    let over: Vec<_> = (0..4).map(|_| (Ulid::new(), rid, Span::new(1000, 2000), None)).collect();
    assert!(matches!(
        engine.batch_confirm_bookings(over).await,
        Err(EngineError::CapacityExceeded(4))
    ));
    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 1);

    // 1 committed + 3 same-span batch members = 4 == capacity 4 → ok.
    let ok: Vec<_> = (0..3).map(|_| (Ulid::new(), rid, Span::new(1000, 2000), None)).collect();
    engine.batch_confirm_bookings(ok).await.unwrap();
    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 4);
}

#[tokio::test]
async fn batch_capacity_u32_max_does_not_overflow() {
    // Capacity is untrusted (parse_u32 accepts 4294967295). The batch check computes
    // capacity + 1; with overflow-checks on, a plain `+` panics the connection task on a
    // capacity-u32::MAX resource. Such a capacity can never saturate, so the batch must commit.
    let path = test_wal_path("batch_cap_u32_max.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, u32::MAX, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10000), false).await.unwrap();

    let batch: Vec<_> = (0..2)
        .map(|_| (Ulid::new(), rid, Span::new(1000, 2000), None))
        .collect();
    engine.batch_confirm_bookings(batch).await.unwrap();

    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 2);
}

#[tokio::test]
async fn sync_stable_unit_multi_night_availability() {
    // SYNC-01: a capacity-2 pool = 2 interchangeable rooms. Two overlapping multi-night stays
    // saturate the middle night; a longer stay spanning it would require switching rooms, so it
    // must be rejected, while a stay clear of it fits on a single stable room. The capacity
    // sweep already guarantees this: a stay is ONE interval, and max-overlap < capacity over its
    // span ⟺ a stable unit exists for the whole span (interval-graph colouring; chromatic
    // number = max clique). No new primitive needed; this test locks the guarantee.
    let path = test_wal_path("sync_stable.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 2, None).await.unwrap();
    let day = 24 * H;
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10 * day), false).await.unwrap();

    // Stay A: nights 1-3, Stay B: nights 2-4 → night [2,3) is fully booked (2 of 2).
    engine.confirm_booking(Ulid::new(), rid, Span::new(day, 3 * day), None).await.unwrap();
    engine.confirm_booking(Ulid::new(), rid, Span::new(2 * day, 4 * day), None).await.unwrap();

    // A 3-night stay across the saturated night would need a 3rd room → rejected.
    assert!(engine
        .confirm_booking(Ulid::new(), rid, Span::new(day, 4 * day), None)
        .await
        .is_err());

    // A stay clear of the saturated night fits on a stable room.
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(5 * day, 8 * day), None)
        .await
        .unwrap();

    // Availability with a 2-night minimum lists exactly the stable multi-night openings:
    // everything except the saturated night [2,3) → [0,2) and [3,10).
    let openings = engine.compute_availability(rid, 0, 10 * day, Some(2 * day)).await.unwrap();
    assert_eq!(openings, vec![Span::new(0, 2 * day), Span::new(3 * day, 10 * day)]);
}

#[tokio::test]
async fn capacity_availability_shows_partial_slots() {
    let path = test_wal_path("cap_avail.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 3, None).await.unwrap();

    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 10000), false)
        .await
        .unwrap();

    // Book 2 of 3 slots
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();

    // Availability should still show [1000,2000) because capacity is 3 and only 2 booked
    let avail = engine
        .compute_availability(rid, 0, 10000, None)
        .await
        .unwrap();
    assert_eq!(avail, vec![Span::new(0, 10000)]);
}

#[tokio::test]
async fn capacity_saturated_removes_from_availability() {
    let path = test_wal_path("cap_sat.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 2, None).await.unwrap();

    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 10000), false)
        .await
        .unwrap();

    // Fill capacity
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();

    // [1000,2000) should be removed from availability
    let avail = engine
        .compute_availability(rid, 0, 10000, None)
        .await
        .unwrap();
    assert_eq!(
        avail,
        vec![Span::new(0, 1000), Span::new(2000, 10000)]
    );
}

// ── Buffer After tests ───────────────────────────────────────

#[tokio::test]
async fn buffer_after_shrinks_availability() {
    let path = test_wal_path("buf_shrinks.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    let buffer_30min: Ms = 30 * 60 * 1000; // 30 minutes in ms
    engine
        .create_resource(rid, None, None, 1, Some(buffer_30min))
        .await
        .unwrap();

    let h = 3_600_000; // 1 hour in ms
    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 24 * h), false)
        .await
        .unwrap();

    // Booking from 10:00 to 11:00
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(10 * h, 11 * h), None)
        .await
        .unwrap();

    let avail = engine
        .compute_availability(rid, 0, 24 * h, None)
        .await
        .unwrap();

    // Should be: [0, 10h), [11.5h, 24h). Buffer pushes next available to 11:30
    let h_half = h / 2;
    assert_eq!(
        avail,
        vec![Span::new(0, 10 * h), Span::new(11 * h + h_half, 24 * h)]
    );
}

#[tokio::test]
async fn buffer_after_between_bookings() {
    let path = test_wal_path("buf_between.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    let buffer_1h: Ms = 3_600_000;
    engine
        .create_resource(rid, None, None, 1, Some(buffer_1h))
        .await
        .unwrap();

    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 100_000_000), false)
        .await
        .unwrap();

    // Two bookings, should not be able to book immediately after the first
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(0, 10_000_000), None)
        .await
        .unwrap();

    // This booking starts right at the first's end, but buffer should block it
    let result = engine
        .confirm_booking(Ulid::new(), rid, Span::new(10_000_000, 20_000_000), None)
        .await;
    assert!(result.is_err());

    // Booking after buffer gap should succeed
    engine
        .confirm_booking(
            Ulid::new(),
            rid,
            Span::new(10_000_000 + buffer_1h, 20_000_000 + buffer_1h),
            None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn buffer_after_none_is_default_behavior() {
    let path = test_wal_path("buf_none.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 100_000), false)
        .await
        .unwrap();

    engine
        .confirm_booking(Ulid::new(), rid, Span::new(0, 50_000), None)
        .await
        .unwrap();

    // Adjacent booking should succeed with no buffer
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(50_000, 100_000), None)
        .await
        .unwrap();
}

// ── Combined capacity + buffer tests ─────────────────────────

#[tokio::test]
async fn capacity_and_buffer_combined() {
    let path = test_wal_path("cap_buf.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let rid = Ulid::new();
    let buffer = 1000_i64;
    engine
        .create_resource(rid, None, None, 2, Some(buffer))
        .await
        .unwrap();

    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 100_000), false)
        .await
        .unwrap();

    // Two bookings on same slot (capacity=2), both succeed
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 5000), None)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 5000), None)
        .await
        .unwrap();

    // Third fails (capacity exceeded)
    let result = engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 5000), None)
        .await;
    assert!(result.is_err());

    // Booking right after buffer should work for 1 slot
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(5000 + buffer, 10000), None)
        .await
        .unwrap();
}

// ── Vertical: Yoga class with capacity ───────────────────────

#[tokio::test]
async fn vertical_yoga_class_capacity() {
    let path = test_wal_path("vert_yoga.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let class_id = Ulid::new();
    engine
        .create_resource(class_id, None, None, 20, None)
        .await
        .unwrap();

    let h = 3_600_000_i64;
    // Class runs 9am-10am
    engine
        .add_rule(Ulid::new(), class_id, Span::new(9 * h, 10 * h), false)
        .await
        .unwrap();

    // Book 20 people
    for _ in 0..20 {
        engine
            .confirm_booking(Ulid::new(), class_id, Span::new(9 * h, 10 * h), None)
            .await
            .unwrap();
    }

    // 21st person fails
    let result = engine
        .confirm_booking(Ulid::new(), class_id, Span::new(9 * h, 10 * h), None)
        .await;
    assert!(result.is_err());

    // Availability should be empty (class is full)
    let avail = engine
        .compute_availability(class_id, 0, 24 * h, None)
        .await
        .unwrap();
    assert!(avail.is_empty());
}

// ── Vertical: Hotel room with buffer (cleaning time) ─────────

#[tokio::test]
async fn vertical_hotel_room_buffer() {
    let path = test_wal_path("vert_hotel_buf.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let room = Ulid::new();
    let day = 86_400_000_i64; // 1 day in ms
    let cleaning = 2 * 3_600_000_i64; // 2 hours cleaning buffer

    engine
        .create_resource(room, None, None, 1, Some(cleaning))
        .await
        .unwrap();

    // Available for 30 days
    engine
        .add_rule(Ulid::new(), room, Span::new(0, 30 * day), false)
        .await
        .unwrap();

    // Guest 1: checkout day 3 noon (day 0 to day 3 noon)
    let noon = day / 2;
    engine
        .confirm_booking(Ulid::new(), room, Span::new(0, 3 * day + noon), None)
        .await
        .unwrap();

    // Guest 2 cannot check in at day 3 noon (cleaning buffer)
    let result = engine
        .confirm_booking(Ulid::new(), room, Span::new(3 * day + noon, 6 * day + noon), None)
        .await;
    assert!(result.is_err());

    // Guest 2 can check in after cleaning buffer
    engine
        .confirm_booking(
            Ulid::new(),
            room,
            Span::new(3 * day + noon + cleaning, 6 * day + noon),
            None,
        )
        .await
        .unwrap();
}

