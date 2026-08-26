use crate::engine::*;
use crate::clock::now_ms;
use super::helpers::*;

// ── T-03: the write path honors rules (read/write agreement) ────

#[tokio::test]
async fn booking_outside_open_hours_is_rejected() {
    // T-03: availability reports time outside the schedule as unavailable, so admission must
    // reject it. Before the fix the conflict check weighed allocations only and a booking at
    // 03:00 on a 9-17 resource succeeded.
    let path = test_wal_path("t03_outside_open.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(9 * H, 17 * H), false).await.unwrap();

    let result = engine.confirm_booking(Ulid::new(), rid, Span::new(3 * H, 4 * H), None).await;
    assert!(
        matches!(result, Err(EngineError::ClosedBySchedule { .. })),
        "booking outside open hours must be rejected, got {result:?}"
    );

    // Inside the schedule it books.
    engine.confirm_booking(Ulid::new(), rid, Span::new(10 * H, 11 * H), None).await.unwrap();
}

#[tokio::test]
async fn booking_into_a_blocked_window_is_rejected() {
    // The audit's concrete scenario: open 9-17 with a blocking rule 12-13. availability reports
    // [9,12)+[13,17), yet a booking [12:00,12:30) was accepted and durably committed.
    let path = test_wal_path("t03_blocked.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(9 * H, 17 * H), false).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(12 * H, 13 * H), true).await.unwrap();

    let result = engine
        .confirm_booking(Ulid::new(), rid, Span::new(12 * H, 12 * H + 30 * M), None)
        .await;
    assert!(
        matches!(result, Err(EngineError::ClosedBySchedule { .. })),
        "booking into a blocked window must be rejected, got {result:?}"
    );
}

#[tokio::test]
async fn hold_on_closed_time_is_rejected() {
    // Holds pass the same admission gate as bookings.
    let path = test_wal_path("t03_hold.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(9 * H, 17 * H), false).await.unwrap();

    let result = engine
        .place_hold(Ulid::new(), rid, Span::new(18 * H, 19 * H), now_ms() + H)
        .await;
    assert!(
        matches!(result, Err(EngineError::ClosedBySchedule { .. })),
        "hold outside open hours must be rejected, got {result:?}"
    );
    engine
        .place_hold(Ulid::new(), rid, Span::new(10 * H, 11 * H), now_ms() + H)
        .await
        .unwrap();
}

#[tokio::test]
async fn booking_outside_inherited_schedule_is_rejected() {
    // A child with no own rules lives under its nearest scheduling ancestor's windows on the
    // read path; admission must apply the same inherited base.
    let path = test_wal_path("t03_inherited.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), parent, Span::new(9 * H, 17 * H), false).await.unwrap();
    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    let result = engine.confirm_booking(Ulid::new(), child, Span::new(3 * H, 4 * H), None).await;
    assert!(
        matches!(result, Err(EngineError::ClosedBySchedule { .. })),
        "booking outside the inherited schedule must be rejected, got {result:?}"
    );
    engine.confirm_booking(Ulid::new(), child, Span::new(10 * H, 11 * H), None).await.unwrap();
}

#[tokio::test]
async fn inherited_blocking_rejects_admission_on_the_child() {
    // Blocking accumulates down the tree on the read path; admission subtracts it too.
    let path = test_wal_path("t03_inherited_blocking.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), parent, Span::new(0, 24 * H), false).await.unwrap();
    engine.add_rule(Ulid::new(), parent, Span::new(12 * H, 13 * H), true).await.unwrap();
    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    let result = engine
        .confirm_booking(Ulid::new(), child, Span::new(12 * H, 13 * H), None)
        .await;
    assert!(
        matches!(result, Err(EngineError::ClosedBySchedule { .. })),
        "inherited blocking must reject the child booking, got {result:?}"
    );
}

#[tokio::test]
async fn unscheduled_resource_admits_by_collision_only() {
    // The documented T-03 exception: a chain with no non-blocking rule anywhere defines no
    // schedule. Open hours cannot constrain (deltat's collision-detector mode; the read path
    // reports zero availability because it has no windows to enumerate), but blocking rules
    // still reject.
    let path = test_wal_path("t03_unscheduled.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    // No rules at all: any free span books.
    engine.confirm_booking(Ulid::new(), rid, Span::new(3 * H, 4 * H), None).await.unwrap();

    // A blocking rule on an unscheduled resource still closes its window for admission.
    engine.add_rule(Ulid::new(), rid, Span::new(12 * H, 13 * H), true).await.unwrap();
    let result = engine
        .confirm_booking(Ulid::new(), rid, Span::new(12 * H + 15 * M, 12 * H + 45 * M), None)
        .await;
    assert!(
        matches!(result, Err(EngineError::ClosedBySchedule { .. })),
        "blocking must reject even without a schedule, got {result:?}"
    );
    // Outside the blocked window it still books.
    engine.confirm_booking(Ulid::new(), rid, Span::new(14 * H, 15 * H), None).await.unwrap();
}

#[tokio::test]
async fn batch_with_one_member_on_closed_time_rejects_atomically() {
    // Phase-1 validation covers the schedule too: one out-of-hours member fails the whole
    // batch, and nothing is committed.
    let path = test_wal_path("t03_batch.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(9 * H, 17 * H), false).await.unwrap();

    let batch = vec![
        (Ulid::new(), rid, Span::new(10 * H, 11 * H), None),
        (Ulid::new(), rid, Span::new(18 * H, 19 * H), None), // closed
    ];
    let result = engine.batch_confirm_bookings(batch).await;
    assert!(
        matches!(result, Err(EngineError::ClosedBySchedule { .. })),
        "a closed-time member must fail the batch, got {result:?}"
    );
    assert!(engine.get_bookings(rid).await.unwrap().is_empty(), "batch must be atomic");
}

#[tokio::test]
async fn buffer_tail_may_run_past_the_open_window() {
    // The documented buffer exemption: only the RAW span must sit inside the open windows; the
    // turnaround tail may extend past close (cleanup happens after hours). The tail still
    // blocks later allocations via the symmetric footprint check.
    let path = test_wal_path("t03_buffer_exempt.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, Some(2 * H)).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(9 * H, 17 * H), false).await.unwrap();

    // Books although its buffered footprint [16,19) leaves the window at 17.
    engine.confirm_booking(Ulid::new(), rid, Span::new(16 * H, 17 * H), None).await.unwrap();
}

#[tokio::test]
async fn commit_hold_survives_a_blocking_rule_added_after_placement() {
    // The hold IS the admission: rules are checked when the hold is placed, and a blocking rule
    // added afterwards does not revoke it (exactly as it does not revoke an existing booking).
    // commit_hold therefore re-checks allocations only.
    let path = test_wal_path("t03_commit_hold.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(9 * H, 17 * H), false).await.unwrap();

    let hold_id = Ulid::new();
    engine
        .place_hold(hold_id, rid, Span::new(10 * H, 11 * H), now_ms() + H)
        .await
        .unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(10 * H, 11 * H), true).await.unwrap();

    engine.commit_hold(hold_id, Ulid::new(), None).await.unwrap();
    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 1);
}
