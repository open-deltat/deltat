use crate::engine::*;
use crate::clock::{now_ms, TestClock};
use super::helpers::*;

#[tokio::test]
async fn engine_reads_now_from_injected_clock() {
    // The whole determinism seam in one assertion: the engine's notion of "now" is
    // exactly what the injected clock says, and it tracks the clock as it advances.
    let path = test_wal_path("clock_seam.wal");
    let notify = Arc::new(NotifyHub::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let engine = Engine::with_clock(path, notify, clock.clone()).unwrap();

    assert_eq!(engine.now_ms(), 1_000_000);
    clock.advance(5_000);
    assert_eq!(engine.now_ms(), 1_005_000);
}

#[tokio::test]
async fn engine_create_and_query_resource() {
    let path = test_wal_path("create_resource3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let id = Ulid::new();
    engine.create_resource(id, None, None, 1, None).await.unwrap();

    let rs = engine.get_resource(&id).unwrap();
    let guard = rs.read().await;
    assert_eq!(guard.parent_id, None);
}

#[tokio::test]
async fn engine_create_resource_with_parent() {
    let path = test_wal_path("resource_with_parent3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();

    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    let rs = engine.get_resource(&child).unwrap();
    let guard = rs.read().await;
    assert_eq!(guard.parent_id, Some(parent));
}

#[tokio::test]
async fn engine_create_resource_nonexistent_parent_fails() {
    let path = test_wal_path("bad_parent3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let result = engine
        .create_resource(Ulid::new(), Some(Ulid::new()), None, 1, None)
        .await;
    assert!(matches!(result, Err(EngineError::NotFound(_))));
}

#[tokio::test]
async fn engine_create_resource_self_parent_fails() {
    let path = test_wal_path("self_parent3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let id = Ulid::new();
    let result = engine.create_resource(id, Some(id), None, 1, None).await;
    assert!(matches!(result, Err(EngineError::CycleDetected(_))));
}

#[tokio::test]
async fn engine_duplicate_resource_rejected() {
    let path = test_wal_path("dup_resource3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let id = Ulid::new();
    engine.create_resource(id, None, None, 1, None).await.unwrap();
    let result = engine.create_resource(id, None, None, 1, None).await;
    assert!(matches!(result, Err(EngineError::AlreadyExists(_))));
}

#[tokio::test]
async fn engine_delete_resource_with_children_fails() {
    let path = test_wal_path("delete_with_children3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    let result = engine.delete_resource(parent).await;
    assert!(matches!(result, Err(EngineError::HasChildren(_))));
}

#[tokio::test]
async fn engine_hierarchy_inherits_parent_rules() {
    let path = test_wal_path("hierarchy_inherit3.wal");
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

    let avail = engine
        .compute_availability(child, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(avail, vec![Span::new(9 * H, 17 * H)]);
}

#[tokio::test]
async fn engine_hierarchy_blocking_accumulates() {
    let path = test_wal_path("hierarchy_blocking3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), parent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();
    engine
        .add_rule(Ulid::new(), parent, Span::new(12 * H, 13 * H), true)
        .await
        .unwrap();

    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    let avail = engine
        .compute_availability(child, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(
        avail,
        vec![Span::new(9 * H, 12 * H), Span::new(13 * H, 17 * H)]
    );
}

#[tokio::test]
async fn engine_child_overrides_parent_non_blocking() {
    let path = test_wal_path("child_override3.wal");
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
    engine
        .add_rule(Ulid::new(), child, Span::new(14 * H, 16 * H), false)
        .await
        .unwrap();

    let avail = engine
        .compute_availability(child, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(avail, vec![Span::new(14 * H, 16 * H)]);
}

#[tokio::test]
async fn engine_three_level_hierarchy() {
    let path = test_wal_path("three_level3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let theater = Ulid::new();
    engine.create_resource(theater, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), theater, Span::new(9 * H, 23 * H), false)
        .await
        .unwrap();

    let screen = Ulid::new();
    engine
        .create_resource(screen, Some(theater), None, 1, None)
        .await
        .unwrap();
    engine
        .add_rule(Ulid::new(), screen, Span::new(14 * H, 16 * H), false)
        .await
        .unwrap();
    engine
        .add_rule(Ulid::new(), screen, Span::new(18 * H, 20 * H), false)
        .await
        .unwrap();

    // Theater-level blocking added AFTER screen rules
    engine
        .add_rule(Ulid::new(), theater, Span::new(15 * H, 15 * H + 30 * M), true)
        .await
        .unwrap();

    let seat = Ulid::new();
    engine.create_resource(seat, Some(screen), None, 1, None).await.unwrap();

    let avail = engine
        .compute_availability(seat, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(avail.len(), 3);
    assert_eq!(avail[0], Span::new(14 * H, 15 * H));
    assert_eq!(avail[1], Span::new(15 * H + 30 * M, 16 * H));
    assert_eq!(avail[2], Span::new(18 * H, 20 * H));
}

#[tokio::test]
async fn engine_projection_rejects_outside_parent() {
    let path = test_wal_path("projection_reject3.wal");
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

    let result = engine
        .add_rule(Ulid::new(), child, Span::new(8 * H, 10 * H), false)
        .await;
    assert!(matches!(
        result,
        Err(EngineError::NotCoveredByParent { .. })
    ));
}

#[tokio::test]
async fn engine_projection_allows_blocking_anywhere() {
    let path = test_wal_path("projection_blocking_ok3.wal");
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

    engine
        .add_rule(Ulid::new(), child, Span::new(8 * H, 10 * H), true)
        .await
        .unwrap();
}

#[tokio::test]
async fn engine_min_duration_filter() {
    let path = test_wal_path("min_duration3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), rid, Span::new(9 * H, 12 * H), false)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(10 * H, 10 * H + 15 * M), None)
        .await
        .unwrap();

    let all = engine
        .compute_availability(rid, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);

    let filtered = engine
        .compute_availability(rid, 0, 24 * H, Some(90 * M))
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].start, 10 * H + 15 * M);
}

#[tokio::test]
async fn get_bookings_multi_groups_dedups_and_skips_unknown() {
    let path = test_wal_path("bookings_multi3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    let b = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(9 * H, 17 * H), false).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(9 * H, 17 * H), false).await.unwrap();
    engine.confirm_booking(Ulid::new(), a, Span::new(10 * H, 11 * H), None).await.unwrap();
    engine.confirm_booking(Ulid::new(), b, Span::new(12 * H, 13 * H), None).await.unwrap();

    // Duplicate `a` must NOT re-emit a's booking; the unknown id resolves to nothing.
    let unknown = Ulid::new();
    let rows = engine.get_bookings_multi(&[a, b, a, unknown]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.iter().filter(|r| r.resource_id == a).count(), 1);
    assert_eq!(rows.iter().filter(|r| r.resource_id == b).count(), 1);
}

#[tokio::test]
async fn engine_hold_conflict() {
    let path = test_wal_path("hold_conflict3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let far_future = now_ms() + 3_600_000;
    engine
        .place_hold(Ulid::new(), rid, Span::new(1000, 2000), far_future)
        .await
        .unwrap();

    let result = engine
        .place_hold(Ulid::new(), rid, Span::new(1500, 2500), far_future)
        .await;
    assert!(matches!(result, Err(EngineError::Conflict(_))));
}

#[tokio::test]
async fn engine_wal_replay() {
    let path = test_wal_path("replay3.wal");
    let notify = Arc::new(NotifyHub::new());

    let rid = Ulid::new();
    let parent = Ulid::new();
    {
        let engine = Engine::new(path.clone(), notify.clone()).unwrap();
        engine.create_resource(parent, None, None, 1, None).await.unwrap();
        engine
            .create_resource(rid, Some(parent), None, 1, None)
            .await
            .unwrap();
    }

    let engine2 = Engine::new(path, notify).unwrap();
    let rs = engine2.get_resource(&rid).unwrap();
    let guard = rs.read().await;
    assert_eq!(guard.parent_id, Some(parent));
}

#[tokio::test]
async fn engine_add_and_remove_rule() {
    let path = test_wal_path("add_remove_rule3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let rule_id = Ulid::new();
    engine
        .add_rule(rule_id, rid, Span::new(1000, 2000), false)
        .await
        .unwrap();

    {
        let rs = engine.get_resource(&rid).unwrap();
        let guard = rs.read().await;
        assert_eq!(guard.intervals.len(), 1);
    }

    engine.remove_rule(rule_id).await.unwrap();

    {
        let rs = engine.get_resource(&rid).unwrap();
        let guard = rs.read().await;
        assert!(guard.intervals.is_empty());
    }
}

#[tokio::test]
async fn engine_booking_lifecycle() {
    let path = test_wal_path("booking_lifecycle3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let bid = Ulid::new();
    engine
        .confirm_booking(bid, rid, Span::new(1000, 2000), None)
        .await
        .unwrap();

    {
        let rs = engine.get_resource(&rid).unwrap();
        let guard = rs.read().await;
        assert_eq!(guard.intervals.len(), 1);
    }

    engine.cancel_booking(bid).await.unwrap();

    {
        let rs = engine.get_resource(&rid).unwrap();
        let guard = rs.read().await;
        assert!(guard.intervals.is_empty());
    }
}

#[tokio::test]
async fn engine_hold_release() {
    let path = test_wal_path("hold_release3.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let hid = Ulid::new();
    let far_future = now_ms() + 3_600_000;
    engine
        .place_hold(hid, rid, Span::new(1000, 2000), far_future)
        .await
        .unwrap();

    {
        let rs = engine.get_resource(&rid).unwrap();
        let guard = rs.read().await;
        assert_eq!(guard.intervals.len(), 1);
    }

    engine.release_hold(hid).await.unwrap();

    {
        let rs = engine.get_resource(&rid).unwrap();
        let guard = rs.read().await;
        assert!(guard.intervals.is_empty());
    }
}

#[tokio::test]
async fn engine_commit_hold_converts_hold_to_booking() {
    let path = test_wal_path("commit_hold_convert.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let hid = Ulid::new();
    engine
        .place_hold(hid, rid, Span::new(1000, 2000), now_ms() + H)
        .await
        .unwrap();

    let bid = Ulid::new();
    engine.commit_hold(hid, bid, Some("seat-14F".into())).await.unwrap();

    // The hold is gone; exactly one booking covers the held span.
    assert!(engine.get_holds(rid).await.unwrap().is_empty());
    let bookings = engine.get_bookings(rid).await.unwrap();
    assert_eq!(bookings.len(), 1);
    assert_eq!(bookings[0].id, bid);
    assert_eq!((bookings[0].start, bookings[0].end), (1000, 2000));

    // The span is now booked: a fresh booking attempt on it conflicts.
    let err = engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)));
}

#[tokio::test]
async fn engine_commit_hold_excludes_its_own_hold() {
    let path = test_wal_path("commit_hold_exclude.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let hid = Ulid::new();
    engine
        .place_hold(hid, rid, Span::new(1000, 2000), now_ms() + H)
        .await
        .unwrap();

    // Booking the held span the naive way (without releasing first) conflicts with the hold...
    let naive = engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await;
    assert!(matches!(naive, Err(EngineError::Conflict(_))));

    // ...but committing the hold books that exact span, because the hold is excluded from its own
    // conflict check. No release-then-rebook gap.
    engine.commit_hold(hid, Ulid::new(), None).await.unwrap();
    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 1);
    assert!(engine.get_holds(rid).await.unwrap().is_empty());
}

#[tokio::test]
async fn engine_commit_hold_unknown_id_not_found() {
    let path = test_wal_path("commit_hold_notfound.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let err = engine
        .commit_hold(Ulid::new(), Ulid::new(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::NotFound(_)));
}

#[tokio::test]
async fn engine_commit_hold_on_booking_is_not_found() {
    let path = test_wal_path("commit_hold_wrongkind.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let bid = Ulid::new();
    engine
        .confirm_booking(bid, rid, Span::new(1000, 2000), None)
        .await
        .unwrap();

    // bid is a booking, not a hold → there is no hold to commit.
    let err = engine.commit_hold(bid, Ulid::new(), None).await.unwrap_err();
    assert!(matches!(err, EngineError::NotFound(_)));
}

#[tokio::test]
async fn engine_commit_hold_rejects_when_span_booked_after_expiry() {
    let path = test_wal_path("commit_hold_expired.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    // A hold that is already expired does not protect its span...
    let hid = Ulid::new();
    engine
        .place_hold(hid, rid, Span::new(1000, 2000), 1)
        .await
        .unwrap();

    // ...so a competitor books it.
    engine
        .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
        .await
        .unwrap();

    // Committing the lapsed hold must NOT double-book: the conflict check (excluding the hold)
    // still sees the competitor's booking.
    let err = engine.commit_hold(hid, Ulid::new(), None).await.unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)));
    // And nothing partially applied: still exactly one booking, and the lapsed hold is untouched.
    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 1);
    assert_eq!(engine.get_holds(rid).await.unwrap().len(), 1);
}

#[tokio::test]
async fn engine_commit_hold_persists_across_replay() {
    let path = test_wal_path("commit_hold_replay.wal");
    let rid = Ulid::new();
    let bid = Ulid::new();
    {
        let engine = Engine::new(path.clone(), Arc::new(NotifyHub::new())).unwrap();
        engine.create_resource(rid, None, None, 1, None).await.unwrap();
        let hid = Ulid::new();
        engine
            .place_hold(hid, rid, Span::new(1000, 2000), now_ms() + H)
            .await
            .unwrap();
        engine.commit_hold(hid, bid, None).await.unwrap();
    }

    // Reopen from the WAL after a clean shutdown: the hold is gone and the booking survives. Both
    // halves of the commit are durable.
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    assert!(engine.get_holds(rid).await.unwrap().is_empty());
    let bookings = engine.get_bookings(rid).await.unwrap();
    assert_eq!(bookings.len(), 1);
    assert_eq!(bookings[0].id, bid);
}

#[tokio::test]
async fn engine_commit_hold_torn_write_never_overbooks() {
    // commit_hold writes HoldReleased + BookingConfirmed as two records under one fsync, so a torn
    // write (power loss / IO error after the first record's bytes reach disk but before the
    // second's) can persist HoldReleased and lose BookingConfirmed. Replay discards the torn tail.
    // Because release is written BEFORE confirm, the worst a crash can leave is a freed (re-bookable)
    // slot, never a live hold AND a booking on the span (never an overbook, INV-01). This locks
    // that safe direction; it is the durability posture AVAIL-07 actually provides.
    let path = test_wal_path("commit_hold_torn.wal");
    let rid = Ulid::new();
    {
        let engine = Engine::new(path.clone(), Arc::new(NotifyHub::new())).unwrap();
        engine.create_resource(rid, None, None, 1, None).await.unwrap();
        let hid = Ulid::new();
        engine
            .place_hold(hid, rid, Span::new(1000, 2000), now_ms() + H)
            .await
            .unwrap();
        engine.commit_hold(hid, Ulid::new(), None).await.unwrap();
    }

    // Tear the trailing record (BookingConfirmed) by lopping off its tail; the HoldReleased record
    // before it stays intact, so replay applies the release and rejects the truncated booking.
    let len = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(len - 8).unwrap();
    drop(file);

    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let holds = engine.get_holds(rid).await.unwrap();
    let bookings = engine.get_bookings(rid).await.unwrap();
    // The booking was lost, but the unsafe outcome (a lingering hold AND a booking) never occurs:
    // the span is simply free again.
    assert!(bookings.is_empty(), "a torn commit must not leave a booking");
    assert!(holds.is_empty(), "release was durable, so no hold lingers");
}

#[tokio::test]
async fn engine_commit_hold_excludes_own_hold_on_capacity_n() {
    // On a capacity-2 resource already holding two overlapping allocations, committing one must
    // succeed: excluding the committed hold from its own conflict check leaves only the other
    // allocation (1 < 2). Without the exclusion the sweep would count both holds (= capacity) and
    // reject the new booking, so this exercises the capacity>1 exclusion path through
    // collect_active_allocs_with_buffer, distinct from the capacity-1 fast path.
    let path = test_wal_path("commit_hold_cap_n.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 2, None).await.unwrap();

    let far = now_ms() + H;
    let a = Ulid::new();
    let b = Ulid::new();
    engine.place_hold(a, rid, Span::new(10, 20), far).await.unwrap();
    engine.place_hold(b, rid, Span::new(10, 20), far).await.unwrap(); // 2/2 on [10,20]

    // Convert A's slot in place: booking A + hold B = 2 ≤ capacity.
    engine.commit_hold(a, Ulid::new(), None).await.unwrap();
    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 1);
    assert_eq!(engine.get_holds(rid).await.unwrap().len(), 1);

    // A third overlapping allocation now exceeds capacity. Confirms the resource was genuinely full.
    let err = engine
        .confirm_booking(Ulid::new(), rid, Span::new(10, 20), None)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::CapacityExceeded(_)));
}

#[tokio::test]
async fn engine_commit_hold_with_buffer_books_the_held_span() {
    // With a turnaround buffer on the resource, commit_hold still books exactly the held span, and
    // the buffer-extended conflict check excludes the hold itself (no false self-conflict).
    let path = test_wal_path("commit_hold_buffer.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, Some(5)).await.unwrap();

    let hid = Ulid::new();
    engine
        .place_hold(hid, rid, Span::new(100, 200), now_ms() + H)
        .await
        .unwrap();
    engine.commit_hold(hid, Ulid::new(), None).await.unwrap();

    let bookings = engine.get_bookings(rid).await.unwrap();
    assert_eq!(bookings.len(), 1);
    assert_eq!((bookings[0].start, bookings[0].end), (100, 200));
    assert!(engine.get_holds(rid).await.unwrap().is_empty());
}

#[tokio::test]
async fn engine_commit_hold_notifies_resource_and_ancestors() {
    // commit_hold emits HoldReleased then BookingConfirmed on the resource's channel and bubbles
    // both to ancestors, so a subscriber on the resource AND one on its parent each see both, in
    // order.
    let path = test_wal_path("commit_hold_notify.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let parent = Ulid::new();
    let child = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    let hid = Ulid::new();
    engine
        .place_hold(hid, child, Span::new(10, 20), now_ms() + H)
        .await
        .unwrap();

    // Subscribe after the setup mutations so only the commit's events are observed.
    let mut on_child = engine.notify.subscribe(child);
    let mut on_parent = engine.notify.subscribe(parent);

    engine.commit_hold(hid, Ulid::new(), None).await.unwrap();

    assert!(matches!(on_child.recv().await.unwrap(), Event::HoldReleased { .. }));
    assert!(matches!(on_child.recv().await.unwrap(), Event::BookingConfirmed { .. }));
    assert!(matches!(on_parent.recv().await.unwrap(), Event::HoldReleased { .. }));
    assert!(matches!(on_parent.recv().await.unwrap(), Event::BookingConfirmed { .. }));
}

#[tokio::test]
async fn engine_availability_multi_tags_each_resource_and_dedups() {
    let path = test_wal_path("availability_multi.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let a = Ulid::new();
    let b = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    // Distinct open windows so the per-resource breakdown is observable (a merged intersection
    // would lose this).
    engine.add_rule(Ulid::new(), a, Span::new(0, 10 * H), false).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(0, 5 * H), false).await.unwrap();

    // The duplicate `a` in the list must be deduped (one resource's rows, not two).
    let rows = engine.get_availability_multi(&[a, b, a], 0, 24 * H, None).await.unwrap();
    let a_rows: Vec<_> = rows.iter().filter(|(rid, _)| *rid == a).collect();
    let b_rows: Vec<_> = rows.iter().filter(|(rid, _)| *rid == b).collect();
    assert_eq!(a_rows.len(), 1, "a deduped to one free span");
    assert_eq!(b_rows.len(), 1);
    assert_eq!((a_rows[0].1.start, a_rows[0].1.end), (0, 10 * H));
    assert_eq!((b_rows[0].1.start, b_rows[0].1.end), (0, 5 * H));
}

#[tokio::test]
async fn engine_batch_add_rules_adds_all() {
    let path = test_wal_path("batch_add_rules.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let rules = vec![
        (Ulid::new(), rid, Span::new(0, H), false),
        (Ulid::new(), rid, Span::new(2 * H, 3 * H), false),
        (Ulid::new(), rid, Span::new(4 * H, 5 * H), true),
    ];
    engine.batch_add_rules(rules).await.unwrap();

    // All three rules applied in one call (the round-trip collapse the SDK relies on).
    assert_eq!(engine.get_rules(rid).await.unwrap().len(), 3);
}

#[tokio::test]
async fn engine_batch_create_resources_creates_all_with_intra_batch_parent() {
    let path = test_wal_path("batch_create_resources.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let parent = Ulid::new();
    let child1 = Ulid::new();
    let child2 = Ulid::new();
    // The child rows reference `parent`, which is created earlier in the same batch (applied in
    // list order), so intra-batch parent references resolve.
    let resources = vec![
        (parent, None, Some("section".to_string()), 1u32, None),
        (child1, Some(parent), None, 1u32, None),
        (child2, Some(parent), None, 1u32, None),
    ];
    engine.batch_create_resources(resources).await.unwrap();

    assert!(engine.get_resource(&parent).is_some());
    let children = engine
        .list_resources()
        .await
        .into_iter()
        .filter(|r| r.parent_id == Some(parent))
        .count();
    assert_eq!(children, 2);
}

#[tokio::test]
async fn engine_reaper_watermark_skips_but_still_reaps() {
    let path = test_wal_path("reaper_watermark.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let hid = Ulid::new();
    engine.place_hold(hid, rid, Span::new(0, 100), 2_000_000).await.unwrap();

    // First scan establishes the watermark (earliest live expiry = 2_000_000) and finds nothing due.
    assert!(engine.collect_expired_holds(1_500_000).is_empty());
    // Now strictly below the watermark → the scan is skipped, still nothing.
    assert!(engine.collect_expired_holds(1_600_000).is_empty());
    // At expiry → the hold is reaped (the watermark never causes a missed expiry).
    let expired = engine.collect_expired_holds(2_000_000);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].0, hid);
}

// ══════════════════════════════════════════════════════════════
// Pure function edge cases
// ══════════════════════════════════════════════════════════════

#[test]
fn availability_no_rules_no_availability() {
    // A resource with no rules has zero availability regardless of query window
    let rs = make_resource(vec![]);
    let query = Span::new(0, 24 * H);
    let free = availability(&rs, &query, &[], &[], 0);
    assert!(free.is_empty());
}

#[test]
fn availability_multiple_non_blocking_merge() {
    // Overlapping non-blocking rules should merge: [9,11) + [10,12) → [9,12)
    let rs = make_resource(vec![
        rule(9 * H, 11 * H, false),
        rule(10 * H, 12 * H, false),
    ]);
    let query = Span::new(0, 24 * H);
    let free = availability(&rs, &query, &[], &[], 0);
    assert_eq!(free, vec![Span::new(9 * H, 12 * H)]);
}

#[test]
fn availability_blocking_covers_all_non_blocking() {
    // Blocking completely covers non-blocking → zero availability
    let rs = make_resource(vec![
        rule(9 * H, 17 * H, false),
        rule(8 * H, 18 * H, true), // wider blocking
    ]);
    let query = Span::new(0, 24 * H);
    let free = availability(&rs, &query, &[], &[], 0);
    assert!(free.is_empty());
}

#[test]
fn availability_narrow_query_window() {
    // Query window of exactly 1ms inside a non-blocking rule
    let rs = make_resource(vec![rule(9 * H, 17 * H, false)]);
    let query = Span::new(10 * H, 10 * H + 1);
    let free = availability(&rs, &query, &[], &[], 0);
    assert_eq!(free, vec![Span::new(10 * H, 10 * H + 1)]);
}

#[test]
fn availability_query_larger_than_rules() {
    // Query [0, 48h) but rule only covers [9,17) → result clamped to [9,17)
    let rs = make_resource(vec![rule(9 * H, 17 * H, false)]);
    let query = Span::new(0, 48 * H);
    let free = availability(&rs, &query, &[], &[], 0);
    assert_eq!(free, vec![Span::new(9 * H, 17 * H)]);
}

#[test]
fn availability_mixed_expired_and_active_holds() {
    let nine = 9 * H;
    let ten = 10 * H;
    let eleven = 11 * H;
    let twelve = 12 * H;

    let now = 5000;
    let rs = make_resource(vec![
        rule(nine, twelve, false),
        hold(nine, ten, 1),        // expired (1 < 5000)
        hold(ten, eleven, 99999),  // active
    ]);
    let query = Span::new(0, 24 * H);
    let free = availability(&rs, &query, &[], &[], now);
    // Expired hold ignored → [9,10) available.  Active hold blocks [10,11).  [11,12) available.
    assert_eq!(
        free,
        vec![Span::new(nine, ten), Span::new(eleven, twelve)]
    );
}

#[test]
fn availability_booking_fragments_into_many() {
    // 3 bookings splitting one non-blocking rule into 4 segments
    let rs = make_resource(vec![
        rule(0, 1000, false),
        booking(100, 200),
        booking(400, 500),
        booking(700, 800),
    ]);
    let query = Span::new(0, 1000);
    let free = availability(&rs, &query, &[], &[], 0);
    assert_eq!(
        free,
        vec![
            Span::new(0, 100),
            Span::new(200, 400),
            Span::new(500, 700),
            Span::new(800, 1000),
        ]
    );
}

#[test]
fn availability_blocking_only_no_non_blocking() {
    // Only blocking rules, no non-blocking → zero availability
    let rs = make_resource(vec![rule(9 * H, 17 * H, true)]);
    let query = Span::new(0, 24 * H);
    let free = availability(&rs, &query, &[], &[], 0);
    assert!(free.is_empty());
}

#[test]
fn availability_booking_without_rules() {
    // Bookings exist but no rules → no availability (bookings don't create availability)
    let rs = make_resource(vec![booking(9 * H, 10 * H)]);
    let query = Span::new(0, 24 * H);
    let free = availability(&rs, &query, &[], &[], 0);
    assert!(free.is_empty());
}

#[test]
fn merge_empty() {
    assert!(merge_overlapping(&[]).is_empty());
}

#[test]
fn merge_single() {
    let result = merge_overlapping(&[Span::new(100, 200)]);
    assert_eq!(result, vec![Span::new(100, 200)]);
}

#[test]
fn subtract_empty_base() {
    let result = subtract_intervals(&[], &[Span::new(0, 100)]);
    assert!(result.is_empty());
}

#[test]
fn subtract_empty_removals() {
    let base = vec![Span::new(100, 200)];
    let result = subtract_intervals(&base, &[]);
    assert_eq!(result, base);
}


#[test]
fn store_get_children() {
    let store = InMemoryStore::new();
    let parent = Ulid::new();
    let c1 = Ulid::new();
    let c2 = Ulid::new();

    // Empty initially
    assert!(store.get_children(&parent).is_empty());

    store.add_child(parent, c1);
    store.add_child(parent, c2);
    let kids = store.get_children(&parent);
    assert_eq!(kids.len(), 2);
    assert!(kids.contains(&c1));
    assert!(kids.contains(&c2));

    store.remove_child(&parent, &c1);
    let kids = store.get_children(&parent);
    assert_eq!(kids, vec![c2]);
}

#[test]
fn store_default() {
    let store = InMemoryStore::default();
    assert_eq!(store.resource_count(), 0);
}
