use crate::engine::*;
use crate::clock::{now_ms, TestClock};
use crate::limits::*;
use proptest::prelude::*;
use super::helpers::*;

// ── Untrusted-input hardening regressions (OSS review) ──────────────────

#[tokio::test]
async fn availability_query_with_extreme_bounds_is_rejected_not_panicked() {
    // start = -1, end = i64::MAX orders correctly (end > start), so the inverted-window
    // guard passes, but end - start overflows i64. With overflow-checks on, the naive
    // subtraction panicked the connection task; it must be rejected as a too-wide window.
    let path = test_wal_path("avail_extreme_bounds.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let id = Ulid::new();
    engine.create_resource(id, None, None, 1, None).await.unwrap();

    let single = engine.compute_availability(id, -1, i64::MAX, None).await;
    assert!(matches!(
        single,
        Err(EngineError::LimitExceeded("query window too wide"))
    ));

    let multi = engine
        .compute_multi_availability(&[id], -1, i64::MAX, 1, None)
        .await;
    assert!(matches!(
        multi,
        Err(EngineError::LimitExceeded("query window too wide"))
    ));
}

#[tokio::test]
async fn multi_availability_inverted_window_is_empty() {
    let path = test_wal_path("multi_avail_inverted.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let id = Ulid::new();
    engine.create_resource(id, None, None, 1, None).await.unwrap();

    let result = engine
        .compute_multi_availability(&[id], 5_000, 1_000, 1, None)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn multi_availability_threshold_above_resource_count_is_empty() {
    // Asking for 2 free out of 1 resource can never be satisfied. This guard also
    // neutralizes a min_available value that wrapped from a negative SQL literal.
    let path = test_wal_path("multi_avail_threshold.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let id = Ulid::new();
    engine.create_resource(id, None, None, 1, None).await.unwrap();

    let result = engine
        .compute_multi_availability(&[id], 0, 10_000, 2, None)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn gc_with_unbounded_retention_does_not_underflow() {
    // retention_ms is operator-configured; now - retention_ms must saturate, not underflow.
    let path = test_wal_path("gc_unbounded_retention.wal");
    let notify = Arc::new(NotifyHub::new());
    let clock = Arc::new(TestClock::new(1_000));
    let engine = Engine::with_clock(path, notify, clock).unwrap();
    let id = Ulid::new();
    engine.create_resource(id, None, None, 1, None).await.unwrap();
    // A booking that ended well before now, eligible for collection under a normal retention.
    engine
        .confirm_booking(Ulid::new(), id, Span::new(10, 20), None)
        .await
        .unwrap();

    // A huge retention saturates the cutoff to i64::MIN, so nothing is old enough: collect nothing
    // rather than panic or over-collect.
    assert_eq!(engine.gc_past_intervals(engine.now_ms(), i64::MAX), 0);
    // Zero retention puts the cutoff at now, so the past booking is collected. This proves the
    // assertion above distinguishes correct saturation from accidental over- or under-collection.
    assert_eq!(engine.gc_past_intervals(engine.now_ms(), 0), 1);
}

#[tokio::test]
async fn delete_resource_reclaims_notify_channel() {
    // A deleted resource gets no further events, so its broadcast channel is reclaimed:
    // the delivered ResourceDeleted drains first, then the channel reports closed.
    let path = test_wal_path("delete_reclaims_notify.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify.clone()).unwrap();
    let id = Ulid::new();
    engine.create_resource(id, None, None, 1, None).await.unwrap();

    let mut rx = notify.subscribe(id);
    engine.delete_resource(id).await.unwrap();

    let delivered = rx.recv().await.unwrap();
    assert!(matches!(delivered, Event::ResourceDeleted { .. }));
    assert!(matches!(
        rx.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Closed)
    ));
}

#[tokio::test]
async fn compact_wal_waits_for_a_locked_resource() {
    // Compaction runs on a timer while a mutation holds a resource's write lock across its
    // awaited WAL append. The old try_read().expect() panicked the compactor; it must wait
    // for the lock and keep the resource in the rewritten WAL.
    use std::time::Duration;

    let path = test_wal_path("compact_locked_resource.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());
    let id = Ulid::new();
    engine.create_resource(id, None, None, 1, None).await.unwrap();

    let rs = engine.get_resource(&id).unwrap();
    let guard = rs.write().await;

    let compactor = tokio::spawn({
        let engine = engine.clone();
        async move { engine.compact_wal().await }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(guard);

    compactor.await.unwrap().unwrap();
    assert!(engine.get_resource(&id).is_some());
}

#[tokio::test]
async fn resource_created_during_compaction_window_survives_replay() {
    // A write acked while compact_wal is snapshotting is fsynced into the OLD wal file; the
    // compaction swap must not erase it. Stall the snapshot on r1's write lock, create r2 during
    // the stall (create holds no per-resource lock, so it goes straight through), finish
    // compaction, then replay the file: r2's acknowledged create must still be there.
    use std::time::Duration;

    let path = test_wal_path("compact_window_create.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path.clone(), notify).unwrap());
    let r1 = Ulid::new();
    engine.create_resource(r1, None, None, 1, None).await.unwrap();

    let rs = engine.get_resource(&r1).unwrap();
    let guard = rs.write().await;

    let compactor = tokio::spawn({
        let engine = engine.clone();
        async move { engine.compact_wal().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await; // compactor is now blocked on r1

    let r2 = Ulid::new();
    engine.create_resource(r2, None, None, 1, None).await.unwrap();

    drop(guard);
    compactor.await.unwrap().unwrap();

    let replayed = Wal::replay(&path).unwrap();
    assert!(
        replayed
            .iter()
            .any(|e| matches!(e, Event::ResourceCreated { id, .. } if *id == r2)),
        "acknowledged create during the compaction window must survive the swap"
    );
}

#[tokio::test]
async fn booking_acked_during_compaction_window_survives_replay_exactly_once() {
    // Same stall as above, but for a lock-holding mutation on an already-live resource. Whether
    // the booking lands before or after r2's snapshot read depends on the snapshot loop's
    // iteration order, so this covers whichever path runs: recorded-only (the booking missed the
    // snapshot and must come from the recording) or snapshotted (the recorded duplicate must be
    // dropped). Either way replay must yield the booking exactly once.
    use std::time::Duration;

    let path = test_wal_path("compact_window_booking.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path.clone(), notify).unwrap());
    let r1 = Ulid::new();
    let r2 = Ulid::new();
    engine.create_resource(r1, None, None, 1, None).await.unwrap();
    engine.create_resource(r2, None, None, 1, None).await.unwrap();

    let rs = engine.get_resource(&r1).unwrap();
    let guard = rs.write().await;

    let compactor = tokio::spawn({
        let engine = engine.clone();
        async move { engine.compact_wal().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await; // compactor is now inside its window

    let booking_id = Ulid::new();
    engine
        .confirm_booking(booking_id, r2, Span::new(10 * H, 11 * H), None)
        .await
        .unwrap();

    drop(guard);
    compactor.await.unwrap().unwrap();

    let replayed = Wal::replay(&path).unwrap();
    let occurrences = replayed
        .iter()
        .filter(|e| matches!(e, Event::BookingConfirmed { id, .. } if *id == booking_id))
        .count();
    assert_eq!(
        occurrences, 1,
        "acknowledged booking during the compaction window must survive the swap exactly once"
    );
}

#[test]
fn merge_recorded_drops_snapshotted_additions_and_keeps_the_rest() {
    let rid = Ulid::new();
    let create = Event::ResourceCreated {
        id: rid,
        parent_id: None,
        name: None,
        capacity: 1,
        buffer_after: None,
    };
    let snapshotted_booking = Event::BookingConfirmed {
        id: Ulid::new(),
        resource_id: rid,
        span: Span::new(0, H),
        label: None,
    };
    let new_hold = Event::HoldPlaced {
        id: Ulid::new(),
        resource_id: rid,
        span: Span::new(H, 2 * H),
        expires_at: 3 * H,
    };
    // A removal whose interval is already gone from the snapshot replays as a no-op, so it is
    // kept unconditionally rather than matched against the snapshot.
    let stale_release = Event::HoldReleased { id: Ulid::new(), resource_id: rid };

    let snapshot = vec![create.clone(), snapshotted_booking.clone()];
    let recorded = vec![
        create.clone(),             // duplicate: replaying it would reset the resource
        snapshotted_booking.clone(), // duplicate: replaying it would double-count capacity
        new_hold.clone(),
        stale_release.clone(),
    ];

    let merged = merge_recorded(snapshot, recorded);
    assert_eq!(merged, vec![create, snapshotted_booking, new_hold, stale_release]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_bookings_on_capacity_one_admit_exactly_one() {
    // INV-09: the per-resource write lock serializes mutations, so racing many bookings for the
    // same span on a capacity-1 resource admits exactly one and conflicts the rest. Covers the
    // same-resource race directly (the prior concurrency test uses distinct resources).
    let path = test_wal_path("concurrent_same_resource.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());
    let id = Ulid::new();
    engine.create_resource(id, None, None, 1, None).await.unwrap();

    let span = Span::new(1_000, 2_000);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            engine.confirm_booking(Ulid::new(), id, span, None).await
        }));
    }

    let mut admitted = 0;
    let mut conflicted = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(()) => admitted += 1,
            Err(EngineError::Conflict(_)) => conflicted += 1,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert_eq!(admitted, 1);
    assert_eq!(conflicted, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_bookings_on_capacity_n_admit_exactly_n() {
    // INV-01 under contention on a capacity pool: racing many bookings for one span on a capacity-N
    // resource admits exactly N (the capacity sweep, not just the capacity-1 fast path, holds under
    // the serializing write lock); the rest are rejected as over capacity.
    let path = test_wal_path("concurrent_capacity_n.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());
    let id = Ulid::new();
    let capacity = 2u32;
    engine.create_resource(id, None, None, capacity, None).await.unwrap();

    let span = Span::new(1_000, 2_000);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            engine.confirm_booking(Ulid::new(), id, span, None).await
        }));
    }

    let mut admitted = 0u32;
    let mut over_capacity = 0u32;
    for task in tasks {
        match task.await.unwrap() {
            Ok(()) => admitted += 1,
            Err(EngineError::CapacityExceeded(_)) => over_capacity += 1,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert_eq!(admitted, capacity);
    assert_eq!(over_capacity, 8 - capacity);
}

#[test]
fn availability_never_panics_on_arbitrary_query_bounds() {
    // PRIN-08: the read path must never panic on untrusted bounds, however extreme. Drive both
    // availability functions with arbitrary i64 windows and arbitrary usize thresholds against a
    // non-trivial resource. Any Ok or Err is acceptable; a panic fails the test.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (engine, id) = rt.block_on(async {
        let path = test_wal_path("fuzz_avail_bounds.wal");
        let notify = Arc::new(NotifyHub::new());
        let engine = Engine::new(path, notify).unwrap();
        let id = Ulid::new();
        engine.create_resource(id, None, None, 3, None).await.unwrap();
        engine
            .add_rule(Ulid::new(), id, Span::new(0, 100_000_000), false)
            .await
            .unwrap();
        engine
            .confirm_booking(Ulid::new(), id, Span::new(1_000, 2_000), None)
            .await
            .unwrap();
        (engine, id)
    });

    proptest!(
        ProptestConfig::with_cases(1000),
        |(start in any::<i64>(), end in any::<i64>(), min_av in any::<usize>())| {
            let _ = rt.block_on(engine.compute_availability(id, start, end, None));
            let _ = rt.block_on(engine.compute_multi_availability(&[id], start, end, min_av, None));
        }
    );
}

#[test]
fn engine_never_exceeds_capacity_through_command_path() {
    // INV-01 through the real mutation path (the spec's pending TEST-02, focused form): apply an
    // arbitrary booking sequence to a capacity-K resource. The engine accepts some and rejects any
    // that would breach capacity, so no instant may end up with more than K active bookings.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    proptest!(
        ProptestConfig::with_cases(64),
        |(raw in prop::collection::vec((0i64..20, 1i64..15), 1..25), capacity in 1u32..4)| {
            let accepted: Vec<Span> = rt.block_on(async {
                let path = test_wal_path(&format!("stateful_capacity_{}.wal", Ulid::new()));
                let notify = Arc::new(NotifyHub::new());
                let engine = Engine::new(path, notify).unwrap();
                let id = Ulid::new();
                engine
                    .create_resource(id, None, None, capacity, None)
                    .await
                    .unwrap();
                let mut accepted = Vec::new();
                for (start_h, len_h) in &raw {
                    let span = Span::new(start_h * H, (start_h + len_h) * H);
                    if engine
                        .confirm_booking(Ulid::new(), id, span, None)
                        .await
                        .is_ok()
                    {
                        accepted.push(span);
                    }
                }
                accepted
            });

            for t in 0..40i64 {
                let instant = t * H;
                let active = accepted.iter().filter(|s| s.contains_instant(instant)).count() as u32;
                prop_assert!(
                    active <= capacity,
                    "instant {instant}: {active} active exceeds capacity {capacity}"
                );
            }
        }
    );
}

#[test]
fn multi_availability_never_panics_on_arbitrary_inputs() {
    // Extends the no-panic guarantee to the multi-resource sweep over several occupied resources:
    // arbitrary thresholds and windows must never panic. In particular the segment-close guard must
    // never construct a zero-width Span when coverage opens and closes at the same instant.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (engine, ids) = rt.block_on(async {
        let path = test_wal_path("fuzz_multi_avail.wal");
        let notify = Arc::new(NotifyHub::new());
        let engine = Engine::new(path, notify).unwrap();
        let mut ids = Vec::new();
        for k in 0..4i64 {
            let id = Ulid::new();
            engine.create_resource(id, None, None, 2, None).await.unwrap();
            engine
                .add_rule(Ulid::new(), id, Span::new(0, 10_000), false)
                .await
                .unwrap();
            let base = k * 1_000;
            engine
                .confirm_booking(Ulid::new(), id, Span::new(base, base + 1_500), None)
                .await
                .unwrap();
            ids.push(id);
        }
        (engine, ids)
    });

    proptest!(
        ProptestConfig::with_cases(1500),
        |(start in 0i64..6_000, end in 0i64..6_000, min_av in 1usize..=4, n in 1usize..=4)| {
            let subset = &ids[..n.min(ids.len())];
            let _ = rt.block_on(engine.compute_multi_availability(subset, start, end, min_av, None));
        }
    );
}

#[tokio::test]
async fn availability_bounds_a_corrupt_over_deep_hierarchy() {
    // create_resource caps depth, but WAL replay inserts resources directly without that check, so
    // a crafted or corrupt store could exceed MAX_HIERARCHY_DEPTH. collect_inherited_rules must
    // still bound its ancestor walk and reject it rather than loop unbounded.
    let path = test_wal_path("corrupt_deep_hierarchy.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let mut parent = None;
    let mut deepest = Ulid::new();
    for _ in 0..MAX_HIERARCHY_DEPTH + 5 {
        let id = Ulid::new();
        engine.store.insert_resource(
            id,
            Arc::new(tokio::sync::RwLock::new(ResourceState::new(id, parent, None, 1, None))),
        );
        parent = Some(id);
        deepest = id;
    }

    let result = engine.compute_availability(deepest, 0, 1_000, None).await;
    assert!(matches!(
        result,
        Err(EngineError::LimitExceeded("hierarchy too deep"))
    ));
}

// ══════════════════════════════════════════════════════════════
// Review batch A, correctness fixes
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn availability_negative_window_returns_empty_not_panic() {
    // A negative query_end (reachable from SQL unary-minus, passed raw to the engine) once slipped
    // past the inverted-window guard and panicked inside availability() at Span::new(0, negative).
    // Clamping the bounds must yield an empty result instead.
    let path = test_wal_path("neg_window.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 100_000), false)
        .await
        .unwrap();

    let free = engine.compute_availability(rid, -1000, -500, None).await.unwrap();
    assert!(free.is_empty());
    let free2 = engine.compute_availability(rid, i64::MIN, -1, None).await.unwrap();
    assert!(free2.is_empty());
}

#[tokio::test]
async fn delete_resource_unmaps_owned_entities() {
    let path = test_wal_path("delete_unmaps.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let rule_id = Ulid::new();
    engine
        .add_rule(rule_id, rid, Span::new(0, 100_000), false)
        .await
        .unwrap();

    // Before delete the rule resolves to its resource.
    assert_eq!(engine.get_resource_for_entity(&rule_id), Some(rid));

    engine.delete_resource(rid).await.unwrap();

    // After delete the entity->resource mapping is gone (no leak), and any write op on it is NotFound.
    assert!(engine.get_resource_for_entity(&rule_id).is_none());
    let err = engine.remove_rule(rule_id).await.unwrap_err();
    assert!(matches!(err, EngineError::NotFound(_)));
}

#[tokio::test]
async fn entity_write_ops_reject_kind_mismatch() {
    let path = test_wal_path("kind_mismatch.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), rid, Span::new(0, 1_000_000), false)
        .await
        .unwrap();

    let rule_id = Ulid::new();
    engine
        .add_rule(rule_id, rid, Span::new(0, 10_000), true)
        .await
        .unwrap();
    let booking_id = Ulid::new();
    engine
        .confirm_booking(booking_id, rid, Span::new(20_000, 30_000), None)
        .await
        .unwrap();
    let hold_id = Ulid::new();
    engine
        .place_hold(hold_id, rid, Span::new(40_000, 50_000), now_ms() + H)
        .await
        .unwrap();

    // cancel_booking only cancels bookings.
    assert!(matches!(engine.cancel_booking(rule_id).await, Err(EngineError::NotFound(_))));
    assert!(matches!(engine.cancel_booking(hold_id).await, Err(EngineError::NotFound(_))));
    // release_hold only releases holds.
    assert!(matches!(engine.release_hold(booking_id).await, Err(EngineError::NotFound(_))));
    assert!(matches!(engine.release_hold(rule_id).await, Err(EngineError::NotFound(_))));
    // remove_rule / update_rule only touch rules.
    assert!(matches!(engine.remove_rule(booking_id).await, Err(EngineError::NotFound(_))));
    assert!(matches!(
        engine.update_rule(booking_id, Span::new(20_000, 30_000), false).await,
        Err(EngineError::NotFound(_))
    ));

    // The real entities are all still there (nothing was clobbered).
    assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 1);
    assert_eq!(engine.get_holds(rid).await.unwrap().len(), 1);
    assert_eq!(engine.get_rules(rid).await.unwrap().len(), 2);
}

#[tokio::test]
async fn update_rule_enforces_parent_coverage() {
    let path = test_wal_path("update_rule_coverage.wal");
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    // Parent open only 9:00-17:00.
    engine
        .add_rule(Ulid::new(), parent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();
    let child_rule = Ulid::new();
    engine
        .add_rule(child_rule, child, Span::new(10 * H, 12 * H), false)
        .await
        .unwrap();

    // Updating the child rule to a span the parent has NOT opened must be rejected.
    let err = engine
        .update_rule(child_rule, Span::new(6 * H, 8 * H), false)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::NotCoveredByParent { .. }));

    // A within-parent update is fine.
    engine
        .update_rule(child_rule, Span::new(13 * H, 15 * H), false)
        .await
        .unwrap();
    let rules = engine.get_rules(child).await.unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].start, 13 * H);
}

// ══════════════════════════════════════════════════════════════
// Review batch B, buffer semantics (symmetric, order-independent)
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn buffer_conflict_is_order_independent() {
    // A[10:00,11:00) and B[11:15,11:45) with a 30-min buffer cannot coexist on a capacity-1
    // resource: A's turnaround [11:00,11:30) runs into B. The admission decision must be the same
    // whether A is booked first, B is booked first, or the pair is submitted as one batch.
    let a = Span::new(10 * H, 11 * H);
    let b = Span::new(11 * H + 15 * M, 11 * H + 45 * M);
    let buffer = Some(30 * M);

    // Single, A then B.
    {
        let engine = Engine::new(test_wal_path("buf_order_ab.wal"), Arc::new(NotifyHub::new())).unwrap();
        let rid = Ulid::new();
        engine.create_resource(rid, None, None, 1, buffer).await.unwrap();
        engine.add_rule(Ulid::new(), rid, Span::new(0, 24 * H), false).await.unwrap();
        engine.confirm_booking(Ulid::new(), rid, a, None).await.unwrap();
        let second = engine.confirm_booking(Ulid::new(), rid, b, None).await;
        assert!(second.is_err(), "A-then-B: B must be rejected");
        assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 1);
    }

    // Single, B then A, the previously-inconsistent order that admitted an overbooking.
    {
        let engine = Engine::new(test_wal_path("buf_order_ba.wal"), Arc::new(NotifyHub::new())).unwrap();
        let rid = Ulid::new();
        engine.create_resource(rid, None, None, 1, buffer).await.unwrap();
        engine.add_rule(Ulid::new(), rid, Span::new(0, 24 * H), false).await.unwrap();
        engine.confirm_booking(Ulid::new(), rid, b, None).await.unwrap();
        let second = engine.confirm_booking(Ulid::new(), rid, a, None).await;
        assert!(second.is_err(), "B-then-A: A must be rejected too (symmetric buffer)");
        assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 1);
    }

    // Batch, the pair submitted atomically must be rejected as a whole.
    {
        let engine = Engine::new(test_wal_path("buf_order_batch.wal"), Arc::new(NotifyHub::new())).unwrap();
        let rid = Ulid::new();
        engine.create_resource(rid, None, None, 1, buffer).await.unwrap();
        engine.add_rule(Ulid::new(), rid, Span::new(0, 24 * H), false).await.unwrap();
        let batch = engine
            .batch_confirm_bookings(vec![
                (Ulid::new(), rid, a, None),
                (Ulid::new(), rid, b, None),
            ])
            .await;
        assert!(batch.is_err(), "batch: the conflicting pair must be rejected");
        assert_eq!(engine.get_bookings(rid).await.unwrap().len(), 0);
    }
}

// ══════════════════════════════════════════════════════════════
// Review batch D, reaper watermark race + batch WAL atomicity
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn place_hold_bumps_generation_and_lowers_watermark() {
    use std::sync::atomic::Ordering::Relaxed;
    // D1 mechanism: place_hold lowers the reaper's earliest-expiry watermark (fetch_min) AND bumps
    // the placement generation. collect_expired_holds snapshots the generation before scanning and
    // declines to raise the watermark if it changed, so a placement that races a scan cannot be
    // hidden by the scan storing a higher recomputed bound.
    let engine = Engine::new(test_wal_path("hold_gen.wal"), Arc::new(NotifyHub::new())).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    engine.place_hold(Ulid::new(), rid, Span::new(0, 100), 1_000_000).await.unwrap();
    engine.collect_expired_holds(1);
    assert_eq!(engine.earliest_hold_expiry.load(Relaxed), 1_000_000);
    let gen0 = engine.hold_generation.load(Relaxed);

    // A later placement with an earlier expiry: watermark tracks it down, generation advances.
    engine.place_hold(Ulid::new(), rid, Span::new(200, 300), 500).await.unwrap();
    assert_eq!(engine.earliest_hold_expiry.load(Relaxed), 500);
    assert!(engine.hold_generation.load(Relaxed) > gen0);
}

#[tokio::test]
async fn batch_bookings_atomic_append_survives_replay() {
    // D2: the whole batch is persisted under one WAL append. A multi-resource batch must apply all
    // its bookings and, crucially, all of them must be durable (replay reconstructs every one).
    let path = test_wal_path("batch_atomic_replay.wal");
    let r1 = Ulid::new();
    let r2 = Ulid::new();
    let b1 = Ulid::new();
    let b2 = Ulid::new();
    let b3 = Ulid::new();
    {
        let engine = Engine::new(path.clone(), Arc::new(NotifyHub::new())).unwrap();
        engine.create_resource(r1, None, None, 2, None).await.unwrap();
        engine.create_resource(r2, None, None, 1, None).await.unwrap();
        engine
            .batch_confirm_bookings(vec![
                (b1, r1, Span::new(0, 100), Some("a".into())),
                (b2, r1, Span::new(0, 100), None),
                (b3, r2, Span::new(50, 150), None),
            ])
            .await
            .unwrap();
        assert_eq!(engine.get_bookings(r1).await.unwrap().len(), 2);
        assert_eq!(engine.get_bookings(r2).await.unwrap().len(), 1);
    }
    // Reopen: every booking from the single atomic append is durable.
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    let r1_books = engine.get_bookings(r1).await.unwrap();
    assert_eq!(r1_books.len(), 2);
    assert_eq!(engine.get_bookings(r2).await.unwrap().len(), 1);
    assert_eq!(engine.get_resource_for_entity(&b3), Some(r2));
}

// ══════════════════════════════════════════════════════════════
// Review batch C, ABBA deadlock removed via lock-free parent index
// ══════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ancestor_walk_and_batch_do_not_deadlock() {
    use tokio::time::{timeout, Duration};
    // Upward walks (availability / add_rule coverage) used to hold the descendant's guard while
    // awaiting each ancestor's guard, while batch_confirm_bookings locks in sorted (roughly
    // top-down) order, an ABBA cycle when a batch spans an ancestor+descendant concurrent with a
    // walk on the descendant. The parent index makes the walks lock-free, so this must complete.
    let engine = Arc::new(Engine::new(test_wal_path("abba.wal"), Arc::new(NotifyHub::new())).unwrap());
    let a = Ulid::new();
    let d = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.create_resource(d, Some(a), None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(0, 10_000_000), false).await.unwrap();
    engine.add_rule(Ulid::new(), d, Span::new(0, 10_000_000), false).await.unwrap();

    let work = async {
        for i in 0..100i64 {
            let e1 = engine.clone();
            let e2 = engine.clone();
            let base = i * 1000;
            // Batch touches BOTH ancestor and descendant (locks A then D in sorted order).
            let t1 = tokio::spawn(async move {
                let _ = e1
                    .batch_confirm_bookings(vec![
                        (Ulid::new(), a, Span::new(base, base + 10), None),
                        (Ulid::new(), d, Span::new(base, base + 10), None),
                    ])
                    .await;
            });
            // Concurrent upward walk on the descendant: availability and a covered rule add.
            let t2 = tokio::spawn(async move {
                let _ = e2.compute_availability(d, 0, 1_000_000, None).await;
                let _ = e2.add_rule(Ulid::new(), d, Span::new(base + 500, base + 600), false).await;
            });
            let _ = t1.await;
            let _ = t2.await;
        }
    };

    assert!(
        timeout(Duration::from_secs(30), work).await.is_ok(),
        "ancestor walk and batch deadlocked"
    );
}

// ══════════════════════════════════════════════════════════════
// Hold expiry authority (AVAIL-08): the server clock clamps client expiry
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn place_hold_clamps_far_future_expiry_to_default_cap() {
    // A client-supplied expires_at is a request, not an assignment: the server clamps it to
    // now + the max hold TTL so a skewed or hostile clock can never squat a span until year 3000.
    let path = test_wal_path("clamp_default_cap.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    engine
        .place_hold(Ulid::new(), rid, Span::new(1000, 2000), MAX_VALID_TIMESTAMP_MS)
        .await
        .unwrap();

    let holds = engine.get_holds(rid).await.unwrap();
    assert_eq!(holds.len(), 1);
    assert!(
        holds[0].expires_at <= now_ms() + 3_600_000,
        "expiry must be clamped to now + the default max hold TTL, got {}",
        holds[0].expires_at
    );
    assert!(holds[0].expires_at > now_ms(), "the clamped hold must still be live");
}

#[tokio::test]
async fn place_hold_clamp_uses_injected_clock_and_configured_cap() {
    // Deterministic clamp: with a TestClock at T and a 60s cap, any request past T + 60_000
    // stores exactly T + 60_000. The server clock, not the client's, decides the ceiling.
    let path = test_wal_path("clamp_configured_cap.wal");
    let notify = Arc::new(NotifyHub::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let engine = Engine::with_clock(path, notify, clock).unwrap().with_max_hold_ttl(60_000);
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    engine
        .place_hold(Ulid::new(), rid, Span::new(1000, 2000), 999_999_999_999)
        .await
        .unwrap();

    let holds = engine.get_holds(rid).await.unwrap();
    assert_eq!(holds[0].expires_at, 1_060_000);
}

#[tokio::test]
async fn place_hold_within_cap_keeps_requested_expiry() {
    // The clamp is a ceiling, not an assignment: a request under now + cap is stored verbatim,
    // so short checkout holds keep their exact client-chosen expiry.
    let path = test_wal_path("clamp_within_cap.wal");
    let notify = Arc::new(NotifyHub::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let engine = Engine::with_clock(path, notify, clock).unwrap().with_max_hold_ttl(60_000);
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    engine.place_hold(Ulid::new(), rid, Span::new(1000, 2000), 1_030_000).await.unwrap();

    let holds = engine.get_holds(rid).await.unwrap();
    assert_eq!(holds[0].expires_at, 1_030_000);
}

#[tokio::test]
async fn place_hold_still_rejects_out_of_range_expiry() {
    // Input validation is unchanged by the clamp: an absurd timestamp past year 3000 is still
    // rejected outright rather than silently clamped into acceptance.
    let path = test_wal_path("clamp_still_validates.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let result = engine
        .place_hold(Ulid::new(), rid, Span::new(1000, 2000), MAX_VALID_TIMESTAMP_MS + 1)
        .await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("timestamp out of range"))));
}

#[tokio::test]
async fn clamped_hold_expiry_survives_replay_verbatim() {
    // Replay applies HoldPlaced events as durably written: the clamped expiry is replayed
    // verbatim, never re-clamped against the (later) clock or a different configured cap.
    let path = test_wal_path("clamp_replay.wal");
    let rid = Ulid::new();
    {
        let clock = Arc::new(TestClock::new(1_000_000));
        let engine = Engine::with_clock(path.clone(), Arc::new(NotifyHub::new()), clock)
            .unwrap()
            .with_max_hold_ttl(60_000);
        engine.create_resource(rid, None, None, 1, None).await.unwrap();
        engine
            .place_hold(Ulid::new(), rid, Span::new(1000, 2000), 999_999_999_999)
            .await
            .unwrap();
        assert_eq!(engine.get_holds(rid).await.unwrap()[0].expires_at, 1_060_000);
    }

    let clock = Arc::new(TestClock::new(5_000_000));
    let engine = Engine::with_clock(path, Arc::new(NotifyHub::new()), clock)
        .unwrap()
        .with_max_hold_ttl(1);
    let holds = engine.get_holds(rid).await.unwrap();
    assert_eq!(holds.len(), 1);
    assert_eq!(holds[0].expires_at, 1_060_000);
}

#[tokio::test]
async fn formerly_unreapable_hold_expires_once_the_cap_passes() {
    // The point of the whole fix: a hold that requested a year-3000 expiry used to be
    // unreapable forever; clamped, the reaper collects it as soon as now passes the cap.
    let path = test_wal_path("clamp_reapable.wal");
    let clock = Arc::new(TestClock::new(1_000_000));
    let engine = Engine::with_clock(path, Arc::new(NotifyHub::new()), clock.clone())
        .unwrap()
        .with_max_hold_ttl(60_000);
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let hid = Ulid::new();
    engine.place_hold(hid, rid, Span::new(1000, 2000), MAX_VALID_TIMESTAMP_MS).await.unwrap();

    assert!(engine.collect_expired_holds(engine.now_ms()).is_empty(), "still live under the cap");

    clock.advance(60_001);
    let due = engine.collect_expired_holds(engine.now_ms());
    assert_eq!(due, vec![(hid, rid)]);
}
