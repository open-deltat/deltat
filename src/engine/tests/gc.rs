use crate::engine::*;
use super::helpers::*;

// ── GC tests ─────────────────────────────────────────────

#[tokio::test]
async fn gc_removes_past_bookings() {
    let path = test_wal_path("gc_past_bookings.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(1000, 50000), false).await.unwrap();

    let bid = Ulid::new();
    engine.confirm_booking(bid, rid, Span::new(1000, 2000), None).await.unwrap();

    // now=10000, retention=5000 → cutoff=5000 → booking ends at 2000 < 5000 → collected
    let collected = engine.gc_past_intervals(10000, 5000);
    assert_eq!(collected, 1);

    let bookings = engine.get_bookings(rid).await.unwrap();
    assert!(bookings.is_empty());
}

#[tokio::test]
async fn gc_keeps_future_bookings() {
    let path = test_wal_path("gc_future_bookings.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(1000, 50000), false).await.unwrap();

    let bid = Ulid::new();
    engine.confirm_booking(bid, rid, Span::new(8000, 9000), None).await.unwrap();

    // now=10000, retention=5000 → cutoff=5000 → booking ends at 9000 > 5000 → kept
    let collected = engine.gc_past_intervals(10000, 5000);
    assert_eq!(collected, 0);

    let bookings = engine.get_bookings(rid).await.unwrap();
    assert_eq!(bookings.len(), 1);
}

#[tokio::test]
async fn gc_keeps_booking_whose_buffer_tail_blocks_the_present() {
    // A booking occupies [start, end + buffer_after) on both the read and the write path, so its
    // blocking effect can outlive its raw span by up to the buffer. GC that tests only span.end
    // against the cutoff collects it while the turnaround window is still active, silently
    // opening time that admission rejected a moment earlier.
    let path = test_wal_path("gc_buffer_tail.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    // Turnaround buffer of 24000ms, far larger than the 5000ms retention.
    engine.create_resource(rid, None, None, 1, Some(24000)).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(1000, 50000), false).await.unwrap();

    let bid = Ulid::new();
    engine.confirm_booking(bid, rid, Span::new(1000, 2000), None).await.unwrap();

    // A booking inside the turnaround window [2000, 26000) is rejected before GC runs.
    let probe = Span::new(20000, 21000);
    assert!(engine.confirm_booking(Ulid::new(), rid, probe, None).await.is_err());

    // now=10000, retention=5000 → cutoff=5000. Raw end 2000 < 5000, but the buffered end
    // 26000 still blocks the present: the booking must survive.
    let collected = engine.gc_past_intervals(10000, 5000);
    assert_eq!(collected, 0, "booking with a live buffer tail was collected");

    // The turnaround invariant holds across GC: the same probe is still rejected.
    assert!(
        engine.confirm_booking(Ulid::new(), rid, probe, None).await.is_err(),
        "GC opened the still-active turnaround window"
    );

    // Once the buffered end passes the cutoff too, the booking is collectable.
    let collected = engine.gc_past_intervals(32000, 5000);
    assert_eq!(collected, 1);
}

#[tokio::test]
async fn gc_keeps_rules() {
    let path = test_wal_path("gc_keeps_rules.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let rule_id = Ulid::new();
    engine.add_rule(rule_id, rid, Span::new(1000, 2000), false).await.unwrap();

    // Even with cutoff way past the rule's end, rules are never collected
    let collected = engine.gc_past_intervals(100000, 1000);
    assert_eq!(collected, 0);

    let rules = engine.get_rules(rid).await.unwrap();
    assert_eq!(rules.len(), 1);
}

#[tokio::test]
async fn gc_removes_expired_past_holds() {
    let path = test_wal_path("gc_expired_holds.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(1000, 50000), false).await.unwrap();

    let hid = Ulid::new();
    // Hold span [1000, 2000), expires_at=3000
    engine.place_hold(hid, rid, Span::new(1000, 2000), 3000).await.unwrap();

    // now=10000, retention=5000 → cutoff=5000
    // expires_at=3000 <= now=10000 AND span.end=2000 < cutoff=5000 → collected
    let collected = engine.gc_past_intervals(10000, 5000);
    assert_eq!(collected, 1);

    let holds = engine.get_holds(rid).await.unwrap();
    assert!(holds.is_empty());
}

#[tokio::test]
async fn gc_keeps_active_holds() {
    let path = test_wal_path("gc_active_holds.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(1000, 50000), false).await.unwrap();

    let hid = Ulid::new();
    // Hold span [1000, 2000), expires_at=99999 (still active)
    engine.place_hold(hid, rid, Span::new(1000, 2000), 99999).await.unwrap();

    // now=10000 → expires_at=99999 > now → NOT expired → kept even though span is past cutoff
    let collected = engine.gc_past_intervals(10000, 5000);
    assert_eq!(collected, 0);

    let holds = engine.get_holds(rid).await.unwrap();
    assert_eq!(holds.len(), 1);
}

#[tokio::test]
async fn gc_cleans_entity_index() {
    let path = test_wal_path("gc_entity_index.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(1000, 50000), false).await.unwrap();

    let bid = Ulid::new();
    engine.confirm_booking(bid, rid, Span::new(1000, 2000), None).await.unwrap();

    // Verify entity index has the booking
    assert!(engine.get_resource_for_entity(&bid).is_some());

    let collected = engine.gc_past_intervals(10000, 5000);
    assert_eq!(collected, 1);

    // Entity index should be cleaned up
    assert!(engine.get_resource_for_entity(&bid).is_none());
}

#[tokio::test]
async fn gc_compact_roundtrip() {
    let path = test_wal_path("gc_compact_roundtrip.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path.clone(), notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(1000, 50000), false).await.unwrap();

    let old_bid = Ulid::new();
    let new_bid = Ulid::new();
    engine.confirm_booking(old_bid, rid, Span::new(1000, 2000), Some("old".into())).await.unwrap();
    engine.confirm_booking(new_bid, rid, Span::new(20000, 30000), Some("new".into())).await.unwrap();

    // GC removes old booking
    let collected = engine.gc_past_intervals(10000, 5000);
    assert_eq!(collected, 1);

    // Compact WAL
    engine.compact_wal().await.unwrap();

    // Replay from WAL, old booking should not reappear
    let notify2 = Arc::new(crate::notify::NotifyHub::new());
    let engine2 = Engine::new(path, notify2).unwrap();

    let bookings = engine2.get_bookings(rid).await.unwrap();
    assert_eq!(bookings.len(), 1);
    assert_eq!(bookings[0].label, Some("new".into()));
    assert!(engine2.get_resource_for_entity(&old_bid).is_none());
}

#[tokio::test]
async fn gc_on_empty_resource() {
    let path = test_wal_path("gc_empty_resource.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let collected = engine.gc_past_intervals(10000, 5000);
    assert_eq!(collected, 0);
}

#[tokio::test]
async fn gc_mixed_intervals_selective() {
    let path = test_wal_path("gc_mixed.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 10, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(1000, 100000), false).await.unwrap();

    // Old booking (should be collected)
    let old = Ulid::new();
    engine.confirm_booking(old, rid, Span::new(1000, 2000), None).await.unwrap();
    // Recent booking (should stay)
    let recent = Ulid::new();
    engine.confirm_booking(recent, rid, Span::new(8000, 9000), None).await.unwrap();
    // Future booking (should stay)
    let future = Ulid::new();
    engine.confirm_booking(future, rid, Span::new(20000, 30000), None).await.unwrap();
    // Old expired hold (should be collected)
    let old_hold = Ulid::new();
    engine.place_hold(old_hold, rid, Span::new(3000, 4000), 5000).await.unwrap();
    // Rule (should never be collected)
    let rule = Ulid::new();
    engine.add_rule(rule, rid, Span::new(1000, 2000), true).await.unwrap();

    // now=10000, retention=5000 → cutoff=5000
    let collected = engine.gc_past_intervals(10000, 5000);
    assert_eq!(collected, 2); // old booking + old expired hold

    let bookings = engine.get_bookings(rid).await.unwrap();
    assert_eq!(bookings.len(), 2);
    let holds = engine.get_holds(rid).await.unwrap();
    assert!(holds.is_empty());
    let rules = engine.get_rules(rid).await.unwrap();
    assert_eq!(rules.len(), 2); // original non-blocking + blocking rule
}
