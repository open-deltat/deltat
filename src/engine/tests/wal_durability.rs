use crate::engine::*;
use super::helpers::*;

// ── WAL compaction tests ──────────────────────────────────────

#[tokio::test]
async fn compact_wal_preserves_state() {
    let path = test_wal_path("compact_state.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path.clone(), notify.clone()).unwrap();

    // Build state with churn: create resources, add/remove rules, book/cancel
    let parent = Ulid::new();
    engine.create_resource(parent, None, Some("Building".into()), 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), parent, Span::new(0, 24 * H), false).await.unwrap();

    let child = Ulid::new();
    engine.create_resource(child, Some(parent), Some("Room A".into()), 3, Some(30 * M)).await.unwrap();

    // Add and remove some rules (churn)
    let temp_rule = Ulid::new();
    engine.add_rule(temp_rule, child, Span::new(0, 1000), false).await.unwrap();
    engine.remove_rule(temp_rule).await.unwrap();

    // Add a permanent rule
    let perm_rule = Ulid::new();
    engine.add_rule(perm_rule, child, Span::new(9 * H, 17 * H), false).await.unwrap();

    // Book and cancel (churn)
    let temp_booking = Ulid::new();
    engine.confirm_booking(temp_booking, child, Span::new(9 * H, 10 * H), None).await.unwrap();
    engine.cancel_booking(temp_booking).await.unwrap();

    // Permanent booking
    let perm_booking = Ulid::new();
    engine.confirm_booking(perm_booking, child, Span::new(14 * H, 15 * H), Some("Team Meeting".into())).await.unwrap();

    // Snapshot pre-compact state
    let resources_before = engine.list_resources().await;
    let rules_before = engine.get_rules(child).await.unwrap();
    let bookings_before = engine.get_bookings(child).await.unwrap();
    let avail_before = engine.compute_availability(child, 0, 24 * H, None).await.unwrap();

    // Get WAL size before compaction
    let size_before = std::fs::metadata(&path).unwrap().len();

    // Compact
    engine.compact_wal().await.unwrap();

    // WAL should be smaller (removed churn)
    let size_after = std::fs::metadata(&path).unwrap().len();
    assert!(size_after < size_before, "compacted WAL ({size_after}) should be smaller than original ({size_before})");

    // State should be identical
    let resources_after = engine.list_resources().await;
    assert_eq!(resources_before.len(), resources_after.len());

    let rules_after = engine.get_rules(child).await.unwrap();
    assert_eq!(rules_before.len(), rules_after.len());
    assert_eq!(rules_after[0].id, perm_rule);

    let bookings_after = engine.get_bookings(child).await.unwrap();
    assert_eq!(bookings_before.len(), bookings_after.len());
    assert_eq!(bookings_after[0].label, Some("Team Meeting".into()));

    let avail_after = engine.compute_availability(child, 0, 24 * H, None).await.unwrap();
    assert_eq!(avail_before, avail_after);
}

#[tokio::test]
async fn compact_wal_survives_restart() {
    let path = test_wal_path("compact_restart.wal");
    let notify = Arc::new(NotifyHub::new());

    let parent = Ulid::new();
    let child = Ulid::new();
    let booking_id = Ulid::new();
    let rule_id = Ulid::new();

    {
        let engine = Engine::new(path.clone(), notify.clone()).unwrap();
        engine.create_resource(parent, None, Some("Gym".into()), 1, None).await.unwrap();
        engine.add_rule(Ulid::new(), parent, Span::new(0, 24 * H), false).await.unwrap();
        engine.create_resource(child, Some(parent), Some("Treadmill 1".into()), 1, Some(10 * M)).await.unwrap();
        engine.add_rule(rule_id, child, Span::new(6 * H, 22 * H), false).await.unwrap();
        engine.confirm_booking(booking_id, child, Span::new(9 * H, 10 * H), Some("Alice".into())).await.unwrap();

        // Create churn
        for _ in 0..20 {
            let tmp = Ulid::new();
            engine.add_rule(tmp, child, Span::new(0, 100), false).await.unwrap();
            engine.remove_rule(tmp).await.unwrap();
        }

        // Compact
        engine.compact_wal().await.unwrap();

        // Append new event AFTER compaction
        engine.add_rule(Ulid::new(), child, Span::new(12 * H, 13 * H), true).await.unwrap();
    }

    // Restart from compacted WAL
    let engine2 = Engine::new(path, notify).unwrap();

    let resources = engine2.list_resources().await;
    assert_eq!(resources.len(), 2);
    let gym = resources.iter().find(|r| r.id == parent).unwrap();
    assert_eq!(gym.name, Some("Gym".into()));

    let treadmill = resources.iter().find(|r| r.id == child).unwrap();
    assert_eq!(treadmill.name, Some("Treadmill 1".into()));
    assert_eq!(treadmill.buffer_after, Some(10 * M));
    assert_eq!(treadmill.parent_id, Some(parent));

    let rules = engine2.get_rules(child).await.unwrap();
    assert_eq!(rules.len(), 2); // non-blocking + post-compact blocking

    let bookings = engine2.get_bookings(child).await.unwrap();
    assert_eq!(bookings.len(), 1);
    assert_eq!(bookings[0].id, booking_id);
    assert_eq!(bookings[0].label, Some("Alice".into()));
}

// ── Group-commit WAL tests ───────────────────────────────────

#[tokio::test]
async fn group_commit_batches_appends() {
    let path = test_wal_path("group_commit_batch.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path.clone(), notify.clone()).unwrap());

    let n = 20;
    let mut handles = Vec::new();
    for i in 0..n {
        let eng = engine.clone();
        handles.push(tokio::spawn(async move {
            eng.create_resource(Ulid::new(), None, Some(format!("R{i}")), 1, None)
                .await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert_eq!(engine.list_resources().await.len(), n);

    // Replay WAL from disk, should reconstruct the same N resources
    let engine2 = Engine::new(path, notify).unwrap();
    assert_eq!(engine2.list_resources().await.len(), n);
}

#[tokio::test]
async fn wal_appends_since_compact_through_channel() {
    let path = test_wal_path("appends_counter.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    assert_eq!(engine.wal_appends_since_compact().await, 0);

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let rule_id = Ulid::new();
    engine.add_rule(rule_id, rid, Span::new(0, 1000), false).await.unwrap();
    engine.remove_rule(rule_id).await.unwrap();

    assert_eq!(engine.wal_appends_since_compact().await, 3);
}

#[tokio::test]
async fn compact_resets_append_counter() {
    let path = test_wal_path("compact_counter.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 1000), false).await.unwrap();
    assert!(engine.wal_appends_since_compact().await > 0);

    engine.compact_wal().await.unwrap();
    assert_eq!(engine.wal_appends_since_compact().await, 0);
}

#[tokio::test]
async fn replay_includes_resource_deleted() {
    let path = test_wal_path("replay_delete.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path.clone(), notify).unwrap();

    let parent = Ulid::new();
    let child = Ulid::new();
    engine.create_resource(parent, None, Some("parent".into()), 1, None).await.unwrap();
    engine.create_resource(child, Some(parent), Some("child".into()), 1, None).await.unwrap();

    // Delete child, then verify replay handles ResourceDeleted + children cleanup
    engine.delete_resource(child).await.unwrap();
    assert!(engine.get_resource(&child).is_none());

    // Replay from WAL
    let notify2 = Arc::new(crate::notify::NotifyHub::new());
    let engine2 = Engine::new(path, notify2).unwrap();

    assert!(engine2.get_resource(&child).is_none());
    assert!(engine2.get_resource(&parent).is_some());
    // Parent should have no children after replay
    let resources = engine2.list_resources().await;
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].id, parent);
}
