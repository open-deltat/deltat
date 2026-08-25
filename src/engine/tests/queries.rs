use crate::engine::*;
use crate::clock::now_ms;
use super::helpers::*;

// ── Query method tests ────────────────────────────────────────

#[tokio::test]
async fn list_resources_returns_all() {
    let path = test_wal_path("list_resources.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    let b = Ulid::new();
    engine.create_resource(a, None, Some("Room A".into()), 2, Some(30 * M)).await.unwrap();
    engine.create_resource(b, Some(a), Some("Seat B".into()), 1, None).await.unwrap();

    let mut resources = engine.list_resources().await;
    resources.sort_by_key(|r| r.id);

    assert_eq!(resources.len(), 2);
    let ra = resources.iter().find(|r| r.id == a).unwrap();
    assert_eq!(ra.name, Some("Room A".into()));
    assert_eq!(ra.capacity, 2);
    assert_eq!(ra.buffer_after, Some(30 * M));
    assert_eq!(ra.parent_id, None);

    let rb = resources.iter().find(|r| r.id == b).unwrap();
    assert_eq!(rb.name, Some("Seat B".into()));
    assert_eq!(rb.parent_id, Some(a));
}

#[tokio::test]
async fn list_resources_empty() {
    let path = test_wal_path("list_resources_empty.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    assert!(engine.list_resources().await.is_empty());
}

#[tokio::test]
async fn list_resources_waits_for_a_locked_resource() {
    // Every mutation holds its resource write guard across the awaited WAL fsync, so under
    // ordinary write load SELECT * FROM resources races held locks constantly. A listing that
    // try_reads and skips silently returns an incomplete result with a success status, which a
    // caller cannot distinguish from the resource not existing. The listing must wait like the
    // sibling readers (get_rules/get_bookings/get_holds) do.
    use std::time::Duration;

    let path = test_wal_path("list_resources_locked.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Arc::new(Engine::new(path, notify).unwrap());

    let a = Ulid::new();
    let b = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.create_resource(b, None, None, 1, None).await.unwrap();

    let guard = engine.get_resource(&b).unwrap().write_owned().await;
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(guard);
    });

    let resources = engine.list_resources().await;
    releaser.await.unwrap();

    assert_eq!(resources.len(), 2, "a locked resource must not vanish from the listing");
}

#[tokio::test]
async fn get_rules_for_resource() {
    let path = test_wal_path("get_rules.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let r1 = Ulid::new();
    let r2 = Ulid::new();
    engine.add_rule(r1, rid, Span::new(9 * H, 17 * H), false).await.unwrap();
    engine.add_rule(r2, rid, Span::new(12 * H, 13 * H), true).await.unwrap();

    let rules = engine.get_rules(rid).await.unwrap();
    assert_eq!(rules.len(), 2);

    let nb = rules.iter().find(|r| r.id == r1).unwrap();
    assert!(!nb.blocking);
    assert_eq!(nb.start, 9 * H);
    assert_eq!(nb.end, 17 * H);

    let bl = rules.iter().find(|r| r.id == r2).unwrap();
    assert!(bl.blocking);
    assert_eq!(bl.start, 12 * H);
}

#[tokio::test]
async fn get_rules_not_found() {
    let path = test_wal_path("get_rules_notfound.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rules = engine.get_rules(Ulid::new()).await.unwrap();
    assert!(rules.is_empty());
}

#[tokio::test]
async fn get_bookings_for_resource() {
    let path = test_wal_path("get_bookings.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let b1 = Ulid::new();
    let b2 = Ulid::new();
    engine.confirm_booking(b1, rid, Span::new(9 * H, 10 * H), Some("Alice".into())).await.unwrap();
    engine.confirm_booking(b2, rid, Span::new(14 * H, 15 * H), None).await.unwrap();

    let bookings = engine.get_bookings(rid).await.unwrap();
    assert_eq!(bookings.len(), 2);

    let ba = bookings.iter().find(|b| b.id == b1).unwrap();
    assert_eq!(ba.label, Some("Alice".into()));
    assert_eq!(ba.start, 9 * H);

    let bb = bookings.iter().find(|b| b.id == b2).unwrap();
    assert_eq!(bb.label, None);
}

#[tokio::test]
async fn get_bookings_excludes_cancelled() {
    let path = test_wal_path("get_bookings_cancel.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let bid = Ulid::new();
    engine.confirm_booking(bid, rid, Span::new(9 * H, 10 * H), None).await.unwrap();
    engine.cancel_booking(bid).await.unwrap();

    let bookings = engine.get_bookings(rid).await.unwrap();
    assert!(bookings.is_empty());
}

#[tokio::test]
async fn get_holds_for_resource() {
    let path = test_wal_path("get_holds.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let far_future = now_ms() + 3_600_000;
    let hid = Ulid::new();
    engine.place_hold(hid, rid, Span::new(9 * H, 10 * H), far_future).await.unwrap();

    let holds = engine.get_holds(rid).await.unwrap();
    assert_eq!(holds.len(), 1);
    assert_eq!(holds[0].id, hid);
    assert_eq!(holds[0].expires_at, far_future);
}

// ── Update method tests ────────────────────────────────────

#[tokio::test]
async fn update_resource_changes_fields() {
    let path = test_wal_path("update_resource.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, Some("Old Name".into()), 1, None).await.unwrap();

    engine.update_resource(rid, Some(Some("New Name".into())), Some(3), Some(Some(15 * M))).await.unwrap();

    let resources = engine.list_resources().await;
    let r = resources.iter().find(|r| r.id == rid).unwrap();
    assert_eq!(r.name, Some("New Name".into()));
    assert_eq!(r.capacity, 3);
    assert_eq!(r.buffer_after, Some(15 * M));
}

#[tokio::test]
async fn update_resource_partial_leaves_other_fields_intact() {
    // A partial update that mentions only buffer_after must not wipe name or capacity. The parser
    // sends None for the omitted columns and the apply arm skips them.
    let path = test_wal_path("update_resource_partial.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, Some("Room A".into()), 4, Some(5 * M)).await.unwrap();

    // Only buffer_after is present; name and capacity are absent (None).
    engine.update_resource(rid, None, None, Some(Some(30 * M))).await.unwrap();

    let resources = engine.list_resources().await;
    let r = resources.iter().find(|r| r.id == rid).unwrap();
    assert_eq!(r.name, Some("Room A".into()), "name must be unchanged");
    assert_eq!(r.capacity, 4, "capacity must be unchanged");
    assert_eq!(r.buffer_after, Some(30 * M), "buffer_after must be updated");

    // Setting name to NULL explicitly (Some(None)) clears it, distinct from leaving it absent.
    engine.update_resource(rid, Some(None), None, None).await.unwrap();
    let resources = engine.list_resources().await;
    let r = resources.iter().find(|r| r.id == rid).unwrap();
    assert_eq!(r.name, None, "explicit SET name = NULL clears the name");
    assert_eq!(r.capacity, 4, "capacity still unchanged");
    assert_eq!(r.buffer_after, Some(30 * M), "buffer_after still unchanged");
}

#[tokio::test]
async fn update_resource_not_found() {
    let path = test_wal_path("update_resource_notfound.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    assert!(matches!(
        engine.update_resource(Ulid::new(), None, None, None).await,
        Err(EngineError::NotFound(_))
    ));
}

#[tokio::test]
async fn update_resource_persists_via_wal() {
    let path = test_wal_path("update_resource_wal.wal");
    let notify = Arc::new(NotifyHub::new());

    let rid = Ulid::new();
    {
        let engine = Engine::new(path.clone(), notify.clone()).unwrap();
        engine.create_resource(rid, None, Some("Before".into()), 1, None).await.unwrap();
        engine.update_resource(rid, Some(Some("After".into())), Some(5), Some(Some(H))).await.unwrap();
    }

    // Replay from WAL
    let engine2 = Engine::new(path, notify).unwrap();
    let resources = engine2.list_resources().await;
    let r = resources.iter().find(|r| r.id == rid).unwrap();
    assert_eq!(r.name, Some("After".into()));
    assert_eq!(r.capacity, 5);
    assert_eq!(r.buffer_after, Some(H));
}

#[tokio::test]
async fn update_rule_changes_span_and_blocking() {
    let path = test_wal_path("update_rule.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let rule_id = Ulid::new();
    engine.add_rule(rule_id, rid, Span::new(9 * H, 17 * H), false).await.unwrap();

    // Update: narrow the window and make it blocking
    engine.update_rule(rule_id, Span::new(10 * H, 16 * H), true).await.unwrap();

    let rules = engine.get_rules(rid).await.unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].id, rule_id);
    assert_eq!(rules[0].start, 10 * H);
    assert_eq!(rules[0].end, 16 * H);
    assert!(rules[0].blocking);
}

#[tokio::test]
async fn update_rule_not_found() {
    let path = test_wal_path("update_rule_notfound.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    assert!(matches!(
        engine.update_rule(Ulid::new(), Span::new(0, 1000), false).await,
        Err(EngineError::NotFound(_))
    ));
}

#[tokio::test]
async fn update_rule_persists_via_wal() {
    let path = test_wal_path("update_rule_wal.wal");
    let notify = Arc::new(NotifyHub::new());

    let rid = Ulid::new();
    let rule_id = Ulid::new();
    {
        let engine = Engine::new(path.clone(), notify.clone()).unwrap();
        engine.create_resource(rid, None, None, 1, None).await.unwrap();
        engine.add_rule(rule_id, rid, Span::new(9 * H, 17 * H), false).await.unwrap();
        engine.update_rule(rule_id, Span::new(8 * H, 20 * H), true).await.unwrap();
    }

    let engine2 = Engine::new(path, notify).unwrap();
    let rules = engine2.get_rules(rid).await.unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].start, 8 * H);
    assert_eq!(rules[0].end, 20 * H);
    assert!(rules[0].blocking);
}

#[tokio::test]
async fn booking_label_preserved() {
    let path = test_wal_path("booking_label.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let bid = Ulid::new();
    engine.confirm_booking(bid, rid, Span::new(9 * H, 10 * H), Some("VIP Guest".into())).await.unwrap();

    let bookings = engine.get_bookings(rid).await.unwrap();
    assert_eq!(bookings[0].label, Some("VIP Guest".into()));
}

#[tokio::test]
async fn booking_label_persists_via_wal() {
    let path = test_wal_path("booking_label_wal.wal");
    let notify = Arc::new(NotifyHub::new());

    let rid = Ulid::new();
    let bid = Ulid::new();
    {
        let engine = Engine::new(path.clone(), notify.clone()).unwrap();
        engine.create_resource(rid, None, None, 1, None).await.unwrap();
        engine.confirm_booking(bid, rid, Span::new(9 * H, 10 * H), Some("Replay Test".into())).await.unwrap();
    }

    let engine2 = Engine::new(path, notify).unwrap();
    let bookings = engine2.get_bookings(rid).await.unwrap();
    assert_eq!(bookings.len(), 1);
    assert_eq!(bookings[0].label, Some("Replay Test".into()));
}

#[tokio::test]
async fn resource_name_preserved_after_create() {
    let path = test_wal_path("resource_name.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, Some("Theater".into()), 1, None).await.unwrap();

    let resources = engine.list_resources().await;
    assert_eq!(resources[0].name, Some("Theater".into()));
}

#[tokio::test]
async fn resource_name_persists_via_wal() {
    let path = test_wal_path("resource_name_wal.wal");
    let notify = Arc::new(NotifyHub::new());

    let rid = Ulid::new();
    {
        let engine = Engine::new(path.clone(), notify.clone()).unwrap();
        engine.create_resource(rid, None, Some("Stadium".into()), 50, None).await.unwrap();
    }

    let engine2 = Engine::new(path, notify).unwrap();
    let resources = engine2.list_resources().await;
    let r = resources.iter().find(|r| r.id == rid).unwrap();
    assert_eq!(r.name, Some("Stadium".into()));
    assert_eq!(r.capacity, 50);
}

#[tokio::test]
async fn update_rule_blocking_to_non_blocking() {
    let path = test_wal_path("update_rule_to_nb.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let rule_id = Ulid::new();
    engine.add_rule(rule_id, rid, Span::new(9 * H, 17 * H), true).await.unwrap();

    // Update: make it non-blocking (covers RuleUpdated non-blocking branch in apply_event)
    engine.update_rule(rule_id, Span::new(10 * H, 16 * H), false).await.unwrap();

    let rules = engine.get_rules(rid).await.unwrap();
    assert_eq!(rules.len(), 1);
    assert!(!rules[0].blocking);
    assert_eq!(rules[0].start, 10 * H);
    assert_eq!(rules[0].end, 16 * H);
}
