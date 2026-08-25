use crate::engine::*;
use super::helpers::*;

// Hierarchy deep tests
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn engine_four_level_hierarchy() {
    // Chain → Hotel → Floor → Room
    let path = test_wal_path("four_level.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let chain = Ulid::new();
    engine.create_resource(chain, None, None, 1, None).await.unwrap();
    // Chain open 24/7
    engine
        .add_rule(Ulid::new(), chain, Span::new(0, 24 * H), false)
        .await
        .unwrap();

    let hotel = Ulid::new();
    engine
        .create_resource(hotel, Some(chain), None, 1, None)
        .await
        .unwrap();
    // Hotel-level maintenance 3am-5am
    engine
        .add_rule(Ulid::new(), hotel, Span::new(3 * H, 5 * H), true)
        .await
        .unwrap();

    let floor = Ulid::new();
    engine
        .create_resource(floor, Some(hotel), None, 1, None)
        .await
        .unwrap();
    // Floor-level: no own rules, inherits chain's 24/7

    let room = Ulid::new();
    engine
        .create_resource(room, Some(floor), None, 1, None)
        .await
        .unwrap();

    let avail = engine
        .compute_availability(room, 0, 24 * H, None)
        .await
        .unwrap();
    // Room inherits chain 24/7 (through floor, hotel), minus hotel blocking [3,5)
    assert_eq!(
        avail,
        vec![Span::new(0, 3 * H), Span::new(5 * H, 24 * H)]
    );
}

#[tokio::test]
async fn engine_grandparent_non_blocking_skips_empty_parent() {
    // Grandparent has non-blocking, parent has none → grandparent's rules used
    let path = test_wal_path("grandparent_skip.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let grandparent = Ulid::new();
    engine.create_resource(grandparent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), grandparent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let parent = Ulid::new();
    engine
        .create_resource(parent, Some(grandparent), None, 1, None)
        .await
        .unwrap();
    // Parent has only blocking, no non-blocking
    engine
        .add_rule(Ulid::new(), parent, Span::new(12 * H, 13 * H), true)
        .await
        .unwrap();

    let child = Ulid::new();
    engine
        .create_resource(child, Some(parent), None, 1, None)
        .await
        .unwrap();

    let avail = engine
        .compute_availability(child, 0, 24 * H, None)
        .await
        .unwrap();
    // Non-blocking from grandparent [9,17), blocking from parent [12,13)
    assert_eq!(
        avail,
        vec![Span::new(9 * H, 12 * H), Span::new(13 * H, 17 * H)]
    );
}

#[tokio::test]
async fn engine_sibling_independence() {
    // Two children of same parent, booking on one doesn't affect the other
    let path = test_wal_path("sibling_independence.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), parent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let child_a = Ulid::new();
    let child_b = Ulid::new();
    engine
        .create_resource(child_a, Some(parent), None, 1, None)
        .await
        .unwrap();
    engine
        .create_resource(child_b, Some(parent), None, 1, None)
        .await
        .unwrap();

    // Book child_a solid 9-5
    engine
        .confirm_booking(Ulid::new(), child_a, Span::new(9 * H, 17 * H), None)
        .await
        .unwrap();

    // child_a should have zero availability
    let avail_a = engine
        .compute_availability(child_a, 0, 24 * H, None)
        .await
        .unwrap();
    assert!(avail_a.is_empty());

    // child_b should still have full 9-5
    let avail_b = engine
        .compute_availability(child_b, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(avail_b, vec![Span::new(9 * H, 17 * H)]);
}

#[tokio::test]
async fn engine_parent_blocking_after_child_booking() {
    // Child has a booking, then parent adds blocking that overlaps it.
    // The availability should reflect the blocking even though booking was placed first.
    let path = test_wal_path("parent_block_after_book.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), parent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let child = Ulid::new();
    engine
        .create_resource(child, Some(parent), None, 1, None)
        .await
        .unwrap();

    // Book child at 10-11
    engine
        .confirm_booking(Ulid::new(), child, Span::new(10 * H, 11 * H), None)
        .await
        .unwrap();

    // Now parent blocks 14-15 (emergency)
    engine
        .add_rule(Ulid::new(), parent, Span::new(14 * H, 15 * H), true)
        .await
        .unwrap();

    let avail = engine
        .compute_availability(child, 0, 24 * H, None)
        .await
        .unwrap();
    // Base [9,17) minus booking [10,11) minus parent blocking [14,15)
    assert_eq!(
        avail,
        vec![
            Span::new(9 * H, 10 * H),
            Span::new(11 * H, 14 * H),
            Span::new(15 * H, 17 * H),
        ]
    );
}

#[tokio::test]
async fn engine_child_inherits_updated_parent_rules() {
    // Parent adds a second non-blocking rule after child exists
    let path = test_wal_path("updated_parent_rules.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), parent, Span::new(9 * H, 12 * H), false)
        .await
        .unwrap();

    let child = Ulid::new();
    engine
        .create_resource(child, Some(parent), None, 1, None)
        .await
        .unwrap();

    let avail1 = engine
        .compute_availability(child, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(avail1, vec![Span::new(9 * H, 12 * H)]);

    // Parent adds afternoon availability
    engine
        .add_rule(Ulid::new(), parent, Span::new(14 * H, 17 * H), false)
        .await
        .unwrap();

    let avail2 = engine
        .compute_availability(child, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(
        avail2,
        vec![Span::new(9 * H, 12 * H), Span::new(14 * H, 17 * H)]
    );
}

#[tokio::test]
async fn override_is_window_independent_across_query_bounds() {
    // Audit finding: the own-vs-inherited OVERRIDE was decided per query window, so the same
    // instant read open in a narrow query (child's rule outside the window, inherited base wins)
    // and closed in a wide one (child's rule overlaps, own base wins). The override must be a
    // state fact: a child that defines its own schedule is closed outside it in EVERY window.
    let path = test_wal_path("override_window_independent.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let day = 24 * H;

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), parent, Span::new(0, 7 * day), false)
        .await
        .unwrap();

    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();
    // Child's own schedule: Monday 9-17 only.
    engine
        .add_rule(Ulid::new(), child, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    // Wide query (covers the child's rule): Tuesday is closed.
    let wide = engine.compute_availability(child, 0, 2 * day, None).await.unwrap();
    assert_eq!(wide, vec![Span::new(9 * H, 17 * H)]);

    // Narrow query (Tuesday only, child's rule outside the window): Tuesday must STILL be closed.
    let narrow = engine.compute_availability(child, day, 2 * day, None).await.unwrap();
    assert!(
        narrow.is_empty(),
        "Tue 10:00 flipped open in the narrow window: {narrow:?}"
    );
}

#[tokio::test]
async fn child_rule_outside_parents_own_schedule_is_rejected() {
    // AVAIL-09 through the same window-scoped override bug: the parent-coverage check evaluated
    // the parent's availability over the rule's span, where the parent's own schedule did not
    // overlap, so the check fell through to the grandparent's open time and admitted a child rule
    // the parent's schedule forbids.
    let path = test_wal_path("coverage_window_independent.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let grandparent = Ulid::new();
    engine.create_resource(grandparent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), grandparent, Span::new(0, 24 * H), false)
        .await
        .unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, Some(grandparent), None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), parent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();
    let result = engine
        .add_rule(Ulid::new(), child, Span::new(18 * H, 20 * H), false)
        .await;
    assert!(
        matches!(result, Err(EngineError::NotCoveredByParent { .. })),
        "parent is closed [18,20): the child rule must be rejected, got {result:?}"
    );
}

#[tokio::test]
async fn engine_delete_child_then_parent() {
    let path = test_wal_path("delete_child_parent.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    // Can't delete parent while child exists
    assert!(matches!(
        engine.delete_resource(parent).await,
        Err(EngineError::HasChildren(_))
    ));

    // Delete child first, then parent succeeds
    engine.delete_resource(child).await.unwrap();
    engine.delete_resource(parent).await.unwrap();

    assert!(engine.get_resource(&parent).is_none());
    assert!(engine.get_resource(&child).is_none());
}

#[tokio::test]
async fn engine_children_index_rebuilt_on_replay() {
    let path = test_wal_path("children_index_replay.wal");
    let notify = Arc::new(NotifyHub::new());

    let parent = Ulid::new();
    let child = Ulid::new();
    {
        let engine = Engine::new(path.clone(), notify.clone()).unwrap();
        engine.create_resource(parent, None, None, 1, None).await.unwrap();
        engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();
    }

    // Replay from WAL, children index should be rebuilt
    let engine2 = Engine::new(path, notify).unwrap();
    // Verify by trying to delete parent (should fail because child exists)
    assert!(matches!(
        engine2.delete_resource(parent).await,
        Err(EngineError::HasChildren(_))
    ));
}

#[tokio::test]
async fn engine_multiple_blocking_from_different_ancestors() {
    // Blocking rules from multiple ancestor levels should all accumulate
    let path = test_wal_path("multi_ancestor_blocking.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let grandparent = Ulid::new();
    engine.create_resource(grandparent, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), grandparent, Span::new(0, 24 * H), false)
        .await
        .unwrap();
    // Grandparent blocks 2am-3am
    engine
        .add_rule(Ulid::new(), grandparent, Span::new(2 * H, 3 * H), true)
        .await
        .unwrap();

    let parent = Ulid::new();
    engine
        .create_resource(parent, Some(grandparent), None, 1, None)
        .await
        .unwrap();
    // Parent blocks 5am-6am
    engine
        .add_rule(Ulid::new(), parent, Span::new(5 * H, 6 * H), true)
        .await
        .unwrap();

    let child = Ulid::new();
    engine
        .create_resource(child, Some(parent), None, 1, None)
        .await
        .unwrap();

    let avail = engine
        .compute_availability(child, 0, 8 * H, None)
        .await
        .unwrap();
    // Base 24h from grandparent, minus [2,3) from grandparent, minus [5,6) from parent
    assert_eq!(
        avail,
        vec![
            Span::new(0, 2 * H),
            Span::new(3 * H, 5 * H),
            Span::new(6 * H, 8 * H),
        ]
    );
}


// ══════════════════════════════════════════════════════════════
// Edge case: rule removal and re-query
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn engine_remove_parent_rule_affects_children() {
    let path = test_wal_path("remove_parent_rule.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    let rule_id = Ulid::new();
    engine
        .add_rule(rule_id, parent, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let child = Ulid::new();
    engine
        .create_resource(child, Some(parent), None, 1, None)
        .await
        .unwrap();

    // Child has availability from parent
    let avail = engine
        .compute_availability(child, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(avail, vec![Span::new(9 * H, 17 * H)]);

    // Remove parent's rule
    engine.remove_rule(rule_id).await.unwrap();

    // Child now has zero availability
    let avail_after = engine
        .compute_availability(child, 0, 24 * H, None)
        .await
        .unwrap();
    assert!(avail_after.is_empty());
}

#[tokio::test]
async fn engine_remove_blocking_restores_availability() {
    let path = test_wal_path("remove_blocking.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), rid, Span::new(9 * H, 17 * H), false)
        .await
        .unwrap();

    let block_id = Ulid::new();
    engine
        .add_rule(block_id, rid, Span::new(12 * H, 13 * H), true)
        .await
        .unwrap();

    let avail = engine
        .compute_availability(rid, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(avail.len(), 2); // split by blocking

    // Remove blocking
    engine.remove_rule(block_id).await.unwrap();

    let avail_after = engine
        .compute_availability(rid, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(avail_after, vec![Span::new(9 * H, 17 * H)]); // restored
}
