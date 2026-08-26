use crate::engine::*;
use super::helpers::*;

// ══════════════════════════════════════════════════════════════
// Multi-resource availability: comprehensive edge case coverage
// ══════════════════════════════════════════════════════════════

// ── Basic operations ──────────────────────────────────────────

#[tokio::test]
async fn multi_avail_intersection() {
    // Two independent resources: mechanic + plane.
    // Intersection = when BOTH are free.
    let path = test_wal_path("multi_avail_intersect.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let mechanic = Ulid::new();
    engine.create_resource(mechanic, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), mechanic, Span::new(8 * H, 16 * H), false).await.unwrap();

    let plane = Ulid::new();
    engine.create_resource(plane, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), plane, Span::new(6 * H, 24 * H), false).await.unwrap();
    engine.confirm_booking(Ulid::new(), plane, Span::new(10 * H, 13 * H), None).await.unwrap();

    // Mechanic: [8,16). Plane: [6,10) ∪ [13,24). Overlap: [8,10) ∪ [13,16)
    let result = engine
        .compute_multi_availability(&[mechanic, plane], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![
        Span::new(8 * H, 10 * H),
        Span::new(13 * H, 16 * H),
    ]);
}

#[tokio::test]
async fn multi_avail_union_pool() {
    // Three mechanics, need ANY one free (pool scheduling).
    let path = test_wal_path("multi_avail_pool.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let m1 = Ulid::new();
    engine.create_resource(m1, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), m1, Span::new(8 * H, 12 * H), false).await.unwrap();

    let m2 = Ulid::new();
    engine.create_resource(m2, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), m2, Span::new(11 * H, 16 * H), false).await.unwrap();

    let m3 = Ulid::new();
    engine.create_resource(m3, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), m3, Span::new(15 * H, 20 * H), false).await.unwrap();

    // Union (min_available = 1): [8,20) (continuous coverage)
    let result = engine
        .compute_multi_availability(&[m1, m2, m3], 0, 24 * H, 1, None)
        .await
        .unwrap();
    assert_eq!(result, vec![Span::new(8 * H, 20 * H)]);

    // At-least-2: [11,12) (m1+m2) ∪ [15,16) (m2+m3)
    let result2 = engine
        .compute_multi_availability(&[m1, m2, m3], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result2, vec![
        Span::new(11 * H, 12 * H),
        Span::new(15 * H, 16 * H),
    ]);

    // At-least-3 (ALL): no time when all 3 overlap
    let result3 = engine
        .compute_multi_availability(&[m1, m2, m3], 0, 24 * H, 3, None)
        .await
        .unwrap();
    assert!(result3.is_empty());
}

#[tokio::test]
async fn multi_avail_merges_adjacent_coverage_before_min_duration() {
    // GAP-13 regression: when coverage of one continuous window is handed off
    // between resources at a shared half-open boundary (r1 free [8,12), r2 free
    // [12,16)), the sweep emits two adjacent segments. They must be merged before
    // the min_duration filter, or a genuinely continuous 8h window gets split into
    // two 4h fragments and dropped, hiding a real slot.
    let path = test_wal_path("multi_avail_gap13.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let r1 = Ulid::new();
    engine.create_resource(r1, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), r1, Span::new(8 * H, 12 * H), false).await.unwrap();

    let r2 = Ulid::new();
    engine.create_resource(r2, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), r2, Span::new(12 * H, 16 * H), false).await.unwrap();

    // Union is one continuous window [8,16); output must be merged, not fragmented.
    let union = engine
        .compute_multi_availability(&[r1, r2], 0, 24 * H, 1, None)
        .await
        .unwrap();
    assert_eq!(union, vec![Span::new(8 * H, 16 * H)]);

    // The continuous 8h window must survive a 6h minimum (the bug returned []).
    let filtered = engine
        .compute_multi_availability(&[r1, r2], 0, 24 * H, 1, Some(6 * H))
        .await
        .unwrap();
    assert_eq!(filtered, vec![Span::new(8 * H, 16 * H)]);

    // k-of-N variant: r3 spans the whole window so "at least 2" is continuous
    // across the r1→r2 handoff at 12h.
    let r3 = Ulid::new();
    engine.create_resource(r3, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), r3, Span::new(8 * H, 16 * H), false).await.unwrap();

    let two_of_three = engine
        .compute_multi_availability(&[r1, r2, r3], 0, 24 * H, 2, Some(6 * H))
        .await
        .unwrap();
    assert_eq!(two_of_three, vec![Span::new(8 * H, 16 * H)]);
}

#[tokio::test]
async fn multi_avail_with_min_duration() {
    let path = test_wal_path("multi_avail_mindur.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(8 * H, 17 * H), false).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(8 * H, 17 * H), false).await.unwrap();
    engine.confirm_booking(Ulid::new(), b, Span::new(10 * H, 15 * H), None).await.unwrap();

    // Intersection: [8,10) = 2h, [15,17) = 2h
    let all = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(all, vec![Span::new(8 * H, 10 * H), Span::new(15 * H, 17 * H)]);

    // min_duration = 3h: both too short
    let filtered = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, Some(3 * H))
        .await
        .unwrap();
    assert!(filtered.is_empty());

    // min_duration = 2h: both exactly qualify
    let passes = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, Some(2 * H))
        .await
        .unwrap();
    assert_eq!(passes.len(), 2);

    // min_duration = 2h + 1ms: both just under threshold
    let barely_miss = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, Some(2 * H + 1))
        .await
        .unwrap();
    assert!(barely_miss.is_empty());
}

// ── Edge cases: empty / degenerate inputs ─────────────────────

#[tokio::test]
async fn multi_avail_empty_resources() {
    let path = test_wal_path("multi_avail_empty.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let result = engine
        .compute_multi_availability(&[], 0, 100_000, 1, None)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn multi_avail_min_available_zero() {
    // min_available = 0 should return empty (nothing to satisfy)
    let path = test_wal_path("multi_avail_min0.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let r = Ulid::new();
    engine.create_resource(r, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), r, Span::new(0, 10000), false).await.unwrap();

    let result = engine
        .compute_multi_availability(&[r], 0, 10000, 0, None)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn multi_avail_min_available_exceeds_count() {
    // min_available > resource count: impossible, always empty
    let path = test_wal_path("multi_avail_exceed.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(0, 10000), false).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(0, 10000), false).await.unwrap();

    // Need 3 of 2, impossible
    let result = engine
        .compute_multi_availability(&[a, b], 0, 10000, 3, None)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn multi_avail_single_resource() {
    // IN list with one resource should behave same as regular availability
    let path = test_wal_path("multi_avail_single.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let r = Ulid::new();
    engine.create_resource(r, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), r, Span::new(8 * H, 17 * H), false).await.unwrap();
    engine.confirm_booking(Ulid::new(), r, Span::new(12 * H, 13 * H), None).await.unwrap();

    // Multi with min_available = 1 should match regular availability
    let multi = engine
        .compute_multi_availability(&[r], 0, 24 * H, 1, None)
        .await
        .unwrap();
    let regular = engine
        .compute_availability(r, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(multi, regular);
}

// ── Resources with no availability ────────────────────────────

#[tokio::test]
async fn multi_avail_one_resource_has_no_rules() {
    // Resource with no rules has no availability → intersection is empty
    let path = test_wal_path("multi_avail_norules.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(8 * H, 17 * H), false).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    // No rules for b, zero availability

    // Intersection: a has [8,17), b has nothing → empty
    let result = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert!(result.is_empty());

    // Union: only a has availability → [8,17)
    let union = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 1, None)
        .await
        .unwrap();
    assert_eq!(union, vec![Span::new(8 * H, 17 * H)]);
}

#[tokio::test]
async fn multi_avail_all_resources_no_availability() {
    let path = test_wal_path("multi_avail_allnone.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();

    let result = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 1, None)
        .await
        .unwrap();
    assert!(result.is_empty());
}

// ── Blocking rules on resources ───────────────────────────────

#[tokio::test]
async fn multi_avail_with_blocking_rules() {
    // Blocking rules should subtract from availability before sweep
    let path = test_wal_path("multi_avail_blocking.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(8 * H, 17 * H), false).await.unwrap();
    // Blocking 12-1pm (lunch)
    engine.add_rule(Ulid::new(), a, Span::new(12 * H, 13 * H), true).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(8 * H, 17 * H), false).await.unwrap();

    // a: [8,12) ∪ [13,17). b: [8,17).
    // Intersection: [8,12) ∪ [13,17) (limited by a's blocking)
    let result = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![
        Span::new(8 * H, 12 * H),
        Span::new(13 * H, 17 * H),
    ]);
}

#[tokio::test]
async fn multi_avail_with_inherited_blocking() {
    // Parent blocking rule propagates to child, affects multi-avail
    let path = test_wal_path("multi_avail_inh_block.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Parent with blocking rule
    let parent = Ulid::new();
    engine.create_resource(parent, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), parent, Span::new(0, 24 * H), false).await.unwrap();
    // Maintenance 2-4pm
    engine.add_rule(Ulid::new(), parent, Span::new(14 * H, 16 * H), true).await.unwrap();

    // Child inherits parent rules
    let child = Ulid::new();
    engine.create_resource(child, Some(parent), None, 1, None).await.unwrap();

    // Independent resource
    let other = Ulid::new();
    engine.create_resource(other, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), other, Span::new(12 * H, 18 * H), false).await.unwrap();

    // child: [0,14) ∪ [16,24). other: [12,18).
    // Intersection: [12,14) ∪ [16,18)
    let result = engine
        .compute_multi_availability(&[child, other], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![
        Span::new(12 * H, 14 * H),
        Span::new(16 * H, 18 * H),
    ]);
}

// ── Buffer interaction ────────────────────────────────────────

#[tokio::test]
async fn multi_avail_with_buffer_after() {
    // buffer_after should shrink availability before sweep
    let path = test_wal_path("multi_avail_buffer.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Resource with 1h buffer
    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, Some(H)).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(8 * H, 17 * H), false).await.unwrap();
    // Booking 10-11am → effective end 12pm (with buffer)
    engine.confirm_booking(Ulid::new(), a, Span::new(10 * H, 11 * H), None).await.unwrap();

    // Resource without buffer
    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(8 * H, 17 * H), false).await.unwrap();

    // a availability: [8,10) ∪ [12,17) (booking 10-11 + 1h buffer = gap 10-12)
    // b availability: [8,17)
    // Intersection: [8,10) ∪ [12,17)
    let result = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![
        Span::new(8 * H, 10 * H),
        Span::new(12 * H, 17 * H),
    ]);
}

// ── Capacity interaction ──────────────────────────────────────

#[tokio::test]
async fn multi_avail_with_capacity_resource() {
    // Resource with capacity > 1 should still have availability until saturated
    let path = test_wal_path("multi_avail_capacity.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Meeting room: capacity 2
    let room = Ulid::new();
    engine.create_resource(room, None, None, 2, None).await.unwrap();
    engine.add_rule(Ulid::new(), room, Span::new(8 * H, 17 * H), false).await.unwrap();
    // One booking 10-11am, room NOT saturated (1 of 2)
    engine.confirm_booking(Ulid::new(), room, Span::new(10 * H, 11 * H), None).await.unwrap();

    // Projector: capacity 1
    let projector = Ulid::new();
    engine.create_resource(projector, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), projector, Span::new(8 * H, 17 * H), false).await.unwrap();

    // Room still available [8,17) (capacity not saturated).
    // Intersection: [8,17)
    let result = engine
        .compute_multi_availability(&[room, projector], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![Span::new(8 * H, 17 * H)]);

    // Now saturate the room at 10-11am
    engine.confirm_booking(Ulid::new(), room, Span::new(10 * H, 11 * H), None).await.unwrap();

    // Room: [8,10) ∪ [11,17). Projector: [8,17).
    // Intersection: [8,10) ∪ [11,17)
    let result2 = engine
        .compute_multi_availability(&[room, projector], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result2, vec![
        Span::new(8 * H, 10 * H),
        Span::new(11 * H, 17 * H),
    ]);
}

// ── Boundary conditions ───────────────────────────────────────

#[tokio::test]
async fn multi_avail_exact_boundary_touch() {
    // Two resources whose availability spans share exact boundaries
    // [8,12) and [12,17), they touch but don't overlap
    let path = test_wal_path("multi_avail_boundary.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(8 * H, 12 * H), false).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(12 * H, 17 * H), false).await.unwrap();

    // Intersection: no overlap (half-open intervals don't share any point)
    let result = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert!(result.is_empty());

    // Union: [8,12) ∪ [12,17) = [8,17). The two resources hand off coverage at
    // the exact boundary 12h, so the result is ONE continuous window, not two
    // fragments. (Before GAP-13 the sweep emitted two adjacent spans here; that
    // representation silently dropped continuous windows under a min_duration
    // filter, so the result is now merged to match the single-resource path.)
    let union = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 1, None)
        .await
        .unwrap();
    assert_eq!(union, vec![Span::new(8 * H, 17 * H)]);
}

#[tokio::test]
async fn multi_avail_single_ms_overlap() {
    // Spans overlap by exactly 1ms
    let path = test_wal_path("multi_avail_1ms.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(0, 1001), false).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(1000, 2000), false).await.unwrap();

    // Intersection: [1000, 1001), 1ms overlap
    let result = engine
        .compute_multi_availability(&[a, b], 0, 3000, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![Span::new(1000, 1001)]);
}

#[tokio::test]
async fn multi_avail_identical_spans() {
    // All resources have exactly the same availability
    let path = test_wal_path("multi_avail_identical.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let ids: Vec<Ulid> = (0..5).map(|_| Ulid::new()).collect();
    for &id in &ids {
        engine.create_resource(id, None, None, 1, None).await.unwrap();
        engine.add_rule(Ulid::new(), id, Span::new(9 * H, 17 * H), false).await.unwrap();
    }

    // All thresholds 1-5 should return [9,17)
    for min in 1..=5 {
        let result = engine
            .compute_multi_availability(&ids, 0, 24 * H, min, None)
            .await
            .unwrap();
        assert_eq!(result, vec![Span::new(9 * H, 17 * H)],
            "threshold {min} should return full span");
    }

    // Threshold 6: impossible
    let impossible = engine
        .compute_multi_availability(&ids, 0, 24 * H, 6, None)
        .await
        .unwrap();
    assert!(impossible.is_empty());
}

// ── Query window clipping ─────────────────────────────────────

#[tokio::test]
async fn multi_avail_query_clips_results() {
    // Query window is narrower than actual availability
    let path = test_wal_path("multi_avail_clip.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(6 * H, 22 * H), false).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(8 * H, 20 * H), false).await.unwrap();

    // Intersection without clip: [8,20)
    // Query only 10am-15pm:
    let result = engine
        .compute_multi_availability(&[a, b], 10 * H, 15 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![Span::new(10 * H, 15 * H)]);
}

#[tokio::test]
async fn multi_avail_query_no_overlap_with_availability() {
    // Query window entirely outside availability
    let path = test_wal_path("multi_avail_noqoverlap.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(8 * H, 12 * H), false).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(8 * H, 12 * H), false).await.unwrap();

    // Query 20pm-24pm: no availability
    let result = engine
        .compute_multi_availability(&[a, b], 20 * H, 24 * H, 1, None)
        .await
        .unwrap();
    assert!(result.is_empty());
}

// ── Multiple availability windows per resource ────────────────

#[tokio::test]
async fn multi_avail_fragmented_availability() {
    // Resources with multiple disjoint availability windows
    let path = test_wal_path("multi_avail_frag.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    // a: morning [8,12) and afternoon [14,18)
    engine.add_rule(Ulid::new(), a, Span::new(8 * H, 12 * H), false).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(14 * H, 18 * H), false).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    // b: midday [10,16)
    engine.add_rule(Ulid::new(), b, Span::new(10 * H, 16 * H), false).await.unwrap();

    // Intersection: [10,12) (a-morning ∩ b) ∪ [14,16) (a-afternoon ∩ b)
    let result = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![
        Span::new(10 * H, 12 * H),
        Span::new(14 * H, 16 * H),
    ]);
}

// ── Cascading overlaps ────────────────────────────────────────

#[tokio::test]
async fn multi_avail_cascading_no_triple_overlap() {
    // A overlaps B, B overlaps C, but A doesn't overlap C
    let path = test_wal_path("multi_avail_cascade.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(8 * H, 12 * H), false).await.unwrap();

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(10 * H, 16 * H), false).await.unwrap();

    let c = Ulid::new();
    engine.create_resource(c, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), c, Span::new(14 * H, 20 * H), false).await.unwrap();

    // min=1: [8,20), continuous chain
    let union = engine
        .compute_multi_availability(&[a, b, c], 0, 24 * H, 1, None)
        .await
        .unwrap();
    assert_eq!(union, vec![Span::new(8 * H, 20 * H)]);

    // min=2: [10,12) (a+b) ∪ [14,16) (b+c)
    let two = engine
        .compute_multi_availability(&[a, b, c], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(two, vec![
        Span::new(10 * H, 12 * H),
        Span::new(14 * H, 16 * H),
    ]);

    // min=3: empty (no triple overlap)
    let three = engine
        .compute_multi_availability(&[a, b, c], 0, 24 * H, 3, None)
        .await
        .unwrap();
    assert!(three.is_empty());
}

// ── Bookings reducing availability ────────────────────────────

#[tokio::test]
async fn multi_avail_multiple_bookings_fragment() {
    // Multiple bookings on different resources create complex patterns
    let path = test_wal_path("multi_avail_multi_book.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let a = Ulid::new();
    engine.create_resource(a, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), a, Span::new(8 * H, 18 * H), false).await.unwrap();
    engine.confirm_booking(Ulid::new(), a, Span::new(10 * H, 11 * H), None).await.unwrap();
    engine.confirm_booking(Ulid::new(), a, Span::new(14 * H, 15 * H), None).await.unwrap();
    // a: [8,10) ∪ [11,14) ∪ [15,18)

    let b = Ulid::new();
    engine.create_resource(b, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), b, Span::new(8 * H, 18 * H), false).await.unwrap();
    engine.confirm_booking(Ulid::new(), b, Span::new(9 * H, 12 * H), None).await.unwrap();
    // b: [8,9) ∪ [12,18)

    // Intersection:
    // a: [8,10) [11,14) [15,18)
    // b: [8,9)  [12,18)
    // → [8,9) ∩ both, [12,14) ∩ both, [15,18) ∩ both
    let result = engine
        .compute_multi_availability(&[a, b], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![
        Span::new(8 * H, 9 * H),
        Span::new(12 * H, 14 * H),
        Span::new(15 * H, 18 * H),
    ]);
}

// ── Duplicate resource ID ─────────────────────────────────────

#[tokio::test]
async fn multi_avail_duplicate_resource_id() {
    // Same resource listed twice: counts as 2 for threshold purposes
    let path = test_wal_path("multi_avail_dup.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let r = Ulid::new();
    engine.create_resource(r, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), r, Span::new(8 * H, 17 * H), false).await.unwrap();

    // Same ID twice: each contributes +1 to the count, so count=2 during [8,17)
    let result = engine
        .compute_multi_availability(&[r, r], 0, 24 * H, 2, None)
        .await
        .unwrap();
    assert_eq!(result, vec![Span::new(8 * H, 17 * H)]);
}

// ── Large pool scenario ───────────────────────────────────────

#[tokio::test]
async fn multi_avail_large_pool_various_thresholds() {
    // 10 resources, staggered start times
    let path = test_wal_path("multi_avail_large.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let mut ids = Vec::new();
    for i in 0..10u64 {
        let r = Ulid::new();
        engine.create_resource(r, None, None, 1, None).await.unwrap();
        // Each starts 1h later: resource 0=[0,20h), resource 1=[1h,20h), ...
        engine.add_rule(Ulid::new(), r, Span::new(i as i64 * H, 20 * H), false).await.unwrap();
        ids.push(r);
    }

    // At time 9h, all 10 are available. At time 0h, only resource 0 is.
    // min=1: [0,20h), at least one is always free from 0-20h
    let union = engine
        .compute_multi_availability(&ids, 0, 24 * H, 1, None)
        .await
        .unwrap();
    assert_eq!(union, vec![Span::new(0, 20 * H)]);

    // min=10: [9h,20h), all 10 are free only from 9h onward
    let all = engine
        .compute_multi_availability(&ids, 0, 24 * H, 10, None)
        .await
        .unwrap();
    assert_eq!(all, vec![Span::new(9 * H, 20 * H)]);

    // min=5: [4h,20h), resources 0-4 all available from 4h
    let five = engine
        .compute_multi_availability(&ids, 0, 24 * H, 5, None)
        .await
        .unwrap();
    assert_eq!(five, vec![Span::new(4 * H, 20 * H)]);
}

// ── Nonexistent resource ──────────────────────────────────────

#[tokio::test]
async fn multi_avail_nonexistent_resource_ignored() {
    let path = test_wal_path("multi_avail_notfound.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let real = Ulid::new();
    engine.create_resource(real, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), real, Span::new(0, 10000), false).await.unwrap();

    let fake = Ulid::new(); // never created, contributes 0 availability

    // min_available=1, so the real resource's availability is enough
    let result = engine
        .compute_multi_availability(&[real, fake], 0, 10000, 1, None)
        .await
        .unwrap();
    assert_eq!(result, vec![Span::new(0, 10000)]);
}

// ── Vertical: mechanic + plane + hangar ───────────────────────

#[tokio::test]
async fn multi_avail_vertical_maintenance_scheduling() {
    // Real-world scenario: schedule maintenance when mechanic, plane, and hangar
    // are all free simultaneously.
    let path = test_wal_path("multi_avail_maint.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let mechanic = Ulid::new();
    engine.create_resource(mechanic, None, None, 1, None).await.unwrap();
    // Mechanic: 7am-3pm Mon-Fri (we simulate one day)
    engine.add_rule(Ulid::new(), mechanic, Span::new(7 * H, 15 * H), false).await.unwrap();
    // Already doing another job 9am-11am
    engine.confirm_booking(Ulid::new(), mechanic, Span::new(9 * H, 11 * H), None).await.unwrap();

    let plane = Ulid::new();
    engine.create_resource(plane, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), plane, Span::new(0, 24 * H), false).await.unwrap();
    // Flying 6am-9am and 1pm-5pm
    engine.confirm_booking(Ulid::new(), plane, Span::new(6 * H, 9 * H), None).await.unwrap();
    engine.confirm_booking(Ulid::new(), plane, Span::new(13 * H, 17 * H), None).await.unwrap();

    let hangar = Ulid::new();
    engine.create_resource(hangar, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), hangar, Span::new(6 * H, 22 * H), false).await.unwrap();
    // Another plane in hangar 7am-10am
    engine.confirm_booking(Ulid::new(), hangar, Span::new(7 * H, 10 * H), None).await.unwrap();

    // mechanic: [7,9) ∪ [11,15)
    // plane: [0,6) ∪ [9,13) ∪ [17,24)
    // hangar: [6,7) ∪ [10,22)
    // ALL three free: [11,13), the only maintenance window
    let window = engine
        .compute_multi_availability(&[mechanic, plane, hangar], 0, 24 * H, 3, None)
        .await
        .unwrap();
    assert_eq!(window, vec![Span::new(11 * H, 13 * H)]);

    // Check it's long enough for a 2h maintenance job
    let with_dur = engine
        .compute_multi_availability(&[mechanic, plane, hangar], 0, 24 * H, 3, Some(2 * H))
        .await
        .unwrap();
    assert_eq!(with_dur, vec![Span::new(11 * H, 13 * H)]);

    // Not long enough for 3h job
    let too_short = engine
        .compute_multi_availability(&[mechanic, plane, hangar], 0, 24 * H, 3, Some(3 * H))
        .await
        .unwrap();
    assert!(too_short.is_empty());
}

// ── Vertical: doctor + room + anesthesiologist ─────────────────

#[tokio::test]
async fn multi_avail_vertical_surgery_scheduling() {
    let path = test_wal_path("multi_avail_surgery.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let doctor = Ulid::new();
    engine.create_resource(doctor, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), doctor, Span::new(6 * H, 18 * H), false).await.unwrap();
    // Rounds 6-8am, surgery 8-11am, appointments 2-5pm
    engine.confirm_booking(Ulid::new(), doctor, Span::new(6 * H, 8 * H), None).await.unwrap();
    engine.confirm_booking(Ulid::new(), doctor, Span::new(8 * H, 11 * H), None).await.unwrap();
    engine.confirm_booking(Ulid::new(), doctor, Span::new(14 * H, 17 * H), None).await.unwrap();
    // doctor free: [11,14) ∪ [17,18)

    let or_room = Ulid::new();
    engine.create_resource(or_room, None, None, 1, Some(30 * M)).await.unwrap(); // 30min cleaning buffer
    engine.add_rule(Ulid::new(), or_room, Span::new(7 * H, 20 * H), false).await.unwrap();
    // Surgery 7-10am (+ 30min cleaning = effective 10:30)
    engine.confirm_booking(Ulid::new(), or_room, Span::new(7 * H, 10 * H), None).await.unwrap();
    // or_room free: [10h30m, 20)

    let anesthesiologist = Ulid::new();
    engine.create_resource(anesthesiologist, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), anesthesiologist, Span::new(8 * H, 16 * H), false).await.unwrap();
    // Busy 8-9am
    engine.confirm_booking(Ulid::new(), anesthesiologist, Span::new(8 * H, 9 * H), None).await.unwrap();
    // anesthesiologist free: [9,16)

    // All three free:
    // doctor: [11,14) [17,18)
    // or_room: [10:30,20)
    // anesthesiologist: [9,16)
    // Intersection: [11,14)
    let window = engine
        .compute_multi_availability(&[doctor, or_room, anesthesiologist], 0, 24 * H, 3, None)
        .await
        .unwrap();
    assert_eq!(window, vec![Span::new(11 * H, 14 * H)]);
}

// ── Vertical: pool of interchangeable resources ───────────────

#[tokio::test]
async fn multi_avail_vertical_taxi_dispatch() {
    // 4 taxis, dispatcher needs to know when at least 1 is available
    let path = test_wal_path("multi_avail_taxi.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let mut taxis = Vec::new();
    for _ in 0..4 {
        let t = Ulid::new();
        engine.create_resource(t, None, None, 1, None).await.unwrap();
        engine.add_rule(Ulid::new(), t, Span::new(0, 24 * H), false).await.unwrap();
        taxis.push(t);
    }

    // All taxis busy 8-9am (rush hour)
    for &t in &taxis {
        engine.confirm_booking(Ulid::new(), t, Span::new(8 * H, 9 * H), None).await.unwrap();
    }

    // Taxis 0,1 busy 12-1pm (lunch)
    engine.confirm_booking(Ulid::new(), taxis[0], Span::new(12 * H, 13 * H), None).await.unwrap();
    engine.confirm_booking(Ulid::new(), taxis[1], Span::new(12 * H, 13 * H), None).await.unwrap();

    // min=1 (any taxi free): [0,8) ∪ [9,24), 8-9am completely blocked
    let any = engine
        .compute_multi_availability(&taxis, 0, 24 * H, 1, None)
        .await
        .unwrap();
    assert_eq!(any, vec![Span::new(0, 8 * H), Span::new(9 * H, 24 * H)]);

    // min=3: [0,8) ∪ [9,12) ∪ [13,24), at lunch only 2 taxis (0,1 busy)
    let three = engine
        .compute_multi_availability(&taxis, 0, 24 * H, 3, None)
        .await
        .unwrap();
    assert_eq!(three, vec![
        Span::new(0, 8 * H),
        Span::new(9 * H, 12 * H),
        Span::new(13 * H, 24 * H),
    ]);

    // min=4 (all taxis): [0,8) ∪ [9,12) ∪ [13,24)
    // Wait, taxis 2,3 are free at lunch, so at lunch count=2.
    // For min=4 we also lose 12-1pm.
    let all = engine
        .compute_multi_availability(&taxis, 0, 24 * H, 4, None)
        .await
        .unwrap();
    assert_eq!(all, vec![
        Span::new(0, 8 * H),
        Span::new(9 * H, 12 * H),
        Span::new(13 * H, 24 * H),
    ]);
}
