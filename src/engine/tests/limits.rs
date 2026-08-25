use crate::engine::*;
use crate::engine::conflict::{validate_buffer, validate_span, validate_timestamp};
use crate::limits::*;
use super::helpers::*;

// ── Limit tests ──────────────────────────────────────────

#[tokio::test]
async fn query_window_too_wide() {
    let path = test_wal_path("limit_query_window.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let too_wide = MAX_QUERY_WINDOW_MS + 1;
    let result = engine.compute_availability(rid, 0, too_wide, None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("query window too wide"))));
}

#[tokio::test]
async fn query_window_at_limit() {
    let path = test_wal_path("limit_query_window_ok.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let result = engine.compute_availability(rid, 0, MAX_QUERY_WINDOW_MS, None).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn multi_avail_too_many_ids() {
    let path = test_wal_path("limit_multi_ids.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let ids: Vec<Ulid> = (0..MAX_IN_CLAUSE_IDS + 1).map(|_| Ulid::new()).collect();
    let result = engine.compute_multi_availability(&ids, 0, H, 1, None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("too many resource IDs"))));
}

#[tokio::test]
async fn multi_avail_at_limit() {
    let path = test_wal_path("limit_multi_ids_ok.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Create MAX_IN_CLAUSE_IDS resources
    let mut ids = Vec::new();
    for _ in 0..MAX_IN_CLAUSE_IDS {
        let rid = Ulid::new();
        engine.create_resource(rid, None, None, 1, None).await.unwrap();
        ids.push(rid);
    }
    let result = engine.compute_multi_availability(&ids, 0, H, 1, None).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_resource_too_many() {
    let path = test_wal_path("limit_resources.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    for _ in 0..MAX_RESOURCES_PER_TENANT {
        engine.create_resource(Ulid::new(), None, None, 1, None).await.unwrap();
    }
    let result = engine.create_resource(Ulid::new(), None, None, 1, None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("too many resources"))));
}

#[tokio::test]
async fn create_resource_name_too_long() {
    let path = test_wal_path("limit_name.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let long_name = "x".repeat(MAX_NAME_LEN + 1);
    let result = engine.create_resource(Ulid::new(), None, Some(long_name), 1, None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("resource name too long"))));
}

#[tokio::test]
async fn create_resource_capacity_zero_rejected() {
    // Capacity 0 naturally reads as "not bookable", but both the read and the write path lump
    // it in with 1 (`<= 1` branches), so it silently behaved as a bookable single slot. Reject
    // it at the boundary instead of defining a surprise semantic.
    let path = test_wal_path("capacity_zero_create.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let result = engine.create_resource(Ulid::new(), None, None, 0, None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("capacity must be at least 1"))));
}

#[tokio::test]
async fn update_resource_capacity_zero_rejected() {
    // Same boundary on the update path: an existing resource must not be flipped into the
    // undefined capacity-0 state.
    let path = test_wal_path("capacity_zero_update.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 2, None).await.unwrap();
    let result = engine.update_resource(rid, None, Some(0), None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("capacity must be at least 1"))));

    // The rejected update left the stored capacity untouched.
    let info = engine.list_resources().await.into_iter().find(|r| r.id == rid).unwrap();
    assert_eq!(info.capacity, 2);
}

#[tokio::test]
async fn hierarchy_too_deep() {
    let path = test_wal_path("limit_hierarchy.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Build a chain of MAX_HIERARCHY_DEPTH + 1 resources (0 is root, 1..=MAX are children)
    let mut prev = Ulid::new();
    engine.create_resource(prev, None, None, 1, None).await.unwrap();
    for _ in 0..MAX_HIERARCHY_DEPTH {
        let next = Ulid::new();
        engine.create_resource(next, Some(prev), None, 1, None).await.unwrap();
        prev = next;
    }

    // One more should fail
    let result = engine.create_resource(Ulid::new(), Some(prev), None, 1, None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("hierarchy too deep"))));
}

#[tokio::test]
async fn hierarchy_at_limit() {
    let path = test_wal_path("limit_hierarchy_ok.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let mut prev = Ulid::new();
    engine.create_resource(prev, None, None, 1, None).await.unwrap();
    // Build chain of exactly MAX_HIERARCHY_DEPTH parents
    for _ in 0..MAX_HIERARCHY_DEPTH - 1 {
        let next = Ulid::new();
        engine.create_resource(next, Some(prev), None, 1, None).await.unwrap();
        prev = next;
    }

    // This is the MAX_HIERARCHY_DEPTH-th child, should succeed
    let result = engine.create_resource(Ulid::new(), Some(prev), None, 1, None).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn interval_limit_rule() {
    let path = test_wal_path("limit_intervals_rule.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    // Fill resource with MAX_INTERVALS_PER_RESOURCE intervals
    for i in 0..MAX_INTERVALS_PER_RESOURCE {
        let start = (i as i64) * 10;
        engine.add_rule(Ulid::new(), rid, Span::new(start, start + 5), false).await.unwrap();
    }

    let start = (MAX_INTERVALS_PER_RESOURCE as i64) * 10;
    let result = engine.add_rule(Ulid::new(), rid, Span::new(start, start + 5), false).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("too many intervals on resource"))));
}

#[tokio::test]
async fn interval_limit_hold() {
    let path = test_wal_path("limit_intervals_hold.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    // Capacity matches the number of holds we'll place
    engine.create_resource(rid, None, None, (MAX_INTERVALS_PER_RESOURCE + 1) as u32, None).await.unwrap();

    // Add one non-blocking rule to cover all holds
    engine.add_rule(Ulid::new(), rid, Span::new(0, (MAX_INTERVALS_PER_RESOURCE as i64 + 2) * 10), false).await.unwrap();

    // The largest valid expiry instant; i64::MAX/2 is rejected by validate_timestamp (out of range).
    let far_future = MAX_VALID_TIMESTAMP_MS;
    for i in 0..MAX_INTERVALS_PER_RESOURCE - 1 {
        let start = (i as i64) * 10;
        engine.place_hold(Ulid::new(), rid, Span::new(start, start + 5), far_future).await.unwrap();
    }

    let start = ((MAX_INTERVALS_PER_RESOURCE - 1) as i64) * 10;
    let result = engine.place_hold(Ulid::new(), rid, Span::new(start, start + 5), far_future).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("too many intervals on resource"))));
}

#[tokio::test]
async fn interval_limit_booking() {
    let path = test_wal_path("limit_intervals_booking.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, (MAX_INTERVALS_PER_RESOURCE + 1) as u32, None).await.unwrap();

    engine.add_rule(Ulid::new(), rid, Span::new(0, (MAX_INTERVALS_PER_RESOURCE as i64 + 2) * 10), false).await.unwrap();

    for i in 0..MAX_INTERVALS_PER_RESOURCE - 1 {
        let start = (i as i64) * 10;
        engine.confirm_booking(Ulid::new(), rid, Span::new(start, start + 5), None).await.unwrap();
    }

    let start = ((MAX_INTERVALS_PER_RESOURCE - 1) as i64) * 10;
    let result = engine.confirm_booking(Ulid::new(), rid, Span::new(start, start + 5), None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("too many intervals on resource"))));
}

#[tokio::test]
async fn label_too_long() {
    let path = test_wal_path("limit_label.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10000), false).await.unwrap();

    let long_label = "x".repeat(MAX_LABEL_LEN + 1);
    let result = engine.confirm_booking(Ulid::new(), rid, Span::new(100, 200), Some(long_label)).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("label too long"))));
}

#[tokio::test]
async fn batch_too_large() {
    let path = test_wal_path("limit_batch.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let bookings: Vec<_> = (0..MAX_BATCH_SIZE + 1)
        .map(|i| {
            let start = (i as i64) * 100;
            (Ulid::new(), Ulid::new(), Span::new(start, start + 50), None)
        })
        .collect();
    let result = engine.batch_confirm_bookings(bookings).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("batch too large"))));
}

#[tokio::test]
async fn batch_at_limit() {
    let path = test_wal_path("limit_batch_ok.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, (MAX_BATCH_SIZE + 1) as u32, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, (MAX_BATCH_SIZE as i64 + 1) * 100), false).await.unwrap();

    let bookings: Vec<_> = (0..MAX_BATCH_SIZE)
        .map(|i| {
            let start = (i as i64) * 100;
            (Ulid::new(), rid, Span::new(start, start + 50), None)
        })
        .collect();
    let result = engine.batch_confirm_bookings(bookings).await;
    assert!(result.is_ok());
}

#[test]
fn validate_span_before_epoch() {
    let span = Span::new(-1000, 1000);
    let result = validate_span(&span);
    assert!(matches!(result, Err(EngineError::LimitExceeded("timestamp out of range"))));
}

#[test]
fn validate_span_far_future() {
    let span = Span::new(1000, MAX_VALID_TIMESTAMP_MS + 1);
    let result = validate_span(&span);
    assert!(matches!(result, Err(EngineError::LimitExceeded("timestamp out of range"))));
}

#[test]
fn validate_span_too_wide() {
    let span = Span::new(0, MAX_SPAN_DURATION_MS + 1);
    let result = validate_span(&span);
    assert!(matches!(result, Err(EngineError::LimitExceeded("span too wide"))));
}

// ── Boundary success tests (at exact limit, should pass) ────

#[test]
fn validate_span_at_epoch_boundary() {
    let span = Span::new(MIN_VALID_TIMESTAMP_MS, 1000);
    assert!(validate_span(&span).is_ok());
}

#[test]
fn validate_span_at_max_timestamp_boundary() {
    let span = Span::new(MAX_VALID_TIMESTAMP_MS - 1000, MAX_VALID_TIMESTAMP_MS);
    assert!(validate_span(&span).is_ok());
}

#[test]
fn validate_span_at_max_duration_boundary() {
    let span = Span::new(0, MAX_SPAN_DURATION_MS);
    assert!(validate_span(&span).is_ok());
}

#[test]
fn validate_buffer_bounds() {
    assert!(validate_buffer(None).is_ok());
    assert!(validate_buffer(Some(0)).is_ok());
    assert!(validate_buffer(Some(MAX_SPAN_DURATION_MS)).is_ok());
    assert!(matches!(
        validate_buffer(Some(-1)),
        Err(EngineError::LimitExceeded("buffer_after out of range"))
    ));
    assert!(matches!(
        validate_buffer(Some(MAX_SPAN_DURATION_MS + 1)),
        Err(EngineError::LimitExceeded("buffer_after out of range"))
    ));
    assert!(matches!(
        validate_buffer(Some(i64::MAX)),
        Err(EngineError::LimitExceeded("buffer_after out of range"))
    ));
}

#[test]
fn validate_timestamp_bounds() {
    assert!(validate_timestamp(MIN_VALID_TIMESTAMP_MS).is_ok());
    assert!(validate_timestamp(MAX_VALID_TIMESTAMP_MS).is_ok());
    assert!(validate_timestamp(-1).is_err());
    assert!(validate_timestamp(i64::MAX).is_err());
}

#[tokio::test]
async fn create_resource_rejects_overflowing_buffer() {
    // Regression: an out-of-range buffer_after used to flow into `span.end + buffer` and panic the
    // connection task on every booking/availability query (integer overflow → DoS). It must be
    // rejected at the boundary instead.
    let path = test_wal_path("buffer_overflow_reject.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let result = engine
        .create_resource(Ulid::new(), None, None, 1, Some(i64::MAX))
        .await;
    assert!(matches!(
        result,
        Err(EngineError::LimitExceeded("buffer_after out of range"))
    ));
}

#[tokio::test]
async fn place_hold_rejects_out_of_range_expiry() {
    let path = test_wal_path("hold_expiry_reject.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10_000), false).await.unwrap();
    let result = engine
        .place_hold(Ulid::new(), rid, Span::new(100, 200), i64::MAX)
        .await;
    assert!(matches!(
        result,
        Err(EngineError::LimitExceeded("timestamp out of range"))
    ));
}

#[tokio::test]
async fn create_resource_name_at_limit() {
    let path = test_wal_path("limit_name_ok.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let name = "x".repeat(MAX_NAME_LEN);
    let result = engine.create_resource(Ulid::new(), None, Some(name), 1, None).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn label_at_limit() {
    let path = test_wal_path("limit_label_ok.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10000), false).await.unwrap();

    let label = "x".repeat(MAX_LABEL_LEN);
    let result = engine.confirm_booking(Ulid::new(), rid, Span::new(100, 200), Some(label)).await;
    assert!(result.is_ok());
}

// ── update_resource / update_rule limit tests ───────────────

#[tokio::test]
async fn update_resource_name_too_long() {
    let path = test_wal_path("limit_update_name.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, Some("short".into()), 1, None).await.unwrap();

    let long_name = "x".repeat(MAX_NAME_LEN + 1);
    let result = engine.update_resource(rid, Some(Some(long_name)), Some(1), None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("resource name too long"))));
}

#[tokio::test]
async fn update_resource_name_at_limit() {
    let path = test_wal_path("limit_update_name_ok.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let name = "x".repeat(MAX_NAME_LEN);
    let result = engine.update_resource(rid, Some(Some(name)), Some(1), None).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn update_rule_invalid_span() {
    let path = test_wal_path("limit_update_rule_span.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let rule_id = Ulid::new();
    engine.add_rule(rule_id, rid, Span::new(0, 1000), false).await.unwrap();

    // Try to update with span before epoch
    let result = engine.update_rule(rule_id, Span::new(-1000, 1000), false).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("timestamp out of range"))));
}

#[tokio::test]
async fn update_rule_span_too_wide() {
    let path = test_wal_path("limit_update_rule_wide.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    let rule_id = Ulid::new();
    engine.add_rule(rule_id, rid, Span::new(0, 1000), false).await.unwrap();

    let result = engine.update_rule(rule_id, Span::new(0, MAX_SPAN_DURATION_MS + 1), false).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("span too wide"))));
}

// ── multi_avail query window tests ──────────────────────────

#[tokio::test]
async fn multi_avail_query_window_too_wide() {
    let path = test_wal_path("limit_multi_qw.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let too_wide = MAX_QUERY_WINDOW_MS + 1;
    let result = engine.compute_multi_availability(&[rid], 0, too_wide, 1, None).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("query window too wide"))));
}

#[tokio::test]
async fn multi_avail_query_window_at_limit() {
    let path = test_wal_path("limit_multi_qw_ok.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    let result = engine.compute_multi_availability(&[rid], 0, MAX_QUERY_WINDOW_MS, 1, None).await;
    assert!(result.is_ok());
}

// ── batch_confirm_bookings edge cases ───────────────────────

#[tokio::test]
async fn batch_label_too_long() {
    let path = test_wal_path("limit_batch_label.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 10, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10000), false).await.unwrap();

    let long_label = "x".repeat(MAX_LABEL_LEN + 1);
    let bookings = vec![
        (Ulid::new(), rid, Span::new(100, 200), None),
        (Ulid::new(), rid, Span::new(300, 400), Some(long_label)),
    ];
    let result = engine.batch_confirm_bookings(bookings).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("label too long"))));
}

#[tokio::test]
async fn batch_invalid_span() {
    let path = test_wal_path("limit_batch_span.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 10, None).await.unwrap();

    let bookings = vec![
        (Ulid::new(), rid, Span::new(100, 200), None),
        (Ulid::new(), rid, Span::new(-1000, 200), None),
    ];
    let result = engine.batch_confirm_bookings(bookings).await;
    assert!(matches!(result, Err(EngineError::LimitExceeded("timestamp out of range"))));
}
