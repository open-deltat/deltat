//! Metric-emission tests: booking-domain counters and WAL health instrumentation.
//!
//! Counters are asserted through the engine's public API under a thread-local recorder, so a
//! rejected or failed write showing up in a count fails here, not on a dashboard.

use crate::engine::*;
use super::helpers::*;

use std::path::Path;

use crate::observability::{
    BOOKINGS_CREATED_TOTAL, BOOKINGS_DELETED_TOTAL, GC_INTERVALS_COLLECTED_TOTAL,
    HOLDS_COMMITTED_TOTAL, HOLDS_EXPIRED_TOTAL, HOLDS_PLACED_TOTAL, HOLDS_RELEASED_TOTAL,
    WAL_COMPACTION_DURATION_SECONDS, WAL_ERRORS_TOTAL, WAL_FLUSH_BATCH_SIZE,
    WAL_FLUSH_DURATION_SECONDS, WAL_POISONED,
};

use crate::test_metrics::{block_on, with_metrics};

fn create_event() -> Event {
    Event::ResourceCreated {
        id: Ulid::new(),
        parent_id: None,
        name: None,
        capacity: 1,
        buffer_after: None,
    }
}

/// XOR one byte of the file in place. Applied to the first payload byte of the first record
/// (offset 4, past the length prefix) it makes that record's CRC fail; with a valid record
/// after it that is mid-log corruption, which `Wal::open` refuses, so `recover` fails.
/// Corrupts a byte of the first record's payload. Offsets are measured from the end of the WAL
/// header, so the file stays identifiable as a WAL and the damage lands where a real torn write
/// would leave it.
fn flip_record_byte(path: &Path, offset: u64) {
    flip_byte(path, crate::wal::HEADER_BYTES as u64 + offset)
}

fn flip_byte(path: &Path, offset: u64) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    let mut b = [0u8; 1];
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.read_exact(&mut b).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(&[b[0] ^ 0xFF]).unwrap();
}

// ── booking-domain counters ──────────────────────────────────

#[test]
fn hold_lifecycle_counters_are_exact() {
    // Every hold outcome must count exactly once or the abandonment arithmetic documented on
    // HOLDS_EXPIRED_TOTAL breaks: a commit is not a release, a reap is not a release, and a
    // rejected placement is not a placement.
    let (log, _) = with_metrics(|| {
        block_on(async {
            let path = test_wal_path("metrics_hold_lifecycle.wal");
            let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
            let rid = Ulid::new();
            engine.create_resource(rid, None, None, 1, None).await.unwrap();

            let now = engine.now_ms();
            let committed = Ulid::new();
            let released = Ulid::new();
            let expired = Ulid::new();
            engine.place_hold(committed, rid, Span::new(1000, 2000), now + 60_000).await.unwrap();
            engine.place_hold(released, rid, Span::new(3000, 4000), now + 60_000).await.unwrap();
            engine.place_hold(expired, rid, Span::new(5000, 6000), now - 1000).await.unwrap();

            // A conflicting placement is rejected and must not count.
            assert!(engine
                .place_hold(Ulid::new(), rid, Span::new(1000, 2000), now + 60_000)
                .await
                .is_err());

            engine.commit_hold(committed, Ulid::new(), None).await.unwrap();
            engine.release_hold(released).await.unwrap();

            let due = engine.collect_expired_holds(engine.now_ms());
            assert_eq!(due.len(), 1);
            engine.expire_hold(expired).await.unwrap();
        })
    });

    assert_eq!(log.counter_total(HOLDS_PLACED_TOTAL, &[]), 3);
    assert_eq!(log.counter_total(HOLDS_COMMITTED_TOTAL, &[]), 1);
    assert_eq!(log.counter_total(HOLDS_RELEASED_TOTAL, &[]), 1);
    assert_eq!(log.counter_total(HOLDS_EXPIRED_TOTAL, &[]), 1);
    assert_eq!(log.counter_total(BOOKINGS_CREATED_TOTAL, &[]), 1);
}

#[test]
fn booking_counters_count_singles_batches_and_deletes() {
    let (log, _) = with_metrics(|| {
        block_on(async {
            let path = test_wal_path("metrics_bookings.wal");
            let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
            let rid = Ulid::new();
            engine.create_resource(rid, None, None, 1, None).await.unwrap();

            let b1 = Ulid::new();
            engine.confirm_booking(b1, rid, Span::new(1000, 2000), None).await.unwrap();
            engine
                .batch_confirm_bookings(vec![
                    (Ulid::new(), rid, Span::new(3000, 4000), None),
                    (Ulid::new(), rid, Span::new(5000, 6000), None),
                    (Ulid::new(), rid, Span::new(7000, 8000), None),
                ])
                .await
                .unwrap();

            // Rejected writes must not count: a conflicting single, and a batch whose second
            // member conflicts (all-or-nothing, so it contributes zero).
            assert!(engine
                .confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None)
                .await
                .is_err());
            assert!(engine
                .batch_confirm_bookings(vec![
                    (Ulid::new(), rid, Span::new(9000, 10000), None),
                    (Ulid::new(), rid, Span::new(3000, 4000), None),
                ])
                .await
                .is_err());

            engine.cancel_booking(b1).await.unwrap();
        })
    });

    assert_eq!(log.counter_total(BOOKINGS_CREATED_TOTAL, &[]), 4);
    assert_eq!(log.counter_total(BOOKINGS_DELETED_TOTAL, &[]), 1);
}

#[test]
fn gc_counter_matches_collected_return() {
    let (log, collected) = with_metrics(|| {
        block_on(async {
            let path = test_wal_path("metrics_gc.wal");
            let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
            let rid = Ulid::new();
            engine.create_resource(rid, None, None, 1, None).await.unwrap();
            engine.add_rule(Ulid::new(), rid, Span::new(1000, 50000), false).await.unwrap();
            engine.confirm_booking(Ulid::new(), rid, Span::new(1000, 2000), None).await.unwrap();

            engine.gc_past_intervals(10_000, 5_000)
        })
    });

    assert_eq!(collected, 1);
    assert_eq!(log.counter_total(GC_INTERVALS_COLLECTED_TOTAL, &[]), 1);
}

// ── WAL instrumentation ──────────────────────────────────────

#[test]
fn commit_hold_fsync_records_flush_histograms() {
    // Three flushes: two group-commit singles (create, place) and the commit's AppendAtomic
    // pair. The 2.0 batch-size sample and the third duration sample can only come from the
    // AppendAtomic branch, the terminal write of the booking flow.
    let (log, _) = with_metrics(|| {
        block_on(async {
            let path = test_wal_path("metrics_commit_fsync.wal");
            let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
            let rid = Ulid::new();
            engine.create_resource(rid, None, None, 1, None).await.unwrap();

            let hold = Ulid::new();
            let now = engine.now_ms();
            engine.place_hold(hold, rid, Span::new(1000, 2000), now + 60_000).await.unwrap();
            engine.commit_hold(hold, Ulid::new(), None).await.unwrap();
        })
    });

    assert_eq!(log.histogram_values(WAL_FLUSH_BATCH_SIZE), vec![1.0, 1.0, 2.0]);
    assert_eq!(log.histogram_values(WAL_FLUSH_DURATION_SECONDS).len(), 3);
}

#[test]
fn compaction_duration_is_recorded() {
    let (log, _) = with_metrics(|| {
        block_on(async {
            let path = test_wal_path("metrics_compaction.wal");
            let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
            engine.create_resource(Ulid::new(), None, None, 1, None).await.unwrap();
            engine.compact_wal().await.unwrap();
        })
    });

    assert_eq!(log.histogram_values(WAL_COMPACTION_DURATION_SECONDS).len(), 1);
}

#[test]
fn wal_poisoned_gauge_follows_poison_state() {
    let (log, _) = with_metrics(|| {
        let path = test_wal_path("metrics_poison_gauge.wal");
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&create_event()).unwrap();
        wal.append(&create_event()).unwrap();

        flip_record_byte(&path, 4);
        recover_wal(&mut wal);
        flip_record_byte(&path, 4);
        recover_wal(&mut wal);
    });

    assert_eq!(log.gauge_sets(WAL_POISONED), vec![1.0, 0.0]);
}

#[test]
fn wal_errors_counted_on_poisoned_append_and_flush() {
    let (log, _) = with_metrics(|| {
        let path = test_wal_path("metrics_wal_errors.wal");
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&create_event()).unwrap();
        wal.append(&create_event()).unwrap();
        flip_record_byte(&path, 4);
        recover_wal(&mut wal);

        // Poisoned: the group-commit path and the AppendAtomic path must each count their
        // failed append and failed flush.
        let mut recording = None;
        let (tx, mut rx) = oneshot::channel();
        let mut batch = vec![(create_event(), tx)];
        flush_and_respond(&mut wal, &mut batch, &mut recording);
        assert!(rx.try_recv().unwrap().is_err());

        let (tx, mut rx) = oneshot::channel();
        let cmd = WalCommand::AppendAtomic { events: vec![create_event()], response: tx };
        handle_non_append(&mut wal, cmd, &mut recording);
        assert!(rx.try_recv().unwrap().is_err());
    });

    assert_eq!(log.counter_total(WAL_ERRORS_TOTAL, &[("kind", "append")]), 2);
    assert_eq!(log.counter_total(WAL_ERRORS_TOTAL, &[("kind", "flush")]), 2);
}
