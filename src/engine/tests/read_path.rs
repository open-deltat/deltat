//! Issue #25: reads no longer wait out a write's fsync. Holds, bookings and commits reach memory
//! before their flush, so these pin what that must never change: nothing frees time before it is
//! durable, subscribers only hear of what is durable, and a failed flush cannot leak into the log.

use std::time::{Duration, Instant};

use crate::clock::now_ms;
use crate::engine::*;
use crate::wal::Wal;

use super::helpers::*;

/// Long enough that a read queued behind the sync, as before #25, cannot answer inside the
/// deadlines below.
const SLOW_SYNC_MS: u64 = 300;
const QUICK: Duration = Duration::from_millis(100);

async fn engine_with_resource(path: &std::path::Path) -> (Arc<Engine>, Ulid) {
    let engine = Arc::new(Engine::new(path.to_path_buf(), Arc::new(NotifyHub::new())).unwrap());
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();
    engine.add_rule(Ulid::new(), rid, Span::new(0, 10_000), false).await.unwrap();
    (engine, rid)
}

fn slow_syncs(engine: &Engine) {
    engine.wal_faults.delay_ms.store(SLOW_SYNC_MS, std::sync::atomic::Ordering::Relaxed);
}

async fn holds(engine: &Engine, rid: Ulid) -> Vec<Ulid> {
    tokio::time::timeout(QUICK, engine.get_holds(rid, &[]))
        .await
        .expect("a read waited on a writer's fsync")
        .unwrap()
        .into_iter()
        .map(|h| h.id)
        .collect()
}

#[tokio::test]
async fn a_hold_is_readable_while_its_sync_is_still_in_flight() {
    let (engine, rid) = engine_with_resource(&test_wal_path("rp_hold_visible.wal")).await;
    slow_syncs(&engine);
    let hid = Ulid::new();
    let placing = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.place_hold(hid, rid, Span::new(1000, 2000), now_ms() + H).await })
    };

    let started = Instant::now();
    while holds(&engine, rid).await.is_empty() {
        assert!(started.elapsed() < Duration::from_millis(200), "the hold was not readable before its sync finished");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!placing.is_finished(), "the read came while the write was still waiting for its sync");
    placing.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_subscriber_hears_of_a_hold_only_once_it_is_on_disk() {
    let (engine, rid) = engine_with_resource(&test_wal_path("rp_hold_notice.wal")).await;
    let mut rx = engine.notify.subscribe(rid);
    slow_syncs(&engine);
    let placing = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.place_hold(Ulid::new(), rid, Span::new(1000, 2000), now_ms() + H).await })
    };

    while holds(&engine, rid).await.is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        matches!(rx.try_recv(), Err(tokio::sync::broadcast::error::TryRecvError::Empty)),
        "a subscriber heard of the hold before it was durable"
    );
    placing.await.unwrap().unwrap();
    assert!(matches!(rx.recv().await.unwrap().event, Event::HoldPlaced { .. }));
}

#[tokio::test]
async fn a_commit_never_shows_its_span_free_before_or_after_it_is_durable() {
    let (engine, rid) = engine_with_resource(&test_wal_path("rp_commit.wal")).await;
    let hid = Ulid::new();
    engine.place_hold(hid, rid, Span::new(1000, 2000), now_ms() + H).await.unwrap();
    slow_syncs(&engine);
    let committing = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.commit_hold(hid, Ulid::new(), None).await })
    };

    while !committing.is_finished() {
        let free = tokio::time::timeout(QUICK, engine.compute_availability(rid, 1000, 2000, None))
            .await
            .expect("a read waited on the commit's fsync")
            .unwrap();
        assert!(free.is_empty(), "the committed span read as free: {free:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    committing.await.unwrap().unwrap();
    assert!(engine.compute_availability(rid, 1000, 2000, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_released_hold_is_never_seen_gone_before_the_release_is_on_disk() {
    // Releases free time, so they keep the old order: durable first, visible after. A read that
    // finds the slot free must find the release already in the log.
    let path = test_wal_path("rp_release.wal");
    let (engine, rid) = engine_with_resource(&path).await;
    let hid = Ulid::new();
    engine.place_hold(hid, rid, Span::new(1000, 2000), now_ms() + H).await.unwrap();
    slow_syncs(&engine);
    let releasing = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.release_hold(hid).await })
    };

    loop {
        if engine.get_holds(rid, &[]).await.unwrap().is_empty() {
            let logged = Wal::replay(&path).unwrap();
            assert!(
                logged.iter().any(|e| matches!(e, Event::HoldReleased { id, .. } if *id == hid)),
                "the slot read as free before its release was durable"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    releasing.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_failed_flush_stops_the_tenant_and_what_failed_never_reaches_disk() {
    let path = test_wal_path("rp_fail_stop.wal");
    let (engine, rid) = engine_with_resource(&path).await;
    let hid = Ulid::new();
    engine.wal_faults.fail_next_flush.store(true, std::sync::atomic::Ordering::Relaxed);

    let err = engine.place_hold(hid, rid, Span::new(1000, 2000), now_ms() + H).await.unwrap_err();
    assert!(matches!(err, EngineError::WalError(_)), "got {err:?}");
    assert!(engine.is_failed());

    // The hold may be in memory, since it was applied before its flush. Nothing may build on it:
    // later writes are refused, and so is a compaction, which would write memory to disk.
    assert!(engine.create_resource(Ulid::new(), None, None, 1, None).await.is_err());
    assert!(engine.compact_wal().await.is_err());
    assert!(
        !Wal::replay(&path).unwrap().iter().any(|e| matches!(e, Event::HoldPlaced { id, .. } if *id == hid)),
        "the failed hold reached the log"
    );

    let rebuilt = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    assert!(rebuilt.get_resource(&rid).is_some(), "acknowledged state survives the rebuild");
    assert!(rebuilt.get_holds(rid, &[]).await.unwrap().is_empty(), "the failed hold does not");
}
