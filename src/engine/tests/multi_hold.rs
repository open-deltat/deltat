//! MCP-K1: holding several resources at once and committing them together, all or nothing. A camera
//! kit (body, lens, crew) or an appointment plus its travel either all happen or none do.

use crate::engine::*;
use crate::clock::{now_ms, TestClock};
use crate::notify::{Ended, HoldEnd};
use crate::wal::Wal;
use super::helpers::*;

async fn engine_with(name: &str, capacities: &[u32]) -> (Engine, Vec<Ulid>) {
    let engine = Engine::new(test_wal_path(name), Arc::new(NotifyHub::new())).unwrap();
    let mut ids = Vec::with_capacity(capacities.len());
    for &capacity in capacities {
        let rid = Ulid::new();
        engine.create_resource(rid, None, None, capacity, None).await.unwrap();
        ids.push(rid);
    }
    (engine, ids)
}

async fn held(engine: &Engine, rid: Ulid) -> Vec<Ulid> {
    engine.get_holds(rid, &[]).await.unwrap().into_iter().map(|h| h.id).collect()
}

async fn booked(engine: &Engine, rid: Ulid) -> Vec<Ulid> {
    engine.get_bookings(rid, &[]).await.unwrap().into_iter().map(|b| b.id).collect()
}

// ── Placing several holds at once ────────────────────────────────

#[tokio::test]
async fn batch_holds_place_one_hold_on_each_resource() {
    let (engine, r) = engine_with("mh_place_all.wal", &[1, 1, 1]).await;
    let (body, lens, crew) = (Ulid::new(), Ulid::new(), Ulid::new());
    let span = Span::new(1000, 2000);
    let far = now_ms() + H;

    engine
        .batch_place_holds(vec![(body, r[0], span, far), (lens, r[1], span, far), (crew, r[2], span, far)])
        .await
        .unwrap();

    assert_eq!(held(&engine, r[0]).await, vec![body]);
    assert_eq!(held(&engine, r[1]).await, vec![lens]);
    assert_eq!(held(&engine, r[2]).await, vec![crew]);
}

#[tokio::test]
async fn batch_holds_place_none_when_one_resource_is_taken() {
    let (engine, r) = engine_with("mh_place_none.wal", &[1, 1]).await;
    let span = Span::new(1000, 2000);
    engine.confirm_booking(Ulid::new(), r[1], span, None).await.unwrap();

    let err = engine
        .batch_place_holds(vec![(Ulid::new(), r[0], span, now_ms() + H), (Ulid::new(), r[1], span, now_ms() + H)])
        .await
        .unwrap_err();

    assert!(matches!(err, EngineError::Conflict(_)), "got {err:?}");
    assert!(held(&engine, r[0]).await.is_empty(), "the free resource must not keep a hold");
}

#[tokio::test]
async fn batch_holds_overlapping_on_one_capacity_one_resource_conflict() {
    let (engine, r) = engine_with("mh_place_overlap.wal", &[1]).await;
    let err = engine
        .batch_place_holds(vec![
            (Ulid::new(), r[0], Span::new(1000, 2000), now_ms() + H),
            (Ulid::new(), r[0], Span::new(1500, 2500), now_ms() + H),
        ])
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "got {err:?}");
    assert!(held(&engine, r[0]).await.is_empty());
}

#[tokio::test]
async fn batch_holds_reject_an_id_listed_twice() {
    let (engine, r) = engine_with("mh_place_dup.wal", &[1, 1]).await;
    let id = Ulid::new();
    let err = engine
        .batch_place_holds(vec![
            (id, r[0], Span::new(1000, 2000), now_ms() + H),
            (id, r[1], Span::new(1000, 2000), now_ms() + H),
        ])
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::AlreadyExists(dup) if dup == id), "got {err:?}");
    assert!(held(&engine, r[0]).await.is_empty());
    assert!(held(&engine, r[1]).await.is_empty());
}

#[tokio::test]
async fn batch_holds_clamp_expiry_to_the_operator_ceiling() {
    let clock = Arc::new(TestClock::new(1_000_000));
    let engine = Engine::with_clock(test_wal_path("mh_place_ttl.wal"), Arc::new(NotifyHub::new()), clock)
        .unwrap()
        .with_max_hold_ttl(90_000);
    let rid = Ulid::new();
    engine.create_resource(rid, None, None, 1, None).await.unwrap();

    engine
        .batch_place_holds(vec![(Ulid::new(), rid, Span::new(5_000_000, 6_000_000), 1_000_000 + 24 * H)])
        .await
        .unwrap();

    let holds = engine.get_holds(rid, &[]).await.unwrap();
    assert_eq!(holds[0].expires_at, 1_000_000 + 90_000, "a client cannot park a hold past the ceiling");
}

#[tokio::test]
async fn batch_holds_survive_replay() {
    let path = test_wal_path("mh_place_replay.wal");
    let (a, b) = (Ulid::new(), Ulid::new());
    let (ha, hb) = (Ulid::new(), Ulid::new());
    {
        let engine = Engine::new(path.clone(), Arc::new(NotifyHub::new())).unwrap();
        engine.create_resource(a, None, None, 1, None).await.unwrap();
        engine.create_resource(b, None, None, 1, None).await.unwrap();
        let span = Span::new(1000, 2000);
        engine.batch_place_holds(vec![(ha, a, span, now_ms() + H), (hb, b, span, now_ms() + H)]).await.unwrap();
    }
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    assert_eq!(held(&engine, a).await, vec![ha]);
    assert_eq!(held(&engine, b).await, vec![hb]);
}

// ── Committing several holds at once ─────────────────────────────

async fn hold_each(engine: &Engine, resources: &[Ulid], span: Span) -> Vec<Ulid> {
    let holds: Vec<(Ulid, Ulid, Span, Ms)> =
        resources.iter().map(|&rid| (Ulid::new(), rid, span, now_ms() + H)).collect();
    let ids = holds.iter().map(|(id, ..)| *id).collect();
    engine.batch_place_holds(holds).await.unwrap();
    ids
}

#[tokio::test]
async fn commit_holds_books_every_resource_and_releases_every_hold() {
    let (engine, r) = engine_with("mh_commit_all.wal", &[1, 1]).await;
    let h = hold_each(&engine, &r, Span::new(1000, 2000)).await;
    let (b0, b1) = (Ulid::new(), Ulid::new());

    engine
        .commit_holds(vec![(h[0], b0, Some("body".into())), (h[1], b1, Some("lens".into()))])
        .await
        .unwrap();

    for (rid, bid, label) in [(r[0], b0, "body"), (r[1], b1, "lens")] {
        assert!(held(&engine, rid).await.is_empty());
        let bookings = engine.get_bookings(rid, &[]).await.unwrap();
        assert_eq!(bookings.len(), 1);
        assert_eq!(bookings[0].id, bid);
        assert_eq!((bookings[0].start, bookings[0].end), (1000, 2000), "a booking takes its hold's span");
        assert_eq!(bookings[0].label.as_deref(), Some(label));
    }
}

#[tokio::test]
async fn commit_holds_books_nothing_when_one_hold_lost_its_span() {
    let (engine, r) = engine_with("mh_commit_none.wal", &[1, 1]).await;
    let span = Span::new(1000, 2000);
    let keep = Ulid::new();
    let lapsed = Ulid::new();
    engine.place_hold(keep, r[0], span, now_ms() + H).await.unwrap();
    // Already expired, so it no longer protects its span and a competitor takes it.
    engine.place_hold(lapsed, r[1], span, 1).await.unwrap();
    engine.confirm_booking(Ulid::new(), r[1], span, None).await.unwrap();

    let err = engine
        .commit_holds(vec![(keep, Ulid::new(), None), (lapsed, Ulid::new(), None)])
        .await
        .unwrap_err();

    assert!(matches!(err, EngineError::Conflict(_)), "got {err:?}");
    assert_eq!(held(&engine, r[0]).await, vec![keep], "the healthy hold stays held, uncommitted");
    assert!(booked(&engine, r[0]).await.is_empty());
    assert_eq!(booked(&engine, r[1]).await.len(), 1, "only the competitor's booking");
}

#[tokio::test]
async fn commit_holds_refuses_the_whole_batch_for_an_unknown_hold() {
    let (engine, r) = engine_with("mh_commit_unknown.wal", &[1]).await;
    let h = hold_each(&engine, &r, Span::new(1000, 2000)).await;
    let ghost = Ulid::new();

    let err = engine
        .commit_holds(vec![(h[0], Ulid::new(), None), (ghost, Ulid::new(), None)])
        .await
        .unwrap_err();

    assert!(matches!(err, EngineError::NotFound(id) if id == ghost), "got {err:?}");
    assert_eq!(held(&engine, r[0]).await, h);
    assert!(booked(&engine, r[0]).await.is_empty());
}

#[tokio::test]
async fn commit_holds_refuses_a_booking_id_given_as_a_hold() {
    let (engine, r) = engine_with("mh_commit_wrongkind.wal", &[1, 1]).await;
    let h = hold_each(&engine, &r[..1], Span::new(1000, 2000)).await;
    let booking = Ulid::new();
    engine.confirm_booking(booking, r[1], Span::new(1000, 2000), None).await.unwrap();

    let err = engine
        .commit_holds(vec![(h[0], Ulid::new(), None), (booking, Ulid::new(), None)])
        .await
        .unwrap_err();

    assert!(matches!(err, EngineError::NotFound(id) if id == booking), "got {err:?}");
    assert_eq!(held(&engine, r[0]).await, h);
}

#[tokio::test]
async fn commit_holds_rejects_a_hold_listed_twice() {
    let (engine, r) = engine_with("mh_commit_dup_hold.wal", &[1]).await;
    let h = hold_each(&engine, &r, Span::new(1000, 2000)).await;

    let err = engine
        .commit_holds(vec![(h[0], Ulid::new(), None), (h[0], Ulid::new(), None)])
        .await
        .unwrap_err();

    assert!(matches!(err, EngineError::AlreadyExists(id) if id == h[0]), "got {err:?}");
    assert_eq!(held(&engine, r[0]).await, h);
    assert!(booked(&engine, r[0]).await.is_empty());
}

#[tokio::test]
async fn commit_holds_rejects_a_booking_id_listed_twice_or_already_live() {
    let (engine, r) = engine_with("mh_commit_dup_booking.wal", &[1, 1, 1]).await;
    let h = hold_each(&engine, &r[..2], Span::new(1000, 2000)).await;
    let twice = Ulid::new();
    let err = engine
        .commit_holds(vec![(h[0], twice, None), (h[1], twice, None)])
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::AlreadyExists(id) if id == twice), "got {err:?}");

    let live = Ulid::new();
    engine.confirm_booking(live, r[2], Span::new(5000, 6000), None).await.unwrap();
    let err = engine
        .commit_holds(vec![(h[0], Ulid::new(), None), (h[1], live, None)])
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::AlreadyExists(id) if id == live), "got {err:?}");

    assert_eq!(held(&engine, r[0]).await, vec![h[0]]);
    assert_eq!(held(&engine, r[1]).await, vec![h[1]]);
}

#[tokio::test]
async fn commit_holds_two_holds_on_one_capacity_two_resource() {
    let (engine, r) = engine_with("mh_commit_cap2.wal", &[2]).await;
    let span = Span::new(1000, 2000);
    let (a, b) = (Ulid::new(), Ulid::new());
    engine.batch_place_holds(vec![(a, r[0], span, now_ms() + H), (b, r[0], span, now_ms() + H)]).await.unwrap();

    engine.commit_holds(vec![(a, Ulid::new(), None), (b, Ulid::new(), None)]).await.unwrap();

    assert!(held(&engine, r[0]).await.is_empty());
    assert_eq!(booked(&engine, r[0]).await.len(), 2);
    let err = engine.confirm_booking(Ulid::new(), r[0], span, None).await.unwrap_err();
    assert!(matches!(err, EngineError::CapacityExceeded(_) | EngineError::Conflict(_)), "got {err:?}");
}

#[tokio::test]
async fn commit_holds_survives_replay() {
    let path = test_wal_path("mh_commit_replay.wal");
    let (a, b) = (Ulid::new(), Ulid::new());
    let (ba, bb) = (Ulid::new(), Ulid::new());
    {
        let engine = Engine::new(path.clone(), Arc::new(NotifyHub::new())).unwrap();
        engine.create_resource(a, None, None, 1, None).await.unwrap();
        engine.create_resource(b, None, None, 1, None).await.unwrap();
        let h = hold_each(&engine, &[a, b], Span::new(1000, 2000)).await;
        engine.commit_holds(vec![(h[0], ba, None), (h[1], bb, None)]).await.unwrap();
    }
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    assert!(held(&engine, a).await.is_empty());
    assert!(held(&engine, b).await.is_empty());
    assert_eq!(booked(&engine, a).await, vec![ba]);
    assert_eq!(booked(&engine, b).await, vec![bb]);
}

#[tokio::test]
async fn a_torn_multi_commit_books_nothing_and_keeps_every_hold() {
    // The whole commit is one WAL record. A torn write therefore loses all of it: every hold is still
    // there (and will expire on its own), no booking exists. Never body booked and lens free.
    let path = test_wal_path("mh_commit_torn.wal");
    let (a, b, c) = (Ulid::new(), Ulid::new(), Ulid::new());
    let holds = {
        let engine = Engine::new(path.clone(), Arc::new(NotifyHub::new())).unwrap();
        for rid in [a, b, c] {
            engine.create_resource(rid, None, None, 1, None).await.unwrap();
        }
        let h = hold_each(&engine, &[a, b, c], Span::new(1000, 2000)).await;
        engine
            .commit_holds(h.iter().map(|&hid| (hid, Ulid::new(), None)).collect())
            .await
            .unwrap();
        h
    };

    let len = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(len - 8).unwrap();
    drop(file);

    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    for (rid, hid) in [a, b, c].into_iter().zip(holds) {
        assert!(booked(&engine, rid).await.is_empty(), "a torn commit must book nothing");
        assert_eq!(held(&engine, rid).await, vec![hid], "a torn commit must release nothing");
    }
}

#[tokio::test]
async fn commit_holds_notifies_each_resource_like_a_single_commit() {
    let (engine, r) = engine_with("mh_commit_notify.wal", &[1, 1]).await;
    let span = Span::new(10, 20);
    let h = hold_each(&engine, &r, span).await;
    let mut listeners = [engine.notify.subscribe(r[0]), engine.notify.subscribe(r[1])];
    let (b0, b1) = (Ulid::new(), Ulid::new());

    engine.commit_holds(vec![(h[0], b0, None), (h[1], b1, None)]).await.unwrap();

    for (rx, bid) in listeners.iter_mut().zip([b0, b1]) {
        let released = rx.recv().await.unwrap();
        assert!(matches!(released.event(), Event::HoldReleased { .. }), "got {:?}", released.event());
        assert_eq!(
            released.ended,
            Some(Ended { span, reason: Some(HoldEnd::Committed), booking_id: Some(bid) }),
            "the release names the commit, so no listener reads the span as free"
        );
        let confirmed = rx.recv().await.unwrap();
        assert!(matches!(confirmed.event(), Event::BookingConfirmed { id, .. } if *id == bid));
    }
}

// ── Compaction ───────────────────────────────────────────────────

#[tokio::test]
async fn a_commit_recorded_during_compaction_never_books_twice() {
    // Compaction snapshots resources one lock at a time, and a commit acked meanwhile is recorded and
    // replayed after the snapshot. A commit over A and B can land after A was snapshotted and before
    // B was, so the snapshot holds A's hold and B's booking. Replaying the recorded commit whole would
    // insert B's booking a second time and double-count it against capacity.
    let (a, b) = (Ulid::new(), Ulid::new());
    let (ha, hb, ba, bb) = (Ulid::new(), Ulid::new(), Ulid::new(), Ulid::new());
    let span = Span::new(1000, 2000);
    let far = now_ms() + H;
    let created = |id| Event::ResourceCreated { id, parent_id: None, name: None, capacity: 1, buffer_after: None };
    let snapshot = vec![
        created(a),
        created(b),
        Event::HoldPlaced { id: ha, resource_id: a, span, expires_at: far },
        Event::BookingConfirmed { id: bb, resource_id: b, span, label: None },
    ];
    let commit = |hold_id, booking_id, resource_id| HoldCommit { hold_id, booking_id, resource_id, span, label: None };
    let recorded = vec![Event::HoldsCommitted { commits: vec![commit(ha, ba, a), commit(hb, bb, b)] }];

    let merged = merge_recorded(snapshot, recorded);

    let path = test_wal_path("mh_compaction_merge.wal");
    Wal::open(&path).unwrap().compact(&merged).unwrap();
    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    assert_eq!(booked(&engine, a).await, vec![ba]);
    assert!(held(&engine, a).await.is_empty());
    assert_eq!(booked(&engine, b).await, vec![bb], "B's booking exactly once");
}
