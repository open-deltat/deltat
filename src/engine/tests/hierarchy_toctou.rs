use crate::engine::*;
use super::helpers::*;

// ── Hierarchy create/delete TOCTOU (durable orphan) ─────────────

#[tokio::test]
async fn replay_detaches_a_child_whose_parent_was_deleted() {
    // A pre-fix create(child, parent=P)/delete(P) race could durably commit both events. Such a
    // WAL is forever: replay must not reconstruct a child whose every availability query errors
    // NotFound(P). The orphan is detached to a root at startup; compaction then persists that.
    let path = test_wal_path("replay_orphan.wal");
    let p = Ulid::new();
    let c = Ulid::new();
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&Event::ResourceCreated { id: p, parent_id: None, name: None, capacity: 1, buffer_after: None }).unwrap();
        wal.append(&Event::ResourceCreated { id: c, parent_id: Some(p), name: None, capacity: 1, buffer_after: None }).unwrap();
        wal.append(&Event::ResourceDeleted { id: p }).unwrap();
    }

    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    assert!(engine.get_resource(&c).is_some());
    assert!(engine.get_resource(&p).is_none());

    // The orphan answers availability instead of erroring forever.
    engine
        .compute_availability(c, 0, 100_000, None)
        .await
        .expect("orphaned child must answer availability after replay");

    // Detached means root: the listing shows no dangling parent link.
    let info = engine.list_resources().await.into_iter().find(|r| r.id == c).unwrap();
    assert_eq!(info.parent_id, None, "orphan must be detached to a root");

    // And it is a functioning root: rules admit without a parent-coverage walk to a ghost.
    engine
        .add_rule(Ulid::new(), c, Span::new(0, 10_000), false)
        .await
        .expect("detached orphan must accept rules as a root");
}

#[tokio::test]
async fn replay_tolerates_a_create_after_its_parents_delete() {
    // Same orphan, other WAL order: the child's create lands after the parent's delete, so at
    // replay time the parent never exists. The child must still come up detached and answering.
    let path = test_wal_path("replay_orphan_late_create.wal");
    let p = Ulid::new();
    let c = Ulid::new();
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&Event::ResourceCreated { id: p, parent_id: None, name: None, capacity: 1, buffer_after: None }).unwrap();
        wal.append(&Event::ResourceDeleted { id: p }).unwrap();
        wal.append(&Event::ResourceCreated { id: c, parent_id: Some(p), name: None, capacity: 1, buffer_after: None }).unwrap();
    }

    let engine = Engine::new(path, Arc::new(NotifyHub::new())).unwrap();
    engine
        .compute_availability(c, 0, 100_000, None)
        .await
        .expect("orphaned child must answer availability after replay");
    let info = engine.list_resources().await.into_iter().find(|r| r.id == c).unwrap();
    assert_eq!(info.parent_id, None);
}

#[tokio::test]
async fn create_delete_race_never_leaves_an_orphan() {
    // create(child, parent=P) races delete(P): create checks the parent lock-free, then awaits
    // the WAL fsync before indexing the child, so an unserialized delete slid into that window
    // and both succeeded, leaving a durable orphan. With hierarchy-shape mutations serialized,
    // exactly one side wins: child and parent either both exist or both do not.
    let path = test_wal_path("create_delete_race.wal");
    let engine = Arc::new(Engine::new(path, Arc::new(NotifyHub::new())).unwrap());

    for round in 0..100 {
        let p = Ulid::new();
        engine.create_resource(p, None, None, 1, None).await.unwrap();
        let c = Ulid::new();

        let create = tokio::spawn({
            let engine = engine.clone();
            async move { engine.create_resource(c, Some(p), None, 1, None).await }
        });
        let delete = tokio::spawn({
            let engine = engine.clone();
            async move { engine.delete_resource(p).await }
        });
        let created = create.await.unwrap().is_ok();
        let deleted = delete.await.unwrap().is_ok();

        let child_exists = engine.get_resource(&c).is_some();
        let parent_exists = engine.get_resource(&p).is_some();
        assert_eq!(
            child_exists, parent_exists,
            "round {round}: orphan state (child={child_exists}, parent={parent_exists}, \
             create_ok={created}, delete_ok={deleted})"
        );
        if child_exists {
            engine
                .compute_availability(c, 0, 1000, None)
                .await
                .expect("child availability must not error");
            engine.delete_resource(c).await.unwrap();
            engine.delete_resource(p).await.unwrap();
        }
    }
}
