//! The state machine: availability, conflict detection, mutations, and queries.
//!
//! This is the kernel. It owns resource state in memory, serializes every write through a WAL
//! group-commit loop, and rebuilds itself by replay on startup. The submodules split the work:
//! reads compute availability, writes mutate under per-resource locks, and a verification path
//! cross-checks the two.

mod availability;
mod conflict;
mod error;
mod mutations;
mod offer;
mod queries;
mod store;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod verify;

pub use availability::{availability, compute_saturated_spans, merge_overlapping, subtract_intervals};
pub use error::EngineError;
pub use offer::{CounterOffer, Refused};
pub use store::InMemoryStore;

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, RwLock};
use ulid::Ulid;

use crate::clock::{Clock, SystemClock};
use crate::model::*;
use crate::notify::{Ended, Notice, NotifyHub};
use crate::wal::Wal;

pub type SharedResourceState = Arc<RwLock<ResourceState>>;

// ── Group-commit WAL channel ─────────────────────────────

/// What subscribers are told once an event is durable: the notice and every channel it goes to
/// (the resource, then each ancestor). Built under the resource lock and sent by the WAL writer
/// after the flush, so notices go out in log order and never before their event is on disk.
pub(super) struct Announcement {
    pub(super) notice: Notice,
    pub(super) recipients: Vec<Ulid>,
}

pub(super) enum WalCommand {
    Append {
        event: Event,
        announce: Vec<Announcement>,
        response: oneshot::Sender<io::Result<()>>,
    },
    /// Append several events under a single fsync. An fsync error, or a crash before the flush,
    /// leaves none of them durable. They remain independent length+CRC records, so this does NOT
    /// guarantee all-or-none against a torn write between them (a power loss or write error
    /// after one record's bytes reach disk but before the next's): replay discards the torn tail
    /// and keeps the prefix. Use it only where a kept prefix is safe, as for a batch of holds,
    /// which expire on their own. Where it is not, write one record: that is why hold commits are
    /// a single `HoldsCommitted`.
    AppendAtomic {
        events: Vec<Event>,
        announce: Vec<Announcement>,
        response: oneshot::Sender<io::Result<()>>,
    },
    /// Start recording every event acked from here on, until the matching `Compact` arrives.
    /// `compact_wal` sends this BEFORE it snapshots: appends processed after it are fsynced into
    /// the old file (and acked), which the compaction swap replaces, so the `Compact` handler
    /// writes the recording into the new file after the snapshot events. Without it those
    /// acknowledged records would be erased and lost on the next replay. A recorded event may
    /// already be captured in the snapshot (its resource was snapshotted after the apply), so
    /// the handler drops recorded additions whose id the snapshot carries. Interval mutations
    /// acked before this command are always in the snapshot: each queues its record and applies
    /// it under the resource write lock, and the snapshot read waits on that lock.
    /// Begin/Compact pairs never interleave: `compact_wal` serializes under `compact_lock`.
    CompactBegin {
        response: oneshot::Sender<()>,
    },
    Compact {
        events: Vec<Event>,
        response: oneshot::Sender<io::Result<()>>,
    },
    AppendsSinceCompact {
        response: oneshot::Sender<u64>,
    },
}

/// Faults a test can inject into the writer: a delay before each flush, and one failed flush.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct WalFaults {
    pub(crate) delay_ms: std::sync::atomic::AtomicU64,
    pub(crate) fail_next_flush: std::sync::atomic::AtomicBool,
}

/// The background task that owns the WAL. It batches appends for group commit, flushes each batch
/// under one fsync, then announces and acknowledges it, in log order.
///
/// A failed flush stops the tenant for good, as Postgres stops on an fsync failure. Holds, bookings
/// and commits reach memory before their flush (`Engine::persist_early`), so after a failure memory
/// may hold an event the disk does not, and nothing built on it can be trusted. The writer refuses
/// every later write and compaction, and the tenant manager rebuilds the tenant from its log on next
/// use. Compaction is what makes this necessary: it snapshots memory, so without the stop a failed
/// event could still be written to disk by the next one.
struct Writer {
    wal: Wal,
    /// Events acked between CompactBegin and Compact; None outside a compaction window.
    recording: Option<Vec<Event>>,
    notify: Arc<NotifyHub>,
    failed: Arc<AtomicBool>,
    #[cfg(test)]
    faults: Arc<WalFaults>,
}

struct Pending {
    event: Event,
    announce: Vec<Announcement>,
    response: oneshot::Sender<io::Result<()>>,
}

impl Writer {
    /// Block for a command; for an append, also take every append already queued (the batch window),
    /// then flush them as one. A non-append found while draining is handled after the batch.
    async fn run(mut self, mut rx: mpsc::Receiver<WalCommand>) {
        while let Some(cmd) = rx.recv().await {
            let WalCommand::Append { event, announce, response } = cmd else {
                self.handle(cmd).await;
                continue;
            };
            let mut batch = vec![Pending { event, announce, response }];
            let mut after = None;
            while let Ok(next) = rx.try_recv() {
                match next {
                    WalCommand::Append { event, announce, response } => batch.push(Pending { event, announce, response }),
                    other => {
                        after = Some(other);
                        break;
                    }
                }
            }
            self.commit_batch(batch).await;
            if let Some(other) = after {
                self.handle(other).await;
            }
        }
    }

    async fn commit_batch(&mut self, batch: Vec<Pending>) {
        let events: Vec<&Event> = batch.iter().map(|p| &p.event).collect();
        let result = self.write(&events).await;
        if result.is_ok() {
            self.record(events.into_iter().cloned());
            for pending in &batch {
                self.announce(&pending.announce);
            }
        }
        for pending in batch {
            let _ = pending.response.send(copy(&result));
        }
    }

    async fn handle(&mut self, cmd: WalCommand) {
        match cmd {
            WalCommand::AppendAtomic { events, announce, response } => {
                let result = self.write(&events.iter().collect::<Vec<_>>()).await;
                if result.is_ok() {
                    self.record(events.into_iter());
                    self.announce(&announce);
                }
                let _ = response.send(result);
            }
            WalCommand::CompactBegin { response } => {
                self.recording = Some(Vec::new());
                let _ = response.send(());
            }
            WalCommand::Compact { events, response } => {
                let _ = response.send(self.compact(events));
            }
            WalCommand::AppendsSinceCompact { response } => {
                let _ = response.send(self.wal.appends_since_compact());
            }
            WalCommand::Append { .. } => unreachable!("appends are batched by run"),
        }
    }

    /// Write and fsync `events` as one flush, or refuse if the tenant has stopped.
    async fn write(&mut self, events: &[&Event]) -> io::Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(stopped());
        }
        #[cfg(test)]
        {
            let delay = self.faults.delay_ms.load(Ordering::Relaxed);
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
        }
        metrics::histogram!(crate::observability::WAL_FLUSH_BATCH_SIZE).record(events.len() as f64);
        let flush_start = std::time::Instant::now();
        let result = self.append_and_sync(events);
        metrics::histogram!(crate::observability::WAL_FLUSH_DURATION_SECONDS)
            .record(flush_start.elapsed().as_secs_f64());
        if result.is_err() {
            self.stop();
        }
        result
    }

    fn append_and_sync(&mut self, events: &[&Event]) -> io::Result<()> {
        #[cfg(test)]
        if self.faults.fail_next_flush.swap(false, Ordering::Relaxed) {
            return Err(io::Error::other("injected flush failure"));
        }
        let appended = events.iter().try_for_each(|event| {
            self.wal.append_buffered(event).inspect_err(|_| {
                metrics::counter!(crate::observability::WAL_ERRORS_TOTAL, "kind" => "append").increment(1);
            })
        });
        // Always flush, even after an append error, so partially buffered bytes don't leak into a
        // later write (the callers are told this flush failed).
        let flushed = self.wal.flush_sync().inspect_err(|_| {
            metrics::counter!(crate::observability::WAL_ERRORS_TOTAL, "kind" => "flush").increment(1);
        });
        appended.and(flushed)
    }

    /// Stop the tenant after a failed flush. The torn tail is still cut back to the last good
    /// record, so the rebuild that follows replays a clean log.
    fn stop(&mut self) {
        self.failed.store(true, Ordering::Release);
        tracing::error!("WAL flush failed; the tenant refuses writes until it is rebuilt from its log");
        recover_wal(&mut self.wal);
    }

    /// Only acked events are recorded: a failed flush was reported lost to its callers, so
    /// re-appending it into the compacted file would resurrect it.
    fn record(&mut self, events: impl Iterator<Item = Event>) {
        if let Some(recording) = &mut self.recording {
            recording.extend(events);
        }
    }

    fn announce(&self, announce: &[Announcement]) {
        for a in announce {
            for recipient in &a.recipients {
                self.notify.send(*recipient, &a.notice);
            }
        }
    }

    fn compact(&mut self, events: Vec<Event>) -> io::Result<()> {
        let recorded = self.recording.take().unwrap_or_default();
        if self.failed.load(Ordering::Acquire) {
            return Err(stopped());
        }
        let events = merge_recorded(events, recorded);
        // The rewrite and swap run inline in this single writer task, so every write on the
        // tenant queues behind this duration.
        let compact_start = std::time::Instant::now();
        let result = Wal::write_compact_file(self.wal.path(), &events).and_then(|()| self.wal.swap_compact_file());
        metrics::histogram!(crate::observability::WAL_COMPACTION_DURATION_SECONDS)
            .record(compact_start.elapsed().as_secs_f64());
        // A successful swap replaces whatever tail a failed flush left, clearing the poisoned
        // state (see swap_compact_file), so the gauge follows it down.
        if result.is_ok() {
            wal_poisoned_gauge(&self.wal).set(0.0);
        }
        result
    }
}

fn stopped() -> io::Error {
    io::Error::other("this tenant stopped after a failed WAL flush; it is rebuilt from its log on next use")
}

/// One io::Error per receiver (io::Error is not Clone).
fn copy(result: &io::Result<()>) -> io::Result<()> {
    result.as_ref().map(|_| ()).map_err(|e| io::Error::new(e.kind(), e.to_string()))
}

/// A failed flush may leave a torn record on disk. Discard the buffer and cut the file back to the
/// last good record boundary, so the rebuild that follows the stop replays a clean log. A failed
/// recovery leaves the WAL poisoned as well, which the gauge reports.
fn recover_wal(wal: &mut Wal) {
    match wal.recover() {
        Ok(()) => wal_poisoned_gauge(wal).set(0.0),
        Err(e) => {
            tracing::error!("WAL recovery after a flush failure failed: {e}");
            wal_poisoned_gauge(wal).set(1.0);
        }
    }
}

/// The per-tenant poison gauge. The tenant label is the WAL filename's stem: the tenant
/// manager derives the path as `<data_dir>/<tenant>.wal`, and this is the only tenant
/// identity the writer task has.
fn wal_poisoned_gauge(wal: &Wal) -> metrics::Gauge {
    let tenant = wal
        .path()
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    metrics::gauge!(crate::observability::WAL_POISONED, "tenant" => tenant)
}

/// Append the events recorded during the compaction window onto the snapshot events, dropping the
/// ones the snapshot already captured. Only addition events can duplicate: a create replayed twice
/// would reset the resource (wiping the snapshot intervals replayed before it) and an interval
/// added twice would double-count against capacity, so additions whose id the snapshot carries
/// are dropped. Updates, removals, and deletes replay idempotently on top of either state and
/// are kept unconditionally (a removal already reflected in the snapshot replays as a no-op).
///
/// A recorded `HoldsCommitted` is expanded first. The snapshot takes one resource lock at a time,
/// so a commit over A and B can land after A was snapshotted and before B was; replayed whole, it
/// would add B's booking a second time. Expanded, B's booking is an ordinary addition the filter
/// drops. The compacted file is swapped in by rename, so writing the steps rather than the single
/// record loses no crash atomicity.
fn merge_recorded(mut snapshot: Vec<Event>, recorded: Vec<Event>) -> Vec<Event> {
    let snapshot_ids: std::collections::HashSet<Ulid> =
        snapshot.iter().filter_map(added_id).collect();
    snapshot.extend(
        recorded
            .iter()
            .flat_map(Event::per_resource)
            .filter(|event| added_id(event).is_none_or(|id| !snapshot_ids.contains(&id))),
    );
    snapshot
}

/// The id an event introduces, for `merge_recorded`'s duplicate check. Only the four addition
/// kinds introduce state keyed by a fresh id; every other kind mutates or removes existing state.
/// `HoldsCommitted` introduces several, which is why `merge_recorded` expands it before asking.
fn added_id(event: &Event) -> Option<Ulid> {
    match event {
        Event::ResourceCreated { id, .. }
        | Event::RuleAdded { id, .. }
        | Event::HoldPlaced { id, .. }
        | Event::BookingConfirmed { id, .. } => Some(*id),
        Event::ResourceUpdated { .. }
        | Event::ResourceDeleted { .. }
        | Event::RuleUpdated { .. }
        | Event::RuleRemoved { .. }
        | Event::HoldReleased { .. }
        | Event::BookingCancelled { .. }
        | Event::HoldsCommitted { .. } => None,
    }
}

pub struct Engine {
    pub(super) store: InMemoryStore,
    pub(super) wal_tx: mpsc::Sender<WalCommand>,
    pub notify: Arc<NotifyHub>,
    clock: Arc<dyn Clock>,
    /// A conservative lower bound on the earliest live hold's `expires_at`. The reaper skips its
    /// full scan when `now` is below this. `place_hold` lowers it; each full scan recomputes it
    /// exactly. `i64::MIN` means "unknown, scan" (the initial value, so the first reaper cycle and
    /// the cycle after any replay scan normally).
    pub(super) earliest_hold_expiry: std::sync::atomic::AtomicI64,
    /// Bumped by every `place_hold`. `collect_expired_holds` snapshots it before scanning and only
    /// publishes its recomputed (higher) watermark if it is unchanged afterwards, otherwise a
    /// concurrent placement lowered the watermark and must not be clobbered.
    pub(super) hold_generation: std::sync::atomic::AtomicU64,
    /// Ceiling on hold lifetime (AVAIL-08): `place_hold` clamps the requested `expires_at` to
    /// `now + this`, making the server clock the expiry authority.
    max_hold_ttl_ms: Ms,
    /// Serializes `compact_wal` so the writer sees strictly paired CompactBegin/Compact commands.
    /// The compactor and GC tasks both trigger compaction; interleaved pairs would clobber the
    /// recording and could swap a stale snapshot over a newer one.
    pub(super) compact_lock: tokio::sync::Mutex<()>,
    /// Serializes hierarchy-shape mutations (create, delete). Their existence checks are
    /// lock-free and their WAL fsync sits between check and index update, so an unserialized
    /// create(child, parent=P)/delete(P) pair could both succeed and durably orphan the child
    /// (every availability query on it then errors NotFound(P)); the same window let duplicate-id
    /// creates both pass the AlreadyExists check. Only create/delete take this lock, and no
    /// caller holds a resource guard while acquiring it, so it cannot deadlock with the
    /// per-resource locks. DDL is rare; serializing it through the fsync is cheap.
    pub(super) topology_lock: tokio::sync::Mutex<()>,
    /// Set by the WAL writer when a flush fails; the tenant then refuses writes until the tenant
    /// manager rebuilds it from its log (see `Writer`).
    failed: Arc<AtomicBool>,
    #[cfg(test)]
    pub(crate) wal_faults: Arc<WalFaults>,
}

impl Engine {
    pub fn new(wal_path: PathBuf, notify: Arc<NotifyHub>) -> std::io::Result<Self> {
        Self::with_clock(wal_path, notify, Arc::new(SystemClock))
    }

    /// Construct with an explicit clock. Tests and simulations inject a deterministic
    /// clock here; `new` uses the real wall clock.
    pub fn with_clock(
        wal_path: PathBuf,
        notify: Arc<NotifyHub>,
        clock: Arc<dyn Clock>,
    ) -> std::io::Result<Self> {
        let events = Wal::replay(&wal_path)?;
        let wal = Wal::open(&wal_path)?;
        let (wal_tx, wal_rx) = mpsc::channel(4096);
        let failed = Arc::new(AtomicBool::new(false));
        #[cfg(test)]
        let wal_faults = Arc::new(WalFaults::default());
        let writer = Writer {
            wal,
            recording: None,
            notify: notify.clone(),
            failed: failed.clone(),
            #[cfg(test)]
            faults: wal_faults.clone(),
        };
        tokio::spawn(writer.run(wal_rx));

        let store = InMemoryStore::new();
        let engine = Self {
            store,
            wal_tx,
            notify,
            clock,
            earliest_hold_expiry: std::sync::atomic::AtomicI64::new(i64::MIN),
            hold_generation: std::sync::atomic::AtomicU64::new(0),
            max_hold_ttl_ms: crate::limits::DEFAULT_MAX_HOLD_TTL_MS,
            compact_lock: tokio::sync::Mutex::new(()),
            topology_lock: tokio::sync::Mutex::new(()),
            failed,
            #[cfg(test)]
            wal_faults,
        };

        // Replay events: we're the sole owner of these Arcs, so try_read/try_write
        // always succeed instantly (no contention). Never use blocking_read/blocking_write
        // here because this may run inside an async context (e.g. lazy tenant creation).
        for event in &events {
            match event {
                Event::ResourceCreated { id, parent_id, name, capacity, buffer_after } => {
                    let rs = ResourceState::new(*id, *parent_id, name.clone(), *capacity, *buffer_after);
                    engine.store.insert_resource(*id, Arc::new(RwLock::new(rs)));
                    if let Some(pid) = parent_id {
                        engine.store.add_child(*pid, *id);
                    }
                }
                Event::ResourceDeleted { id } => {
                    if let Some(rs) = engine.store.get_resource(id) {
                        let guard = rs.try_read().expect("replay: uncontended read");
                        if let Some(pid) = guard.parent_id {
                            engine.store.remove_child(&pid, id);
                        }
                    }
                    engine.store.remove_resource(id);
                }
                Event::HoldsCommitted { commits } => {
                    let resource_ids: std::collections::BTreeSet<Ulid> =
                        commits.iter().map(|c| c.resource_id).collect();
                    for resource_id in resource_ids {
                        engine.replay_on(resource_id, event);
                    }
                }
                other => {
                    if let Some(resource_id) = event_resource_id(other) {
                        engine.replay_on(resource_id, other);
                    }
                }
            }
        }

        // A WAL written before hierarchy-shape mutations were serialized (topology_lock) can
        // carry a create/delete interleaving that leaves a child whose parent no longer exists;
        // its every availability query would error NotFound(parent) forever. Detach such
        // orphans to roots so they answer queries again; the next compaction snapshots the
        // detached state and makes the repair durable.
        for id in engine.store.resource_ids() {
            let Some(rs) = engine.store.get_resource(&id) else {
                continue;
            };
            let mut guard = rs.try_write().expect("replay: uncontended write");
            if let Some(pid) = guard.parent_id
                && !engine.store.contains_resource(&pid)
            {
                tracing::warn!("replay: detaching orphaned resource {id} from deleted parent {pid}");
                guard.parent_id = None;
                engine.store.detach_parent(&id);
                engine.store.remove_child(&pid, &id);
            }
        }

        Ok(engine)
    }

    /// Apply one replayed event to one resource, if it still exists. Replay owns every lock, so
    /// `try_write` cannot contend.
    fn replay_on(&self, resource_id: Ulid, event: &Event) {
        if let Some(rs) = self.store.get_resource(&resource_id) {
            let mut guard = rs.try_write().expect("replay: uncontended write");
            self.store.apply_event(&mut guard, event);
        }
    }

    /// Override the hold-lifetime ceiling (AVAIL-08). Applies to future `place_hold` calls only;
    /// replayed events keep the expiry they were durably written with.
    pub fn with_max_hold_ttl(mut self, max_hold_ttl_ms: Ms) -> Self {
        self.max_hold_ttl_ms = max_hold_ttl_ms;
        self
    }

    /// Whether a failed WAL flush stopped this tenant. The tenant manager rebuilds it from its log.
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Hand a command to the WAL writer and return where its outcome will arrive. Refused once the
    /// tenant has stopped, so nothing is queued, or applied early, on top of a failed flush.
    pub(super) async fn queue(
        &self,
        command: impl FnOnce(oneshot::Sender<io::Result<()>>) -> WalCommand,
    ) -> Result<oneshot::Receiver<io::Result<()>>, EngineError> {
        if self.is_failed() {
            return Err(EngineError::WalError(stopped().to_string()));
        }
        let (tx, rx) = oneshot::channel();
        self.wal_tx.send(command(tx)).await.map_err(|_| {
            self.failed.store(true, Ordering::Release);
            EngineError::WalError("WAL writer shut down".into())
        })?;
        Ok(rx)
    }

    /// Wait for a queued command to be durable.
    pub(super) async fn durable(outcome: oneshot::Receiver<io::Result<()>>) -> Result<(), EngineError> {
        outcome
            .await
            .map_err(|_| EngineError::WalError("WAL writer dropped response".into()))?
            .map_err(|e| EngineError::WalError(e.to_string()))
    }

    /// Write one event and wait until it is durable. The caller applies and announces it after.
    async fn wal_append(&self, event: &Event) -> Result<(), EngineError> {
        let outcome = self
            .queue(|response| WalCommand::Append { event: event.clone(), announce: Vec::new(), response })
            .await?;
        Self::durable(outcome).await
    }

    pub fn get_resource(&self, id: &Ulid) -> Option<SharedResourceState> {
        self.store.get_resource(id)
    }

    pub fn get_resource_for_entity(&self, entity_id: &Ulid) -> Option<Ulid> {
        self.store.get_resource_for_entity(entity_id)
    }

    /// Current time in UTC Unix milliseconds, taken from the injected clock, the single
    /// point through which the whole engine reads "now".
    pub fn now_ms(&self) -> Ms {
        self.clock.now_ms()
    }

    /// Persist, apply and announce an event, holding the resource lock throughout: the event is on
    /// disk before anyone can see it. Used for everything that can free time or reshape a resource
    /// (releases, cancellations, rules, resources), where a reader seeing it early could see time
    /// free that a crash would take back. Allocations take `persist_early` instead.
    pub(super) async fn persist_and_apply(
        &self,
        resource_id: Ulid,
        rs: &mut ResourceState,
        event: &Event,
    ) -> Result<(), EngineError> {
        self.persist_and_apply_ended(resource_id, rs, event, None).await
    }

    /// `persist_and_apply` for an event that ends a hold or booking: `ended` goes out on the
    /// notification (never into the WAL) so subscribers learn when it was and why it ended.
    pub(super) async fn persist_and_apply_ended(
        &self,
        resource_id: Ulid,
        rs: &mut ResourceState,
        event: &Event,
        ended: Option<Ended>,
    ) -> Result<(), EngineError> {
        self.wal_append(event).await?;
        self.store.apply_event(rs, event);
        let notice = match ended {
            Some(ended) => Notice::ended(event, ended),
            None => Notice::of(event),
        };
        self.publish(resource_id, rs.parent_id, &notice);
        Ok(())
    }

    /// Who hears about a change to a resource: the resource itself, then each ancestor up to the
    /// root. The walk reads the lock-free parent index, so it never truncates under contention and
    /// cannot deadlock (C1). The depth bound guards against a corrupt or cyclic index.
    pub(super) fn recipients(&self, resource_id: Ulid, parent_id: Option<Ulid>) -> Vec<Ulid> {
        std::iter::once(resource_id)
            .chain(
                std::iter::successors(parent_id, |id| self.store.get_parent(id))
                    .take(crate::limits::MAX_HIERARCHY_DEPTH + 1),
            )
            .collect()
    }

    pub(super) fn publish(&self, resource_id: Ulid, parent_id: Option<Ulid>, notice: &Notice) {
        for recipient in self.recipients(resource_id, parent_id) {
            self.notify.send(recipient, notice);
        }
    }

    /// A notice the WAL writer sends once the event it describes is durable.
    pub(super) fn announcement(&self, resource_id: Ulid, parent_id: Option<Ulid>, notice: Notice) -> Announcement {
        Announcement { recipients: self.recipients(resource_id, parent_id), notice }
    }

    /// Lookup entity → resource, get resource, acquire write lock.
    pub(super) async fn resolve_entity_write(
        &self,
        entity_id: &Ulid,
    ) -> Result<(Ulid, tokio::sync::OwnedRwLockWriteGuard<ResourceState>), EngineError> {
        let resource_id = self
            .get_resource_for_entity(entity_id)
            .ok_or(EngineError::NotFound(*entity_id))?;
        let rs = self
            .get_resource(&resource_id)
            .ok_or(EngineError::NotFound(resource_id))?;
        let guard = rs.write_owned().await;
        Ok((resource_id, guard))
    }
}

/// The one resource an event applies to. None for create/delete, which replay handles itself, and
/// for `HoldsCommitted`, which has several and is routed to each by replay.
fn event_resource_id(event: &Event) -> Option<Ulid> {
    match event {
        Event::RuleAdded { resource_id, .. }
        | Event::RuleUpdated { resource_id, .. }
        | Event::RuleRemoved { resource_id, .. }
        | Event::HoldPlaced { resource_id, .. }
        | Event::HoldReleased { resource_id, .. }
        | Event::BookingConfirmed { resource_id, .. }
        | Event::BookingCancelled { resource_id, .. } => Some(*resource_id),
        Event::ResourceUpdated { id, .. } => Some(*id),
        Event::ResourceCreated { .. } | Event::ResourceDeleted { .. } | Event::HoldsCommitted { .. } => None,
    }
}
