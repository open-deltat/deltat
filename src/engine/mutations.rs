//! The write path: create, delete, add and remove rules, place holds, and book.
//!
//! Every mutation validates against limits and parent coverage, persists to the WAL, and only
//! then applies to memory, so an fsync failure cannot leave a durable-versus-visible split.
//! Batch bookings and hold commits run under one lock so they are all-or-nothing.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use tokio::sync::{oneshot, RwLock};
use ulid::Ulid;

use crate::limits::*;
use crate::model::*;
use crate::notify::{Ended, HoldEnd, Notice};

use super::availability::subtract_intervals;
use super::conflict::{
    check_batch_capacity, check_no_conflict, check_no_conflict_excluding, check_rules_admit,
    reject_repeats, validate_buffer, validate_capacity, validate_label, validate_span,
    validate_timestamp,
};
use super::offer::{self, Refused};
use super::{Engine, EngineError, WalCommand};

/// The rule-collection window for a write that may need to offer alternatives.
///
/// Identical to the candidate span when not offering, so the non-offering path collects exactly
/// what it collects today. Always a superset of `span`, so a candidate longer than the horizon is
/// still fully covered.
fn offer_window(span: &Span, offer_limit: usize) -> Span {
    if offer_limit == 0 {
        return *span;
    }
    let end = span
        .end
        .max(span.start.saturating_add(COUNTER_OFFER_WINDOW_MS))
        .min(MAX_VALID_TIMESTAMP_MS);
    Span::try_new(span.start, end).unwrap_or(*span)
}

/// The resources a multi-resource write touches, write-locked by `Engine::lock_resources`. Holding
/// them across check, append and apply is what makes the write all-or-nothing to every other writer.
type Locked = BTreeMap<Ulid, tokio::sync::OwnedRwLockWriteGuard<ResourceState>>;

impl Engine {
    pub async fn create_resource(
        &self,
        id: Ulid,
        parent_id: Option<Ulid>,
        name: Option<String>,
        capacity: u32,
        buffer_after: Option<Ms>,
    ) -> Result<(), EngineError> {
        // Serialize against delete (and concurrent creates): the parent-existence and
        // duplicate-id checks below are lock-free, and the WAL fsync sits between them and the
        // index update, so an unserialized delete of the parent in that window would durably
        // orphan this child (see topology_lock).
        let _topology = self.topology_lock.lock().await;
        if self.store.resource_count() >= MAX_RESOURCES_PER_TENANT {
            return Err(EngineError::LimitExceeded("too many resources"));
        }
        validate_capacity(capacity)?;
        validate_buffer(buffer_after)?;
        if let Some(ref n) = name
            && n.len() > MAX_NAME_LEN {
                return Err(EngineError::LimitExceeded("resource name too long"));
            }
        if self.store.contains_resource(&id) {
            return Err(EngineError::AlreadyExists(id));
        }
        if let Some(pid) = parent_id {
            // Cheap checks before the O(depth) walk: self-cycle and parent existence.
            if pid == id {
                return Err(EngineError::CycleDetected(id));
            }
            if !self.store.contains_resource(&pid) {
                return Err(EngineError::NotFound(pid));
            }
            let mut depth = 0usize;
            let mut cur = Some(pid);
            while let Some(cid) = cur {
                depth += 1;
                if depth > MAX_HIERARCHY_DEPTH {
                    return Err(EngineError::LimitExceeded("hierarchy too deep"));
                }
                // Lock-free walk via the parent index: exact (no try_read truncation under
                // contention) and cannot deadlock against a concurrent batch (C1).
                cur = self.store.get_parent(&cid);
            }
        }

        let event = Event::ResourceCreated { id, parent_id, name: name.clone(), capacity, buffer_after };
        self.wal_append(&event).await?;
        let rs = ResourceState::new(id, parent_id, name, capacity, buffer_after);
        self.store.insert_resource(id, Arc::new(RwLock::new(rs)));
        if let Some(pid) = parent_id {
            self.store.add_child(pid, id);
        }
        let notice = Notice::of(&event);
        self.notify.send(id, &notice);
        self.notify_ancestors(parent_id, &notice);
        Ok(())
    }

    /// Create several resources in one request. Each goes through the single-resource path with its
    /// full validation (parent existence, hierarchy depth, cycle, limits). Applied in list order, so
    /// a row may reference a parent created earlier in the same batch. The win is collapsing N client
    /// round-trips into one Command; semantics match the SDK's prior per-row creates, including that
    /// a mid-batch failure leaves earlier resources created.
    pub async fn batch_create_resources(
        &self,
        resources: Vec<ResourceRow>,
    ) -> Result<(), EngineError> {
        if resources.len() > MAX_BATCH_SIZE {
            return Err(EngineError::LimitExceeded("batch too large"));
        }
        for (id, parent_id, name, capacity, buffer_after) in resources {
            self.create_resource(id, parent_id, name, capacity, buffer_after).await?;
        }
        Ok(())
    }

    pub async fn delete_resource(&self, id: Ulid) -> Result<(), EngineError> {
        // Serialize against create: has_children below cannot see a child whose create has
        // passed its parent check but not yet indexed itself, so without this lock the delete
        // and the create both succeed and the child is durably orphaned (see topology_lock).
        let _topology = self.topology_lock.lock().await;
        if !self.store.contains_resource(&id) {
            return Err(EngineError::NotFound(id));
        }
        if self.store.has_children(&id) {
            return Err(EngineError::HasChildren(id));
        }

        let Some(rs) = self.get_resource(&id) else {
            return Err(EngineError::NotFound(id));
        };
        let guard = rs.read().await;
        let parent_id = guard.parent_id;
        // Unmap every entity (rule/hold/booking) this resource owned. Without this the
        // entity->resource index keeps dangling rows that resolve to a resource that no longer
        // exists, so a stale id would resolve past the delete instead of returning NotFound.
        for interval in &guard.intervals {
            self.store.unmap_entity(&interval.id);
        }
        if let Some(pid) = parent_id {
            self.store.remove_child(&pid, &id);
        }
        drop(guard);

        let event = Event::ResourceDeleted { id };
        self.wal_append(&event).await?;
        self.store.remove_resource(&id);
        let notice = Notice::of(&event);
        self.notify.send(id, &notice);
        self.notify_ancestors(parent_id, &notice);
        // Deliver the deletion to current listeners above, then reclaim the channel so a
        // long-lived tenant does not leak one broadcast sender per ever-deleted resource.
        self.notify.remove(&id);
        Ok(())
    }

    /// An entity id is claimed exactly once, across every resource.
    ///
    /// Nothing downstream catches a reuse. `check_no_conflict` skips expired holds and never
    /// runs at all for a disjoint span, `add_rule` has no conflict check, and
    /// `insert_interval` does not dedupe. A client retrying with the same id therefore used to
    /// insert a SECOND interval carrying it; removing one copy unmaps `entity_to_resource` and
    /// strands the other beyond the reach of release, commit, cancel and the reaper, where it
    /// holds a slot against `MAX_INTERVALS_PER_RESOURCE` and replays from the WAL forever.
    ///
    /// Retries are the normal behaviour of the agent clients this is built for, so this is a
    /// live path and not a theoretical one. Call it under the resource write guard so two
    /// concurrent retries on the same resource serialise against each other.
    /// Build the refusal, computing alternatives under the guard that just refused.
    ///
    /// Taking `&ResourceState` (which the guard derefs to) rather than the guard itself is what
    /// keeps this synchronous and lock-free: no ancestor is walked, nothing is awaited, and the
    /// acquisition count on a refused statement is identical to today's. Computing against the
    /// same guard and the same `now` that produced the refusal also means the offer cannot hand
    /// back the very span it just refused.
    #[allow(clippy::too_many_arguments)]
    fn refuse(
        &self,
        error: EngineError,
        rs: &ResourceState,
        span: &Span,
        inherited_nb: &[Span],
        inherited_blocking: &[Span],
        ancestor_has_schedule: bool,
        now: Ms,
        window_end: Ms,
        offer_limit: usize,
    ) -> Refused {
        if offer_limit == 0 || !error.is_offerable() {
            return Refused { error, offer: None };
        }
        let ctx = offer::RuleCtx {
            inherited_non_blocking: inherited_nb,
            inherited_blocking,
            ancestor_has_schedule,
        };
        let offer = offer::rank(rs, span, &ctx, now, window_end, offer_limit);
        crate::observability::record_counter_offer(error.kind(), &offer);
        Refused {
            error,
            offer: Some(Box::new(offer)),
        }
    }

    fn reject_reused_id(&self, id: Ulid) -> Result<(), EngineError> {
        match self.store.get_resource_for_entity(&id) {
            Some(_) => Err(EngineError::AlreadyExists(id)),
            None => Ok(()),
        }
    }

    pub async fn add_rule(
        &self,
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        blocking: bool,
    ) -> Result<(), EngineError> {
        validate_span(&span)?;
        let rs = self
            .get_resource(&resource_id)
            .ok_or(EngineError::NotFound(resource_id))?;
        // Coverage check BEFORE taking the child write guard: check_parent_coverage locks the parent
        // (and its ancestors), and holding the child guard across that is the ABBA half of a deadlock
        // with batch_confirm_bookings (C1). parent_id is immutable, read lock-free.
        if !blocking
            && let Some(parent_id) = self.store.get_parent(&resource_id) {
                self.check_parent_coverage(parent_id, span).await?;
            }

        let mut guard = rs.write().await;
        self.reject_reused_id(id)?;
        if guard.intervals.len() >= MAX_INTERVALS_PER_RESOURCE {
            return Err(EngineError::LimitExceeded("too many intervals on resource"));
        }

        let event = Event::RuleAdded { id, resource_id, span, blocking };
        self.persist_and_apply(resource_id, &mut guard, &event).await
    }

    /// AVAIL-09: a non-blocking rule must lie within the parent's availability, else it would open
    /// time the parent has closed. Blocking rules may close time anywhere and are exempt.
    async fn check_parent_coverage(&self, parent_id: Ulid, span: Span) -> Result<(), EngineError> {
        let parent_free = self
            .compute_availability(parent_id, span.start, span.end, None)
            .await?;
        let uncovered = subtract_intervals(&[span], &parent_free);
        if !uncovered.is_empty() {
            return Err(EngineError::NotCoveredByParent {
                rule_span: span,
                uncovered,
            });
        }
        Ok(())
    }

    /// Add several rules in one request. Rules are independent: they carry no capacity/conflict
    /// interaction with each other (unlike batch bookings), so each is applied via the single-rule
    /// path with its full validation (span, parent coverage, interval limit). The win is collapsing
    /// N client round-trips into one Command; semantics match the SDK's prior per-row inserts,
    /// including that a mid-batch failure leaves earlier rules applied.
    pub async fn batch_add_rules(
        &self,
        rules: Vec<(Ulid, Ulid, Span, bool)>,
    ) -> Result<(), EngineError> {
        if rules.len() > MAX_BATCH_SIZE {
            return Err(EngineError::LimitExceeded("batch too large"));
        }
        for (id, resource_id, span, blocking) in rules {
            self.add_rule(id, resource_id, span, blocking).await?;
        }
        Ok(())
    }

    pub async fn remove_rule(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let (resource_id, mut guard) = self.resolve_entity_write(&id).await?;
        // resolve_entity_write matches any entity kind, so without this a booking/hold id would be
        // removed as if it were a rule. The id must resolve to a rule.
        find_interval_of_kind(&guard, &id, is_rule)?;
        let event = Event::RuleRemoved { id, resource_id };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        Ok(resource_id)
    }

    pub async fn place_hold(
        &self,
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        expires_at: Ms,
    ) -> Result<(), EngineError> {
        self.place_hold_offering(id, resource_id, span, expires_at, 0)
            .await
            .map_err(EngineError::from)
    }

    /// `place_hold`, but a refusal carries up to `offer_limit` spans the caller could take instead.
    ///
    /// `offer_limit == 0` is exactly today's behaviour, which is what `place_hold` delegates with,
    /// so the offer path is additive and the kill switch is a number rather than a code path.
    pub async fn place_hold_offering(
        &self,
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        expires_at: Ms,
        offer_limit: usize,
    ) -> Result<(), Refused> {
        validate_span(&span)?;
        let expires_at = self.hold_expiry(expires_at)?;
        let rs = self
            .get_resource(&resource_id)
            .ok_or(EngineError::NotFound(resource_id))?;
        // T-03 schedule context, collected BEFORE the write guard (holding it while awaiting
        // ancestor locks is the ABBA half of a deadlock, C1); own rules are then read under
        // the guard inside check_rules_admit.
        let parent_id = self.store.get_parent(&resource_id);
        // Widened when offering, so a refusal can enumerate alternatives from rule context that is
        // already being collected here anyway. Admission is unaffected: check_rules_admit ends in
        // subtract_intervals(&[*span], &open), so it depends only on `open` intersected with the
        // candidate, and extra rule coverage outside the candidate cannot change the verdict.
        // Always a superset of `span`, so a candidate longer than the horizon still works.
        let rule_window = offer_window(&span, offer_limit);
        let (inherited_nb, inherited_blocking, ancestor_has_schedule) = self
            .collect_inherited_rules(resource_id, parent_id, &rule_window)
            .await?;
        let mut guard = rs.write().await;
        self.reject_reused_id(id)?;
        if guard.intervals.len() >= MAX_INTERVALS_PER_RESOURCE {
            return Err(EngineError::LimitExceeded("too many intervals on resource").into());
        }

        let now = self.now_ms();
        if let Err(error) =
            check_rules_admit(&guard, &span, &inherited_nb, &inherited_blocking, ancestor_has_schedule)
                .and_then(|()| check_no_conflict(&guard, &span, now))
        {
            return Err(self.refuse(
                error,
                &guard,
                &span,
                &inherited_nb,
                &inherited_blocking,
                ancestor_has_schedule,
                now,
                rule_window.end,
                offer_limit,
            ));
        }

        self.watch_hold_expiry(expires_at);
        let event = Event::HoldPlaced { id, resource_id, span, expires_at };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        metrics::counter!(crate::observability::HOLDS_PLACED_TOTAL).increment(1);
        Ok(())
    }

    /// AVAIL-08: the server clock is the expiry authority. The client's expires_at is a request;
    /// clamping it to now + max_hold_ttl_ms means a skewed or hostile client clock can never park a
    /// hold beyond the operator's ceiling (the reaper only releases holds whose expiry has passed,
    /// so an uncapped far-future hold would squat its span forever).
    fn hold_expiry(&self, requested: Ms) -> Result<Ms, EngineError> {
        validate_timestamp(requested)?;
        Ok(requested.min(self.now_ms().saturating_add(self.max_hold_ttl_ms)))
    }

    /// Called before a hold becomes durable. Lowers the reaper's earliest-expiry watermark so it
    /// will scan once this hold can expire; a removal may leave the bound stale-low, which only
    /// costs a redundant scan, never a missed expiry. Bumps the generation so a reaper scan that
    /// overlaps this placement declines to raise the watermark back over it (see
    /// collect_expired_holds).
    fn watch_hold_expiry(&self, expires_at: Ms) {
        self.earliest_hold_expiry
            .fetch_min(expires_at, std::sync::atomic::Ordering::Relaxed);
        self.hold_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn release_hold(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let resource_id = self.remove_hold(id, HoldEnd::Released).await?;
        metrics::counter!(crate::observability::HOLDS_RELEASED_TOTAL).increment(1);
        Ok(resource_id)
    }

    /// Same removal as `release_hold`, counted as an expiry instead of a release. The reaper
    /// calls this so a reaped hold does not inflate the released count (HOLDS_EXPIRED_TOTAL
    /// documents the abandonment arithmetic that depends on it).
    pub async fn expire_hold(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let resource_id = self.remove_hold(id, HoldEnd::Expired).await?;
        metrics::counter!(crate::observability::HOLDS_EXPIRED_TOTAL).increment(1);
        Ok(resource_id)
    }

    async fn remove_hold(&self, id: Ulid, reason: HoldEnd) -> Result<Ulid, EngineError> {
        let (resource_id, mut guard) = self.resolve_entity_write(&id).await?;
        let span = find_interval_of_kind(&guard, &id, is_hold)?.span;
        let event = Event::HoldReleased { id, resource_id };
        let ended = Ended { span, reason: Some(reason), booking_id: None };
        self.persist_and_apply_ended(resource_id, &mut guard, &event, Some(ended)).await?;
        Ok(resource_id)
    }

    pub async fn commit_hold(
        &self,
        hold_id: Ulid,
        booking_id: Ulid,
        label: Option<String>,
    ) -> Result<(), EngineError> {
        self.commit_holds(vec![(hold_id, booking_id, label)]).await
    }

    pub async fn commit_holds(&self, commits: Vec<(Ulid, Ulid, Option<String>)>) -> Result<(), EngineError> {
        self.commit_holds_offering(commits, 0).await.map_err(EngineError::from)
    }

    /// Turn holds `(hold_id, booking_id, label)` into bookings, all or nothing (AVAIL-07, MCP-K1).
    /// Each booking takes its hold's resource and span, and the holds may sit on different
    /// resources: a camera body, its lens and the crew, or an appointment and the drive to it. One
    /// hold is the same operation with one entry.
    ///
    /// Every resource involved is write-locked before anything is checked, and each hold is
    /// excluded from its own conflict check (it is the caller's reservation), so no competing
    /// booker can win a span between release and booking. The commit is one WAL record, so a crash
    /// keeps all of it or none.
    ///
    /// Only a single hold is offered alternatives, and only when its resource's own schedule is the
    /// whole story (non-blocking rules, no parent): no inherited rule context is collected here,
    /// because collecting it under the guard is the ABBA half of C1 and dropping the guard to collect
    /// would reopen the gap this closes. Every bookable delt.at publishes has that shape. A kit's
    /// alternative is a joint availability question, so a kit is offered none.
    pub async fn commit_holds_offering(
        &self,
        commits: Vec<(Ulid, Ulid, Option<String>)>,
        offer_limit: usize,
    ) -> Result<(), Refused> {
        if commits.is_empty() {
            return Ok(());
        }
        if commits.len() > MAX_BATCH_SIZE {
            return Err(EngineError::LimitExceeded("batch too large").into());
        }
        for (_, _, label) in &commits {
            validate_label(label.as_ref())?;
        }
        reject_repeats(commits.iter().map(|(hold_id, ..)| *hold_id))?;
        reject_repeats(commits.iter().map(|(_, booking_id, _)| *booking_id))?;
        let resource_ids = commits
            .iter()
            .map(|(hold_id, ..)| self.get_resource_for_entity(hold_id).ok_or(EngineError::NotFound(*hold_id)))
            .collect::<Result<Vec<_>, _>>()?;
        let mut locked = self.lock_resources(resource_ids.iter().copied()).await?;

        let now = self.now_ms();
        let offer_limit = if commits.len() == 1 { offer_limit } else { 0 };
        let mut entries = Vec::with_capacity(commits.len());
        for ((hold_id, booking_id, label), resource_id) in commits.into_iter().zip(resource_ids) {
            let guard = &locked[&resource_id];
            // Checked under the lock: an id released, reaped or never a hold is NotFound here even
            // if it resolved above. The booking takes exactly the held span.
            let span = find_interval_of_kind(guard, &hold_id, is_hold)?.span;
            // Caller input, so the reuse hole of confirm_booking applies. Rejected before the
            // append, so the hold survives and a retry with a fresh id still has something to commit.
            self.reject_reused_id(booking_id)?;
            // The batch's other holds stay counted, which is exact: each becomes a booking of the
            // same span, so the load does not change.
            if let Err(error) = check_no_conflict_excluding(guard, &span, now, Some(hold_id)) {
                let can_offer = guard.parent_id.is_none() && guard.has_non_blocking_rule();
                let limit = if can_offer { offer_limit } else { 0 };
                let window_end = offer_window(&span, limit).end;
                return Err(self.refuse(error, guard, &span, &[], &[], false, now, window_end, limit));
            }
            entries.push(HoldCommit { hold_id, booking_id, resource_id, span, label });
        }

        // Applied only once durable, like persist_and_apply.
        self.wal_append(&Event::HoldsCommitted { commits: entries.clone() }).await?;
        for (resource_id, guard) in locked.iter_mut() {
            for commit in entries.iter().filter(|c| c.resource_id == *resource_id) {
                self.apply_commit(guard, commit);
            }
        }
        let count = entries.len() as u64;
        metrics::counter!(crate::observability::HOLDS_COMMITTED_TOTAL).increment(count);
        metrics::counter!(crate::observability::BOOKINGS_CREATED_TOTAL).increment(count);
        Ok(())
    }

    /// Apply one durable commit entry to its resource and publish it. The release names the commit
    /// and its booking, so a subscriber never reports the span as free in the instant before the
    /// booking arrives.
    fn apply_commit(&self, guard: &mut ResourceState, commit: &HoldCommit) {
        let [release, book] = commit.events();
        self.store.apply_event(guard, &release);
        self.store.apply_event(guard, &book);
        let ended = Ended { span: commit.span, reason: Some(HoldEnd::Committed), booking_id: Some(commit.booking_id) };
        for notice in [Notice::ended(&release, ended), Notice::of(&book)] {
            self.notify.send(commit.resource_id, &notice);
            self.notify_ancestors(guard.parent_id, &notice);
        }
    }

    pub async fn confirm_booking(
        &self,
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        label: Option<String>,
    ) -> Result<(), EngineError> {
        self.confirm_booking_offering(id, resource_id, span, label, 0)
            .await
            .map_err(EngineError::from)
    }

    /// `confirm_booking`, but a refusal carries up to `offer_limit` spans the caller could take
    /// instead. See `place_hold_offering`.
    pub async fn confirm_booking_offering(
        &self,
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        label: Option<String>,
        offer_limit: usize,
    ) -> Result<(), Refused> {
        validate_span(&span)?;
        validate_label(label.as_ref())?;
        let rs = self
            .get_resource(&resource_id)
            .ok_or(EngineError::NotFound(resource_id))?;
        // T-03 schedule context, collected BEFORE the write guard (C1); see place_hold.
        let parent_id = self.store.get_parent(&resource_id);
        let rule_window = offer_window(&span, offer_limit);
        let (inherited_nb, inherited_blocking, ancestor_has_schedule) = self
            .collect_inherited_rules(resource_id, parent_id, &rule_window)
            .await?;
        let mut guard = rs.write().await;
        self.reject_reused_id(id)?;
        if guard.intervals.len() >= MAX_INTERVALS_PER_RESOURCE {
            return Err(EngineError::LimitExceeded("too many intervals on resource").into());
        }

        let now = self.now_ms();
        if let Err(error) =
            check_rules_admit(&guard, &span, &inherited_nb, &inherited_blocking, ancestor_has_schedule)
                .and_then(|()| check_no_conflict(&guard, &span, now))
        {
            return Err(self.refuse(
                error,
                &guard,
                &span,
                &inherited_nb,
                &inherited_blocking,
                ancestor_has_schedule,
                now,
                rule_window.end,
                offer_limit,
            ));
        }

        let event = Event::BookingConfirmed { id, resource_id, span, label };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        metrics::counter!(crate::observability::BOOKINGS_CREATED_TOTAL).increment(1);
        Ok(())
    }

    /// Atomically book multiple slots. All-or-nothing: if any booking conflicts,
    /// none are committed. Bookings may span different resources.
    pub async fn batch_confirm_bookings(
        &self,
        bookings: Vec<(Ulid, Ulid, Span, Option<String>)>,
    ) -> Result<(), EngineError> {
        if bookings.is_empty() {
            return Ok(());
        }
        for (.., label) in &bookings {
            validate_label(label.as_ref())?;
        }
        let members: Vec<(Ulid, Ulid, Span)> = bookings.iter().map(|(id, rid, span, _)| (*id, *rid, *span)).collect();
        let mut locked = self.admit_batch(&members).await?;

        // A single append is the all-or-nothing durability boundary (AVAIL-06): the previous
        // per-booking loop did N awaited appends, so a mid-batch WAL error left earlier bookings
        // durable while later ones failed, and each append was a serialized fsync held under every
        // batch resource's write lock.
        let events: Vec<Event> = bookings
            .into_iter()
            .map(|(id, resource_id, span, label)| Event::BookingConfirmed { id, resource_id, span, label })
            .collect();
        self.wal_append_atomic(&events).await?;
        self.apply_and_publish(&mut locked, &events);
        metrics::counter!(crate::observability::BOOKINGS_CREATED_TOTAL)
            .increment(events.len() as u64);
        Ok(())
    }

    /// Place several holds at once, all or nothing, possibly across resources (MCP-K1), so a kit is
    /// held together or not at all. Admission is exactly a batch booking's.
    ///
    /// The holds are separate WAL records under one fsync, so a torn write can keep a prefix of
    /// them. That is safe without a combined record: the caller was never told they were placed,
    /// and every hold expires on its own, so the expiry is the rollback.
    pub async fn batch_place_holds(&self, holds: Vec<(Ulid, Ulid, Span, Ms)>) -> Result<(), EngineError> {
        let holds = holds
            .into_iter()
            .map(|(id, rid, span, expires_at)| Ok((id, rid, span, self.hold_expiry(expires_at)?)))
            .collect::<Result<Vec<_>, EngineError>>()?;
        let Some(earliest) = holds.iter().map(|(.., expires_at)| *expires_at).min() else {
            return Ok(());
        };
        let members: Vec<(Ulid, Ulid, Span)> = holds.iter().map(|(id, rid, span, _)| (*id, *rid, *span)).collect();
        let mut locked = self.admit_batch(&members).await?;

        self.watch_hold_expiry(earliest);
        let events: Vec<Event> = holds
            .into_iter()
            .map(|(id, resource_id, span, expires_at)| Event::HoldPlaced { id, resource_id, span, expires_at })
            .collect();
        self.wal_append_atomic(&events).await?;
        self.apply_and_publish(&mut locked, &events);
        metrics::counter!(crate::observability::HOLDS_PLACED_TOTAL).increment(events.len() as u64);
        Ok(())
    }

    /// Write-lock each resource once, in ascending id order (a `BTreeSet`). Every multi-resource
    /// write takes its locks here, so two batches over overlapping sets cannot deadlock each other.
    async fn lock_resources(&self, resource_ids: impl IntoIterator<Item = Ulid>) -> Result<Locked, EngineError> {
        let mut locked = Locked::new();
        for rid in resource_ids.into_iter().collect::<BTreeSet<_>>() {
            let rs = self.get_resource(&rid).ok_or(EngineError::NotFound(rid))?;
            locked.insert(rid, rs.write_owned().await);
        }
        Ok(locked)
    }

    /// Apply durable single-resource events to the locked resources they name, and publish each
    /// to its resource and that resource's ancestors.
    fn apply_and_publish(&self, locked: &mut Locked, events: &[Event]) {
        for (resource_id, guard) in locked.iter_mut() {
            for event in events.iter().filter(|e| super::event_resource_id(e) == Some(*resource_id)) {
                self.store.apply_event(guard, event);
                let notice = Notice::of(event);
                self.notify.send(*resource_id, &notice);
                self.notify_ancestors(guard.parent_id, &notice);
            }
        }
    }

    /// Lock and admit a batch of new allocations `(id, resource_id, span)`, all or nothing. Every
    /// member must pass the schedule, conflict and capacity checks, against current state and
    /// against the rest of the batch, before the caller writes anything. Batch bookings and batch
    /// holds share this, differing only in the event they persist.
    async fn admit_batch(&self, members: &[(Ulid, Ulid, Span)]) -> Result<Locked, EngineError> {
        if members.len() > MAX_BATCH_SIZE {
            return Err(EngineError::LimitExceeded("batch too large"));
        }
        for (_, _, span) in members {
            validate_span(span)?;
        }
        reject_repeats(members.iter().map(|(id, ..)| *id))?;

        let mut by_resource: BTreeMap<Ulid, Vec<(Ulid, Span)>> = BTreeMap::new();
        for (id, rid, span) in members {
            by_resource.entry(*rid).or_default().push((*id, *span));
        }

        // T-03 schedule context per resource, collected BEFORE any write guard (C1). The hull
        // of a resource's member spans bounds its window: every member lies inside it, so the
        // clamped inherited spans cover each member's admission check.
        let mut rule_ctx: HashMap<Ulid, (Vec<Span>, Vec<Span>, bool)> = HashMap::new();
        for (rid, batch) in &by_resource {
            let lo = batch.iter().map(|(_, s)| s.start).min();
            let hi = batch.iter().map(|(_, s)| s.end).max();
            let (Some(lo), Some(hi)) = (lo, hi) else {
                continue;
            };
            let parent_id = self.store.get_parent(rid);
            rule_ctx.insert(*rid, self.collect_inherited_rules(*rid, parent_id, &Span::new(lo, hi)).await?);
        }

        let locked = self.lock_resources(by_resource.keys().copied()).await?;
        if locked.values().any(|g| g.intervals.len() >= MAX_INTERVALS_PER_RESOURCE) {
            return Err(EngineError::LimitExceeded("too many intervals on resource"));
        }

        // Under the guards so concurrent retries serialise, and before anything is applied so the
        // batch stays all-or-nothing.
        for (id, ..) in members {
            self.reject_reused_id(*id)?;
        }

        // Validate all members against current state + intra-batch.
        let now = self.now_ms();
        for (rid, batch) in &by_resource {
            let guard = &locked[rid];
            let (inherited_nb, inherited_blocking, ancestor_has_schedule) = &rule_ctx[rid];

            for (_, span) in batch {
                check_rules_admit(guard, span, inherited_nb, inherited_blocking, *ancestor_has_schedule)?;
                check_no_conflict(guard, span, now)?;
            }

            if batch.len() > 1 {
                if guard.capacity <= 1 {
                    // Capacity-1: any two overlapping members (with buffer) conflict.
                    let buffer = guard.buffer_after.unwrap_or(0);
                    for i in 0..batch.len() {
                        for j in (i + 1)..batch.len() {
                            let effective_i = Span::new(batch[i].1.start, batch[i].1.end.saturating_add(buffer));
                            if effective_i.overlaps(&batch[j].1) {
                                return Err(EngineError::Conflict(batch[i].0));
                            }
                            let effective_j = Span::new(batch[j].1.start, batch[j].1.end.saturating_add(buffer));
                            if effective_j.overlaps(&batch[i].1) {
                                return Err(EngineError::Conflict(batch[j].0));
                            }
                        }
                    }
                } else {
                    // Capacity-N: overlapping members are allowed up to capacity. Fold them in
                    // with committed load and reject only if concurrency would exceed capacity.
                    let spans: Vec<Span> = batch.iter().map(|(_, s)| *s).collect();
                    check_batch_capacity(guard, &spans, now)?;
                }
            }
        }

        Ok(locked)
    }

    pub async fn cancel_booking(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let (resource_id, mut guard) = self.resolve_entity_write(&id).await?;
        let span = find_interval_of_kind(&guard, &id, is_booking)?.span;
        let event = Event::BookingCancelled { id, resource_id };
        let ended = Ended { span, reason: None, booking_id: None };
        self.persist_and_apply_ended(resource_id, &mut guard, &event, Some(ended)).await?;
        metrics::counter!(crate::observability::BOOKINGS_DELETED_TOTAL).increment(1);
        Ok(resource_id)
    }

    /// Partial update: each argument is `None` when the caller left that column out (unchanged). The
    /// inner Option on the nullable fields is the value to set, so `name = Some(None)` clears the
    /// name while `name = None` leaves it as is. Only mentioned fields are validated and written.
    pub async fn update_resource(
        &self,
        id: Ulid,
        name: Option<Option<String>>,
        capacity: Option<u32>,
        buffer_after: Option<Option<Ms>>,
    ) -> Result<(), EngineError> {
        if let Some(cap) = capacity {
            validate_capacity(cap)?;
        }
        if let Some(buffer) = buffer_after {
            validate_buffer(buffer)?;
        }
        if let Some(Some(ref n)) = name
            && n.len() > MAX_NAME_LEN {
                return Err(EngineError::LimitExceeded("resource name too long"));
            }
        let rs = self
            .get_resource(&id)
            .ok_or(EngineError::NotFound(id))?;
        let mut guard = rs.write().await;

        let event = Event::ResourceUpdated { id, name, capacity, buffer_after };
        self.persist_and_apply(id, &mut guard, &event).await
    }

    pub async fn update_rule(
        &self,
        id: Ulid,
        span: Span,
        blocking: bool,
    ) -> Result<Ulid, EngineError> {
        validate_span(&span)?;
        let resource_id = self
            .get_resource_for_entity(&id)
            .ok_or(EngineError::NotFound(id))?;
        // Same parent-coverage invariant add_rule enforces (else an update could open time the
        // parent has closed), checked BEFORE the child guard to stay ABBA-safe (C1).
        if !blocking
            && let Some(parent_id) = self.store.get_parent(&resource_id) {
                self.check_parent_coverage(parent_id, span).await?;
            }
        let rs = self
            .get_resource(&resource_id)
            .ok_or(EngineError::NotFound(resource_id))?;
        let mut guard = rs.write().await;
        // The id must resolve to a rule; the entity index matches any kind, so without this
        // update_rule(booking_id) would morph a booking into a rule.
        find_interval_of_kind(&guard, &id, is_rule)?;
        let event = Event::RuleUpdated { id, resource_id, span, blocking };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        Ok(resource_id)
    }

    pub fn collect_expired_holds(&self, now: Ms) -> Vec<(Ulid, Ulid)> {
        use std::sync::atomic::Ordering::Relaxed;
        // Skip the whole-tenant scan when no hold can be due yet. The watermark is a lower bound on
        // the earliest live hold's expiry, so `now < watermark` proves nothing is expired.
        if now < self.earliest_hold_expiry.load(Relaxed) {
            return Vec::new();
        }

        // Snapshot the placement generation before scanning. A place_hold that runs during the scan
        // lowers the watermark via fetch_min and bumps this; if we see a bump we must NOT overwrite
        // that lower watermark with our (higher) recomputed bound, or the just-placed hold would sit
        // above the watermark and never be scanned. A plain value compare is insufficient: fetch_min
        // at an equal value proves nothing about intervening placements.
        let gen_before = self.hold_generation.load(Relaxed);

        let mut expired = Vec::new();
        // Recompute the exact next earliest expiry from the live (non-expired) holds we see. If any
        // resource is locked we can't see its holds, so we cannot raise the bound past it. Fall
        // back to i64::MIN to force a scan next cycle rather than risk skipping a due hold.
        let mut next_earliest = i64::MAX;
        let mut had_locked = false;
        for rid in self.store.resource_ids() {
            let Some(rs) = self.store.get_resource(&rid) else {
                continue;
            };
            match rs.try_read() {
                Ok(guard) => {
                    for interval in &guard.intervals {
                        if let IntervalKind::Hold { expires_at } = interval.kind {
                            if expires_at <= now {
                                expired.push((interval.id, guard.id));
                            } else {
                                next_earliest = next_earliest.min(expires_at);
                            }
                        }
                    }
                }
                Err(_) => had_locked = true,
            }
        }
        // Only publish the recomputed bound if no placement raced our scan. If one did, its
        // fetch_min already lowered the watermark to cover its hold; leave that lower value.
        if self.hold_generation.load(Relaxed) == gen_before {
            self.earliest_hold_expiry
                .store(if had_locked { i64::MIN } else { next_earliest }, Relaxed);
        }
        expired
    }

    /// Remove past bookings and expired holds older than `retention_ms`.
    /// Rules are never collected. Skips locked resources (best-effort).
    /// Returns count of collected intervals.
    pub fn gc_past_intervals(&self, now: Ms, retention_ms: Ms) -> usize {
        // retention_ms is operator-configured and unbounded, so subtract saturating: a huge
        // value floors the cutoff at i64::MIN (nothing is older) instead of underflowing.
        let cutoff = now.saturating_sub(retention_ms);
        let mut collected = 0usize;

        for rid in self.store.resource_ids() {
            let rs = match self.store.get_resource(&rid) {
                Some(rs) => rs,
                None => continue,
            };
            let mut guard = match rs.try_write() {
                Ok(g) => g,
                Err(_) => continue,
            };

            // An allocation blocks [start, end + buffer_after) on both the read and the write
            // path, so dominance is decided on the buffered end: collecting on the raw end
            // while the turnaround tail still reaches past `now` would silently open time that
            // admission was rejecting (INV-02). Applied to holds too so the dominance rule
            // stays uniform; an expired hold's tail blocks nothing, the cost is only retention.
            let buffer = guard.buffer_after.unwrap_or(0);
            let mut removed_ids = Vec::new();
            guard.intervals.retain(|interval| {
                let buffered_end = interval.span.end.saturating_add(buffer);
                let dominated = match &interval.kind {
                    IntervalKind::Booking { .. } => buffered_end < cutoff,
                    IntervalKind::Hold { expires_at } => *expires_at <= now && buffered_end < cutoff,
                    IntervalKind::NonBlocking | IntervalKind::Blocking => false,
                };
                if dominated {
                    removed_ids.push(interval.id);
                }
                !dominated
            });

            for id in &removed_ids {
                self.store.unmap_entity(id);
            }
            collected += removed_ids.len();
        }

        metrics::counter!(crate::observability::GC_INTERVALS_COLLECTED_TOTAL)
            .increment(collected as u64);
        collected
    }

    /// Compact the WAL by rewriting it with only the events needed to recreate the current state.
    pub async fn compact_wal(&self) -> Result<(), EngineError> {
        let _serialize = self.compact_lock.lock().await;

        // Open the recording window BEFORE snapshotting: a mutation acked while the snapshot loop
        // runs lands in the old file the swap replaces, so the writer must capture it and carry it
        // into the compacted file, or the acknowledged write is erased (see WalCommand::CompactBegin).
        let (begin_tx, begin_rx) = oneshot::channel();
        self.wal_tx
            .send(WalCommand::CompactBegin { response: begin_tx })
            .await
            .map_err(|_| EngineError::WalError("WAL writer shut down".into()))?;
        begin_rx
            .await
            .map_err(|_| EngineError::WalError("WAL writer dropped response".into()))?;

        // Snapshot each resource under an awaited read lock. A resource mid-mutation holds its
        // write lock across an awaited WAL append, so try_read would fail; unwrapping it panics
        // the compactor and skipping it would drop the resource from the rewritten WAL. Await
        // the lock, copy the state, release, then build the event list outside any lock.
        let mut snapshots: Vec<ResourceSnapshot> = Vec::new();
        for id in self.store.resource_ids() {
            let Some(rs) = self.store.get_resource(&id) else {
                continue;
            };
            let guard = rs.read().await;
            snapshots.push(ResourceSnapshot {
                id: guard.id,
                parent_id: guard.parent_id,
                name: guard.name.clone(),
                capacity: guard.capacity,
                buffer_after: guard.buffer_after,
                intervals: guard.intervals.clone(),
            });
        }

        // Emit ancestors before descendants by tree depth, matching the order the live create path
        // enforces and the original WAL preserved. Replay applies events directly so it tolerates
        // any order, but keeping this order leaves the compacted WAL self-consistent.
        let parent_of: HashMap<Ulid, Option<Ulid>> =
            snapshots.iter().map(|s| (s.id, s.parent_id)).collect();
        snapshots.sort_by_key(|s| resource_depth(s.id, &parent_of));

        let mut events = Vec::new();
        for snap in &snapshots {
            events.push(Event::ResourceCreated {
                id: snap.id,
                parent_id: snap.parent_id,
                name: snap.name.clone(),
                capacity: snap.capacity,
                buffer_after: snap.buffer_after,
            });
            for interval in &snap.intervals {
                events.push(interval_to_event(snap.id, interval));
            }
        }

        let (tx, rx) = oneshot::channel();
        self.wal_tx
            .send(WalCommand::Compact { events, response: tx })
            .await
            .map_err(|_| EngineError::WalError("WAL writer shut down".into()))?;
        rx.await
            .map_err(|_| EngineError::WalError("WAL writer dropped response".into()))?
            .map_err(|e| EngineError::WalError(e.to_string()))
    }

    pub async fn wal_appends_since_compact(&self) -> u64 {
        let (tx, rx) = oneshot::channel();
        if self
            .wal_tx
            .send(WalCommand::AppendsSinceCompact { response: tx })
            .await
            .is_err()
        {
            return 0;
        }
        rx.await.unwrap_or(0)
    }
}

fn is_rule(k: &IntervalKind) -> bool {
    matches!(k, IntervalKind::NonBlocking | IntervalKind::Blocking)
}

fn is_hold(k: &IntervalKind) -> bool {
    matches!(k, IntervalKind::Hold { .. })
}

fn is_booking(k: &IntervalKind) -> bool {
    matches!(k, IntervalKind::Booking { .. })
}

/// Find the interval `id` on `guard` and confirm its kind matches the operation. `resolve_entity_write`
/// maps an id to a resource without checking kind, so a booking id passed to `release_hold` (etc.)
/// would otherwise delete the wrong entity. Returns `NotFound` when the id is absent or mismatched.
fn find_interval_of_kind<'a>(
    guard: &'a ResourceState,
    id: &Ulid,
    is_kind: fn(&IntervalKind) -> bool,
) -> Result<&'a Interval, EngineError> {
    match guard.intervals.iter().find(|i| i.id == *id) {
        Some(i) if is_kind(&i.kind) => Ok(i),
        _ => Err(EngineError::NotFound(*id)),
    }
}

/// A point-in-time copy of a resource's compactable state, taken under a read lock so the
/// rewritten WAL is built without holding any lock.
struct ResourceSnapshot {
    id: Ulid,
    parent_id: Option<Ulid>,
    name: Option<String>,
    capacity: u32,
    buffer_after: Option<Ms>,
    intervals: Vec<Interval>,
}

/// Depth of a resource in the tree (root = 0), used to order ancestors before descendants.
/// Bounded by MAX_HIERARCHY_DEPTH; the tree is acyclic by construction (INV-10).
fn resource_depth(id: Ulid, parent_of: &HashMap<Ulid, Option<Ulid>>) -> usize {
    let mut depth = 0usize;
    let mut current = id;
    while let Some(Some(pid)) = parent_of.get(&current) {
        depth += 1;
        if depth > MAX_HIERARCHY_DEPTH {
            break;
        }
        current = *pid;
    }
    depth
}

/// The single WAL event that recreates one live interval.
fn interval_to_event(resource_id: Ulid, interval: &Interval) -> Event {
    match &interval.kind {
        IntervalKind::NonBlocking => Event::RuleAdded {
            id: interval.id,
            resource_id,
            span: interval.span,
            blocking: false,
        },
        IntervalKind::Blocking => Event::RuleAdded {
            id: interval.id,
            resource_id,
            span: interval.span,
            blocking: true,
        },
        IntervalKind::Hold { expires_at } => Event::HoldPlaced {
            id: interval.id,
            resource_id,
            span: interval.span,
            expires_at: *expires_at,
        },
        IntervalKind::Booking { label } => Event::BookingConfirmed {
            id: interval.id,
            resource_id,
            span: interval.span,
            label: label.clone(),
        },
    }
}
