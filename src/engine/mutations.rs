//! The write path: create, delete, add and remove rules, place holds, and book.
//!
//! Every mutation validates against limits and parent coverage, persists to the WAL, and only
//! then applies to memory, so an fsync failure cannot leave a durable-versus-visible split.
//! Batch bookings and hold commits run under one lock so they are all-or-nothing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{oneshot, RwLock};
use ulid::Ulid;

use crate::limits::*;
use crate::model::*;

use super::availability::subtract_intervals;
use super::conflict::{
    check_batch_capacity, check_no_conflict, check_no_conflict_excluding, check_rules_admit,
    validate_buffer, validate_capacity, validate_span, validate_timestamp,
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
        self.notify.send(id, &event);
        self.notify_ancestors(parent_id, &event);
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
        self.notify.send(id, &event);
        self.notify_ancestors(parent_id, &event);
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
        validate_timestamp(expires_at)?;
        // AVAIL-08: the server clock is the expiry authority. The client's expires_at is a
        // request; clamping it to now + max_hold_ttl_ms means a skewed or hostile client clock
        // can never park a hold beyond the operator's ceiling (the reaper only releases holds
        // whose expiry has passed, so an uncapped far-future hold would squat its span forever).
        let expires_at = expires_at.min(self.now_ms().saturating_add(self.max_hold_ttl_ms));
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

        // Lower the reaper's earliest-expiry watermark so it will scan once this hold can expire.
        // A removal (release/commit) may leave the bound stale-low, which only costs a redundant
        // scan, never a missed expiry. Bump the generation so a reaper scan that overlaps this
        // placement declines to raise the watermark back over this hold (see collect_expired_holds).
        self.earliest_hold_expiry
            .fetch_min(expires_at, std::sync::atomic::Ordering::Relaxed);
        self.hold_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let event = Event::HoldPlaced { id, resource_id, span, expires_at };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        metrics::counter!(crate::observability::HOLDS_PLACED_TOTAL).increment(1);
        Ok(())
    }

    pub async fn release_hold(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let resource_id = self.remove_hold(id).await?;
        metrics::counter!(crate::observability::HOLDS_RELEASED_TOTAL).increment(1);
        Ok(resource_id)
    }

    /// Same removal as `release_hold`, counted as an expiry instead of a release. The reaper
    /// calls this so a reaped hold does not inflate the released count (HOLDS_EXPIRED_TOTAL
    /// documents the abandonment arithmetic that depends on it).
    pub async fn expire_hold(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let resource_id = self.remove_hold(id).await?;
        metrics::counter!(crate::observability::HOLDS_EXPIRED_TOTAL).increment(1);
        Ok(resource_id)
    }

    async fn remove_hold(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let (resource_id, mut guard) = self.resolve_entity_write(&id).await?;
        find_interval_of_kind(&guard, &id, is_hold)?;
        let event = Event::HoldReleased { id, resource_id };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        Ok(resource_id)
    }

    /// Atomically convert a live hold into a booking on the same span (AVAIL-07). The whole
    /// operation runs under one resource write lock, and the hold being committed is excluded
    /// from the conflict check (it is the caller's own reservation), so there is no
    /// release-then-rebook gap where a competing booker could win the span in between.
    pub async fn commit_hold(
        &self,
        hold_id: Ulid,
        booking_id: Ulid,
        label: Option<String>,
    ) -> Result<(), EngineError> {
        self.commit_hold_offering(hold_id, booking_id, label, 0)
            .await
            .map_err(EngineError::from)
    }

    /// `commit_hold`, but a refusal may carry alternatives.
    ///
    /// Offers here are deliberately narrower than on the other two paths. This body holds a single
    /// guard from `resolve_entity_write` and no inherited rule context was collected, because
    /// collecting it under that guard is the ABBA half of C1, and dropping the guard to collect
    /// then relocking would reopen the release-then-rebook TOCTOU the single-lock design exists to
    /// close. So an offer is made only when this resource's own schedule is the whole story: it
    /// defines non-blocking rules and has no parent, therefore no inherited blocking rule can
    /// exist to be missed. Every bookable delt.at publishes has that shape. Otherwise it stays
    /// silent, which is honest, rather than offering spans a retry would refuse.
    pub async fn commit_hold_offering(
        &self,
        hold_id: Ulid,
        booking_id: Ulid,
        label: Option<String>,
        offer_limit: usize,
    ) -> Result<(), Refused> {
        if let Some(ref l) = label
            && l.len() > MAX_LABEL_LEN {
                return Err(EngineError::LimitExceeded("label too long").into());
            }
        let (resource_id, mut guard) = self.resolve_entity_write(&hold_id).await?;

        // The entity must be a hold; a booking/rule id (or one already reaped) means there is no
        // hold to commit. The booking takes exactly the held span.
        let span = find_interval_of_kind(&guard, &hold_id, is_hold)?.span;

        // The booking id is caller input here exactly as it is in `confirm_booking`, so the same
        // reuse hole applies: without this, committing onto an id already in use inserts a second
        // interval carrying it, and removing either unmaps the id for both. Rejected before the WAL
        // append, so the hold survives and a retry with a fresh booking id still has something to
        // commit.
        self.reject_reused_id(booking_id)?;

        let now = self.now_ms();
        if let Err(error) = check_no_conflict_excluding(&guard, &span, now, Some(hold_id)) {
            // See the doc comment: own schedule only, no parent, or no offer at all.
            let can_offer = guard.parent_id.is_none() && guard.has_non_blocking_rule();
            let limit = if can_offer { offer_limit } else { 0 };
            let window_end = offer_window(&span, limit).end;
            return Err(self.refuse(
                error, &guard, &span, &[], &[], false, now, window_end, limit,
            ));
        }

        // Release + confirm share one fsync (WalCommand::AppendAtomic): an fsync error or a crash
        // before the flush leaves neither durable. They are still two WAL records, so a torn write
        // between them can lose the booking, but release is written before confirm, so the worst
        // case a crash can leave is a freed (re-bookable) slot, never a live hold AND booking, i.e.
        // never an overbook (INV-01 holds). Apply only after the append is durable, like
        // persist_and_apply. (This closes the in-memory release-then-book TOCTOU; it is not a
        // claim of torn-write crash atomicity. See WalCommand::AppendAtomic.)
        let release = Event::HoldReleased { id: hold_id, resource_id };
        let book = Event::BookingConfirmed { id: booking_id, resource_id, span, label };
        self.wal_append_atomic(&[release.clone(), book.clone()]).await?;
        self.store.apply_event(&mut guard, &release);
        self.store.apply_event(&mut guard, &book);
        let parent_id = guard.parent_id;
        self.notify.send(resource_id, &release);
        self.notify.send(resource_id, &book);
        self.notify_ancestors(parent_id, &release);
        self.notify_ancestors(parent_id, &book);
        metrics::counter!(crate::observability::HOLDS_COMMITTED_TOTAL).increment(1);
        metrics::counter!(crate::observability::BOOKINGS_CREATED_TOTAL).increment(1);
        Ok(())
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
        if let Some(ref l) = label
            && l.len() > MAX_LABEL_LEN {
                return Err(EngineError::LimitExceeded("label too long").into());
            }
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
        if bookings.len() > MAX_BATCH_SIZE {
            return Err(EngineError::LimitExceeded("batch too large"));
        }
        for (_, _, span, label) in &bookings {
            validate_span(span)?;
            if let Some(l) = label
                && l.len() > MAX_LABEL_LEN {
                    return Err(EngineError::LimitExceeded("label too long"));
                }
        }

        // Acquire write locks in sorted order to prevent deadlocks.
        let mut resource_ids: Vec<Ulid> = bookings.iter().map(|(_, rid, _, _)| *rid).collect();
        resource_ids.sort();
        resource_ids.dedup();

        // T-03 schedule context per resource, collected BEFORE any write guard (C1). The hull
        // of a resource's member spans bounds its window: every member lies inside it, so the
        // clamped inherited spans cover each member's admission check.
        let mut rule_ctx: HashMap<Ulid, (Vec<Span>, Vec<Span>, bool)> = HashMap::new();
        for rid in &resource_ids {
            let member_spans = bookings.iter().filter(|(_, r, _, _)| r == rid);
            let lo = member_spans.clone().map(|(_, _, s, _)| s.start).min();
            let hi = member_spans.map(|(_, _, s, _)| s.end).max();
            let (Some(lo), Some(hi)) = (lo, hi) else {
                continue;
            };
            let hull = Span::new(lo, hi);
            let parent_id = self.store.get_parent(rid);
            rule_ctx.insert(*rid, self.collect_inherited_rules(*rid, parent_id, &hull).await?);
        }

        let mut guards = Vec::with_capacity(resource_ids.len());
        let mut rs_map = HashMap::new();

        for rid in &resource_ids {
            let rs = self
                .get_resource(rid)
                .ok_or(EngineError::NotFound(*rid))?;
            let guard = rs.write_owned().await;
            if guard.intervals.len() >= MAX_INTERVALS_PER_RESOURCE {
                return Err(EngineError::LimitExceeded("too many intervals on resource"));
            }
            rs_map.insert(*rid, guards.len());
            guards.push(guard);
        }

        // Reuse rejection, under the guards so concurrent retries serialise, and before anything is
        // applied so the batch stays all-or-nothing. Two distinct cases: an id already live
        // anywhere in the tenant, which `reject_reused_id` catches, and two members of this batch
        // sharing one id, which no amount of state inspection can catch because neither exists yet.
        // Both produce the same stranded-interval outcome described on `reject_reused_id`.
        let mut batch_ids = HashSet::with_capacity(bookings.len());
        for (id, _, _, _) in &bookings {
            self.reject_reused_id(*id)?;
            if !batch_ids.insert(*id) {
                return Err(EngineError::AlreadyExists(*id));
            }
        }

        // Phase 1: Validate all bookings against current state + intra-batch.
        let now = self.now_ms();

        let mut by_resource: HashMap<Ulid, Vec<(Ulid, Span)>> = HashMap::new();
        for (id, rid, span, _) in &bookings {
            by_resource.entry(*rid).or_default().push((*id, *span));
        }

        for (rid, batch) in &by_resource {
            let guard = &guards[rs_map[rid]];
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

        // Phase 2: All validated, persist the whole batch under ONE fsync, then apply + notify.
        // A single append is the all-or-nothing durability boundary (AVAIL-06): the previous
        // per-booking loop did N awaited appends, so a mid-batch WAL error left earlier bookings
        // durable while later ones failed, and each append was a serialized fsync held under every
        // batch resource's write lock.
        let events: Vec<Event> = bookings
            .iter()
            .map(|(id, resource_id, span, label)| Event::BookingConfirmed {
                id: *id,
                resource_id: *resource_id,
                span: *span,
                label: label.clone(),
            })
            .collect();
        self.wal_append_atomic(&events).await?;
        for event in &events {
            if let Event::BookingConfirmed { resource_id, .. } = event {
                let guard_idx = rs_map[resource_id];
                let parent_id = guards[guard_idx].parent_id;
                self.store.apply_event(&mut guards[guard_idx], event);
                self.notify.send(*resource_id, event);
                self.notify_ancestors(parent_id, event);
            }
        }
        metrics::counter!(crate::observability::BOOKINGS_CREATED_TOTAL)
            .increment(events.len() as u64);

        Ok(())
    }

    pub async fn cancel_booking(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let (resource_id, mut guard) = self.resolve_entity_write(&id).await?;
        find_interval_of_kind(&guard, &id, is_booking)?;
        let event = Event::BookingCancelled { id, resource_id };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
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
