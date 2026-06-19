use crate::model::*;

// ── Availability Algorithm ────────────────────────────────────────

/// Compute raw free intervals for a resource using its unified interval list
/// plus inherited rules from ancestors.
///
/// Non-blocking: OVERRIDE — if resource has own non-blocking rules, use those;
/// otherwise fall back to inherited_non_blocking.
/// Blocking: ACCUMULATE — own blocking + inherited_blocking are all subtracted.
pub fn availability(
    resource: &ResourceState,
    query: &Span,
    inherited_non_blocking: &[Span],
    inherited_blocking: &[Span],
    now: Ms,
) -> Vec<Span> {
    let buffer = resource.buffer_after.unwrap_or(0);
    let capacity = resource.capacity;

    // Step 1: Determine base non-blocking spans (using binary search)
    let mut own_non_blocking: Vec<Span> = Vec::new();
    let mut own_blocking: Vec<Span> = Vec::new();
    let mut active_allocs: Vec<Span> = Vec::new();

    // Allocations carry a buffer that extends their effective end, so one ending
    // just before `query.start` can still block the head of the window. Scan a
    // buffer-expanded window (mirrors check_no_conflict) so the read path agrees
    // with the write path. Rules carry no buffer, so they only count when they
    // overlap the real query. MIN_VALID_TIMESTAMP_MS is 0, so this stays valid.
    let scan = Span::new((query.start - buffer).max(0), query.end);

    for interval in resource.overlapping(&scan) {
        match &interval.kind {
            IntervalKind::NonBlocking | IntervalKind::Blocking => {
                if interval.span.end <= query.start {
                    continue; // entirely before the window — rules have no buffer
                }
                let clamped = Span::new(
                    interval.span.start.max(query.start),
                    interval.span.end.min(query.end),
                );
                if matches!(interval.kind, IntervalKind::NonBlocking) {
                    own_non_blocking.push(clamped);
                } else {
                    own_blocking.push(clamped);
                }
            }
            IntervalKind::Hold { expires_at } if *expires_at > now => {
                let effective_end = interval.span.end + buffer;
                if effective_end > query.start {
                    active_allocs.push(Span::new(interval.span.start, effective_end));
                }
            }
            IntervalKind::Booking { .. } => {
                let effective_end = interval.span.end + buffer;
                if effective_end > query.start {
                    active_allocs.push(Span::new(interval.span.start, effective_end));
                }
            }
            _ => {} // expired hold
        }
    }

    let mut free = if own_non_blocking.is_empty() {
        inherited_non_blocking.to_vec()
    } else {
        own_non_blocking
    };

    free.sort_by_key(|s| s.start);
    free = merge_overlapping(&free);

    // Step 2: Collect ALL blocking rules (own + inherited)
    let mut blocked = own_blocking;
    blocked.extend_from_slice(inherited_blocking);
    blocked.sort_by_key(|s| s.start);

    if !blocked.is_empty() {
        free = subtract_intervals(&free, &blocked);
    }

    // Step 3: Subtract active allocations (with capacity awareness)
    if !active_allocs.is_empty() {
        active_allocs.sort_by_key(|s| s.start);
        if capacity <= 1 {
            free = subtract_intervals(&free, &active_allocs);
        } else {
            let saturated = compute_saturated_spans(&active_allocs, capacity);
            if !saturated.is_empty() {
                free = subtract_intervals(&free, &saturated);
            }
        }
    }

    free
}

/// Merge sorted overlapping/adjacent intervals into disjoint intervals.
pub fn merge_overlapping(sorted: &[Span]) -> Vec<Span> {
    let mut merged: Vec<Span> = Vec::new();
    for &span in sorted {
        if let Some(last) = merged.last_mut()
            && span.start <= last.end {
                last.end = last.end.max(span.end);
                continue;
            }
        merged.push(span);
    }
    merged
}

pub fn subtract_intervals(base: &[Span], to_remove: &[Span]) -> Vec<Span> {
    let mut result = Vec::new();
    let mut ri = 0;

    for &b in base {
        let mut current_start = b.start;
        let current_end = b.end;

        while ri < to_remove.len() && to_remove[ri].end <= current_start {
            ri += 1;
        }

        let mut j = ri;
        while j < to_remove.len() && to_remove[j].start < current_end {
            let r = &to_remove[j];
            if r.start > current_start {
                result.push(Span::new(current_start, r.start));
            }
            current_start = current_start.max(r.end);
            j += 1;
        }

        if current_start < current_end {
            result.push(Span::new(current_start, current_end));
        }
    }

    result
}

/// Sweep-line algorithm: find time ranges where allocation count >= capacity.
/// Returns sorted, merged spans representing fully-saturated time ranges.
pub fn compute_saturated_spans(allocs: &[Span], capacity: u32) -> Vec<Span> {
    if allocs.is_empty() || capacity == 0 {
        return Vec::new();
    }
    if capacity == 1 {
        return merge_overlapping(allocs);
    }

    // Build sweep-line events: +1 at start, -1 at end
    let mut events: Vec<(Ms, i32)> = Vec::with_capacity(allocs.len() * 2);
    for a in allocs {
        events.push((a.start, 1));
        events.push((a.end, -1));
    }
    events.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut result = Vec::new();
    let mut count: u32 = 0;
    let mut saturated_start: Option<Ms> = None;

    for (time, delta) in &events {
        if *delta > 0 {
            count += *delta as u32;
        } else {
            count -= (-*delta) as u32;
        }

        if count >= capacity && saturated_start.is_none() {
            saturated_start = Some(*time);
        } else if count < capacity
            && let Some(start) = saturated_start.take()
            && *time > start {
                result.push(Span::new(start, *time));
            }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: Ms = 3_600_000;
    const M: Ms = 60_000;

    fn make_resource(intervals: Vec<Interval>) -> ResourceState {
        make_resource_with_capacity(intervals, 1, None)
    }

    fn make_resource_with_capacity(intervals: Vec<Interval>, capacity: u32, buffer_after: Option<Ms>) -> ResourceState {
        let mut rs = ResourceState::new(ulid::Ulid::new(), None, None, capacity, buffer_after);
        for i in intervals {
            rs.insert_interval(i);
        }
        rs
    }

    fn rule(start: Ms, end: Ms, blocking: bool) -> Interval {
        Interval {
            id: ulid::Ulid::new(),
            span: Span::new(start, end),
            kind: if blocking {
                IntervalKind::Blocking
            } else {
                IntervalKind::NonBlocking
            },
        }
    }

    fn booking(start: Ms, end: Ms) -> Interval {
        Interval {
            id: ulid::Ulid::new(),
            span: Span::new(start, end),
            kind: IntervalKind::Booking { label: None },
        }
    }

    fn hold(start: Ms, end: Ms, expires_at: Ms) -> Interval {
        Interval {
            id: ulid::Ulid::new(),
            span: Span::new(start, end),
            kind: IntervalKind::Hold { expires_at },
        }
    }

    // ── subtract_intervals ────────────────────────────────

    #[test]
    fn subtract_no_overlap() {
        let base = vec![Span::new(100, 200), Span::new(300, 400)];
        let remove = vec![Span::new(200, 300)];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(result, base);
    }

    #[test]
    fn subtract_full_overlap() {
        let base = vec![Span::new(100, 200)];
        let remove = vec![Span::new(50, 250)];
        let result = subtract_intervals(&base, &remove);
        assert!(result.is_empty());
    }

    #[test]
    fn subtract_partial_left() {
        let base = vec![Span::new(100, 200)];
        let remove = vec![Span::new(50, 150)];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(result, vec![Span::new(150, 200)]);
    }

    #[test]
    fn subtract_partial_right() {
        let base = vec![Span::new(100, 200)];
        let remove = vec![Span::new(150, 250)];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(result, vec![Span::new(100, 150)]);
    }

    #[test]
    fn subtract_middle_punch() {
        let base = vec![Span::new(100, 300)];
        let remove = vec![Span::new(150, 200)];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(result, vec![Span::new(100, 150), Span::new(200, 300)]);
    }

    #[test]
    fn subtract_multiple_punches() {
        let base = vec![Span::new(0, 1000)];
        let remove = vec![
            Span::new(100, 200),
            Span::new(400, 500),
            Span::new(800, 900),
        ];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(
            result,
            vec![
                Span::new(0, 100),
                Span::new(200, 400),
                Span::new(500, 800),
                Span::new(900, 1000),
            ]
        );
    }

    // ── merge_overlapping ────────────────────────────────

    #[test]
    fn merge_overlapping_basic() {
        let spans = vec![
            Span::new(100, 300),
            Span::new(200, 400),
            Span::new(500, 600),
        ];
        let merged = merge_overlapping(&spans);
        assert_eq!(merged, vec![Span::new(100, 400), Span::new(500, 600)]);
    }

    #[test]
    fn merge_overlapping_adjacent() {
        let spans = vec![Span::new(100, 200), Span::new(200, 300)];
        let merged = merge_overlapping(&spans);
        assert_eq!(merged, vec![Span::new(100, 300)]);
    }

    // ── availability (pure function, no hierarchy) ────────

    #[test]
    fn availability_basic_raw_intervals() {
        let nine = 9 * H;
        let twelve = 12 * H;
        let ten = 10 * H;
        let ten_thirty = ten + 30 * M;

        let rs = make_resource(vec![
            rule(nine, twelve, false),
            booking(ten, ten_thirty),
        ]);
        let query = Span::new(0, 24 * H);
        let free = availability(&rs, &query, &[], &[], 0);
        assert_eq!(free.len(), 2);
        assert_eq!(free[0], Span::new(nine, ten));
        assert_eq!(free[1], Span::new(ten_thirty, twelve));
    }

    #[test]
    fn availability_with_inherited_non_blocking() {
        let rs = make_resource(vec![]);
        let inherited = vec![Span::new(9 * H, 17 * H)];
        let query = Span::new(0, 24 * H);
        let free = availability(&rs, &query, &inherited, &[], 0);
        assert_eq!(free, vec![Span::new(9 * H, 17 * H)]);
    }

    #[test]
    fn availability_own_overrides_inherited() {
        let rs = make_resource(vec![rule(14 * H, 16 * H, false)]);
        let inherited = vec![Span::new(9 * H, 17 * H)];
        let query = Span::new(0, 24 * H);
        let free = availability(&rs, &query, &inherited, &[], 0);
        assert_eq!(free, vec![Span::new(14 * H, 16 * H)]);
    }

    #[test]
    fn availability_inherited_blocking_accumulates() {
        let rs = make_resource(vec![rule(9 * H, 17 * H, false)]);
        let inherited_blocking = vec![Span::new(12 * H, 13 * H)];
        let query = Span::new(0, 24 * H);
        let free = availability(&rs, &query, &[], &inherited_blocking, 0);
        assert_eq!(
            free,
            vec![Span::new(9 * H, 12 * H), Span::new(13 * H, 17 * H)]
        );
    }

    #[test]
    fn expired_hold_not_counted() {
        let nine = 9 * H;
        let ten = 10 * H;

        let rs = make_resource(vec![
            rule(nine, ten, false),
            hold(nine, ten, 1), // expired
        ]);
        let query = Span::new(0, ten + H);
        let now = 1000;
        let free = availability(&rs, &query, &[], &[], now);
        assert_eq!(free, vec![Span::new(nine, ten)]);
    }

    #[test]
    fn buffer_straddling_query_start_blocks_availability() {
        let ten = 10 * H;
        let eleven = 11 * H;
        let buffer = 30 * M;
        // Open all day, capacity 1, 30-min buffer after each booking.
        let rs = make_resource_with_capacity(
            vec![rule(0, 24 * H, false), booking(ten, eleven)],
            1,
            Some(buffer),
        );
        // Query a window that STARTS inside the booking's buffer tail [11:00, 11:30).
        // The read path must agree with check_no_conflict: this slot is not bookable.
        let query = Span::new(eleven + 5 * M, 12 * H);
        let free = availability(&rs, &query, &[], &[], 0);
        assert_eq!(free, vec![Span::new(eleven + buffer, 12 * H)]);
    }

    #[test]
    fn blocking_rule_subtracts() {
        let nine = 9 * H;
        let ten = 10 * H;
        let eleven = 11 * H;
        let twelve = 12 * H;

        let rs = make_resource(vec![
            rule(nine, twelve, false),
            rule(ten, eleven, true),
        ]);
        let query = Span::new(0, twelve + H);
        let free = availability(&rs, &query, &[], &[], 0);
        assert_eq!(
            free,
            vec![Span::new(nine, ten), Span::new(eleven, twelve)]
        );
    }

    // ── compute_saturated_spans ────────────────────────────

    #[test]
    fn saturated_spans_basic() {
        let allocs = vec![Span::new(0, 100), Span::new(50, 150)];
        let sat = compute_saturated_spans(&allocs, 2);
        assert_eq!(sat, vec![Span::new(50, 100)]);
    }

    #[test]
    fn saturated_spans_no_overlap() {
        let allocs = vec![Span::new(0, 100), Span::new(200, 300)];
        let sat = compute_saturated_spans(&allocs, 2);
        assert!(sat.is_empty());
    }

    #[test]
    fn saturated_spans_capacity_one() {
        let allocs = vec![Span::new(0, 100), Span::new(200, 300)];
        let sat = compute_saturated_spans(&allocs, 1);
        assert_eq!(sat, vec![Span::new(0, 100), Span::new(200, 300)]);
    }

    #[test]
    fn saturated_spans_three_overlap_capacity_three() {
        let allocs = vec![
            Span::new(0, 100),
            Span::new(25, 75),
            Span::new(50, 150),
        ];
        let sat = compute_saturated_spans(&allocs, 3);
        assert_eq!(sat, vec![Span::new(50, 75)]);
    }

    #[test]
    fn saturated_spans_empty() {
        let sat = compute_saturated_spans(&[], 5);
        assert!(sat.is_empty());
    }
}

/// Executable spec (TEST-01/02): property-test `availability()` against an
/// independent brute-force reference. The reference samples every integer
/// millisecond in the query window and decides freeness from first principles
/// (open ∧ ¬blocked ∧ active < capacity), then reassembles maximal free runs.
/// Because all coordinates are integers and spans are half-open `[start, end)`,
/// point-sampling at integers reconstructs the exact span set — so any
/// disagreement is a real bug in the production algorithm, not sampling error.
///
/// This makes INV-01 (availability is derived, never stored) and INV-02
/// (a point is free iff open, unblocked, and under capacity) *verified* across
/// thousands of generated edge cases rather than asserted by hand-picked tests.
#[cfg(test)]
mod spec {
    use super::*;
    use proptest::prelude::*;

    /// All generated coordinates live in `[0, RANGE)`, and the query is exactly
    /// `[0, RANGE)`. Keeping every interval inside the query makes rule-clamping a
    /// no-op and guarantees every allocation passes the engine's `overlapping(query)`
    /// gate, isolating the test to the core set math (this is the contract the
    /// engine and reference must agree on; query-boundary buffer behaviour is
    /// covered separately by unit tests).
    const RANGE: Ms = 60;
    const NOW: Ms = 1000;

    #[derive(Debug, Clone)]
    enum GenKind {
        NonBlocking,
        Blocking,
        Booking,
        Hold { expires_at: Ms },
    }

    #[derive(Debug, Clone)]
    struct GenInterval {
        start: Ms,
        end: Ms,
        kind: GenKind,
    }

    fn kind_strategy() -> impl Strategy<Value = GenKind> {
        // Concentrate expires_at ON and AROUND `now`: the `expires_at > now`
        // boundary (AVAIL-11 — a hold at exactly `now` is expired) is a one-point
        // edge that uniform sampling almost never hits. Without this weighting the
        // test cannot distinguish `>` from `>=` (a mutation that proved exactly
        // this slipped through before the weighting was added).
        let expires = prop_oneof![
            3 => Just(NOW),
            3 => NOW - 2..=NOW + 2,
            1 => 0i64..2 * NOW,
        ];
        prop_oneof![
            Just(GenKind::NonBlocking),
            Just(GenKind::Blocking),
            Just(GenKind::Booking),
            expires.prop_map(|e| GenKind::Hold { expires_at: e }),
        ]
    }

    fn interval_strategy() -> impl Strategy<Value = GenInterval> {
        (0i64..RANGE - 1, 1i64..=12, kind_strategy()).prop_map(|(start, len, kind)| GenInterval {
            start,
            end: (start + len).min(RANGE),
            kind,
        })
    }

    fn span_strategy() -> impl Strategy<Value = Span> {
        (0i64..RANGE - 1, 1i64..=12).prop_map(|(start, len)| Span::new(start, (start + len).min(RANGE)))
    }

    fn to_interval(g: &GenInterval) -> Interval {
        Interval {
            id: ulid::Ulid::new(),
            span: Span::new(g.start, g.end),
            kind: match &g.kind {
                GenKind::NonBlocking => IntervalKind::NonBlocking,
                GenKind::Blocking => IntervalKind::Blocking,
                GenKind::Booking => IntervalKind::Booking { label: None },
                GenKind::Hold { expires_at } => IntervalKind::Hold { expires_at: *expires_at },
            },
        }
    }

    /// Brute-force reference: independent of the production algorithm.
    fn reference(
        intervals: &[Interval],
        capacity: u32,
        buffer: Ms,
        inherited_nb: &[Span],
        inherited_b: &[Span],
        query: &Span,
        now: Ms,
    ) -> Vec<Span> {
        // OVERRIDE rule (AVAIL): own non-blocking, if any overlaps the query, fully
        // replaces inherited non-blocking as the base of what is open.
        let own_nb_present = intervals
            .iter()
            .any(|i| matches!(i.kind, IntervalKind::NonBlocking) && i.span.overlaps(query));

        let mut runs = Vec::new();
        let mut run_start: Option<Ms> = None;

        for t in query.start..query.end {
            let open = if own_nb_present {
                intervals
                    .iter()
                    .any(|i| matches!(i.kind, IntervalKind::NonBlocking) && i.span.contains_instant(t))
            } else {
                inherited_nb.iter().any(|s| s.contains_instant(t))
            };

            let blocked = intervals
                .iter()
                .any(|i| matches!(i.kind, IntervalKind::Blocking) && i.span.contains_instant(t))
                || inherited_b.iter().any(|s| s.contains_instant(t));

            // ACCUMULATE rule: count live allocations (bookings + unexpired holds),
            // each extended by the buffer. Capacity is the max concurrent count.
            // No query gate — an allocation occupies `[start, end + buffer)` wherever
            // that lands, including a buffer tail that reaches past `query.start`.
            let active = intervals
                .iter()
                .filter(|i| {
                    let is_live = match &i.kind {
                        IntervalKind::Booking { .. } => true,
                        IntervalKind::Hold { expires_at } => *expires_at > now,
                        _ => false,
                    };
                    is_live && i.span.start <= t && t < i.span.end + buffer
                })
                .count() as u32;

            let free = open && !blocked && active < capacity;

            match (free, run_start) {
                (true, None) => run_start = Some(t),
                (false, Some(s)) => {
                    runs.push(Span::new(s, t));
                    run_start = None;
                }
                _ => {}
            }
        }
        if let Some(s) = run_start.take() {
            runs.push(Span::new(s, query.end));
        }
        runs
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]

        #[test]
        fn availability_matches_brute_force_reference(
            gen_intervals in prop::collection::vec(interval_strategy(), 0..12),
            capacity in 1u32..=3,
            buffer in 0i64..=8,
            // A sub-window query (start > 0) exercises rule-clamping and the
            // buffer-straddle path: allocations before the window whose buffer
            // tail reaches into it must still subtract.
            q_start in 0i64..RANGE / 2,
            gen_inherited_nb in prop::collection::vec(span_strategy(), 0..4),
            gen_inherited_b in prop::collection::vec(span_strategy(), 0..4),
        ) {
            let query = Span::new(q_start, RANGE);
            let intervals: Vec<Interval> = gen_intervals.iter().map(to_interval).collect();

            // collect_inherited_rules always clamps inherited spans to the query
            // window before they reach availability(); mirror that contract here.
            let clamp = |spans: &[Span]| -> Vec<Span> {
                spans
                    .iter()
                    .filter_map(|s| {
                        let start = s.start.max(query.start);
                        let end = s.end.min(query.end);
                        (start < end).then(|| Span::new(start, end))
                    })
                    .collect()
            };
            let inherited_nb = clamp(&gen_inherited_nb);
            let inherited_b = clamp(&gen_inherited_b);

            let mut rs = ResourceState::new(ulid::Ulid::new(), None, None, capacity, Some(buffer));
            for i in &intervals {
                rs.insert_interval(i.clone());
            }

            let got = availability(&rs, &query, &inherited_nb, &inherited_b, NOW);
            let want = reference(&intervals, capacity, buffer, &inherited_nb, &inherited_b, &query, NOW);

            prop_assert_eq!(got, want);
        }
    }
}
