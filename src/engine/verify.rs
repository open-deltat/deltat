//! Cross-path verification (TEST-01 family): the read path and the write path
//! must agree, and each must match an independent brute-force reference.
//!
//! `availability()` (read) tells a client what is bookable; `check_no_conflict()`
//! (write) decides whether a booking is accepted. If they disagree, a client is
//! shown a free slot it cannot book (or vice versa). GAP-12 was exactly such a
//! disagreement on the buffer dimension; these properties regression-lock the
//! contract across thousands of generated states.

use super::availability::availability;
use super::conflict::check_no_conflict;
use crate::model::*;
use proptest::prelude::*;

const RANGE: Ms = 60;
const NOW: Ms = 1000;

#[derive(Debug, Clone)]
enum AllocKind {
    Booking,
    Hold { expires_at: Ms },
}

#[derive(Debug, Clone)]
struct GenAlloc {
    start: Ms,
    end: Ms,
    kind: AllocKind,
}

impl GenAlloc {
    fn live(&self, now: Ms) -> bool {
        match self.kind {
            AllocKind::Booking => true,
            AllocKind::Hold { expires_at } => expires_at > now,
        }
    }
    fn to_interval(&self) -> Interval {
        Interval {
            id: ulid::Ulid::new(),
            span: Span::new(self.start, self.end),
            kind: match self.kind {
                AllocKind::Booking => IntervalKind::Booking { label: None },
                AllocKind::Hold { expires_at } => IntervalKind::Hold { expires_at },
            },
        }
    }
}

fn alloc_kind() -> impl Strategy<Value = AllocKind> {
    // Holds concentrated on the `expires_at > now` boundary (AVAIL-11) so the
    // live/expired edge is exercised, not sampled away (see mod `spec`).
    let expires = prop_oneof![
        3 => Just(NOW),
        3 => NOW - 2..=NOW + 2,
        1 => 0i64..2 * NOW,
    ];
    prop_oneof![
        Just(AllocKind::Booking),
        expires.prop_map(|e| AllocKind::Hold { expires_at: e }),
    ]
}

fn alloc_strategy() -> impl Strategy<Value = GenAlloc> {
    (0i64..RANGE - 1, 1i64..=12, alloc_kind()).prop_map(|(start, len, kind)| GenAlloc {
        start,
        end: (start + len).min(RANGE),
        kind,
    })
}

fn span_strategy() -> impl Strategy<Value = Span> {
    (0i64..RANGE - 1, 1i64..=12).prop_map(|(start, len)| Span::new(start, (start + len).min(RANGE)))
}

fn build(allocs: &[GenAlloc], capacity: u32, buffer: Ms, open_window: Option<Span>) -> ResourceState {
    let mut rs = ResourceState::new(ulid::Ulid::new(), None, None, capacity, Some(buffer));
    if let Some(w) = open_window {
        rs.insert_interval(Interval {
            id: ulid::Ulid::new(),
            span: w,
            kind: IntervalKind::NonBlocking,
        });
    }
    for a in allocs {
        rs.insert_interval(a.to_interval());
    }
    rs
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]

    /// The read path and the write path agree on the allocation/capacity/buffer
    /// dimension. The whole window is open (one non-blocking rule), so the only
    /// thing that can close an instant is allocation pressure, which both paths
    /// must compute identically. (The blocking-rule read/write disagreement is
    /// the separate, still-open T-03, deliberately excluded by generating no
    /// blocking rules here.)
    ///
    /// Buffer is SYMMETRIC (B1): a booking occupies its span PLUS its own turnaround
    /// `[t, t + 1 + buffer)`, so it is bookable iff that whole footprint is free in the
    /// read view, not just its raw span. Only probes whose footprint stays inside the
    /// computed window are asserted, so availability (computed over `[0, RANGE)`) covers it.
    #[test]
    fn read_path_agrees_with_write_path(
        allocs in prop::collection::vec(alloc_strategy(), 0..10),
        capacity in 1u32..=3,
        buffer in 0i64..=8,
    ) {
        let query = Span::new(0, RANGE);
        let rs = build(&allocs, capacity, buffer, Some(query));

        let free = availability(&rs, &query, &[], &[], NOW);
        let last = (RANGE - buffer).max(0);
        for t in 0..last {
            let write_ok = check_no_conflict(&rs, &Span::new(t, t + 1), NOW).is_ok();
            let footprint_free = (t..t + 1 + buffer).all(|u| free.iter().any(|s| s.contains_instant(u)));
            prop_assert_eq!(
                write_ok, footprint_free,
                "read/write disagree at t={}: buffered footprint free={}, conflict-check says bookable={}",
                t, footprint_free, write_ok
            );
        }
    }

    /// The write path matches an independent brute-force reference: a candidate
    /// booking is rejected iff, at some instant its buffered footprint covers, the
    /// count of live buffered allocations already meets capacity. The footprint is
    /// `[candidate.start, candidate.end + buffer)`, the candidate carries its own
    /// turnaround too (symmetric buffer, B1).
    #[test]
    fn check_no_conflict_matches_brute_force(
        allocs in prop::collection::vec(alloc_strategy(), 0..10),
        capacity in 1u32..=3,
        buffer in 0i64..=8,
        candidate in span_strategy(),
    ) {
        let rs = build(&allocs, capacity, buffer, None);
        let accepted = check_no_conflict(&rs, &candidate, NOW).is_ok();

        let mut should_reject = false;
        for t in candidate.start..(candidate.end + buffer) {
            let count = allocs
                .iter()
                .filter(|a| a.live(NOW) && a.start <= t && t < a.end + buffer)
                .count() as u32;
            if count >= capacity {
                should_reject = true;
                break;
            }
        }
        prop_assert_eq!(accepted, !should_reject);
    }
}
