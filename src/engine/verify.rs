//! Cross-path verification (TEST-01 family): the read path and the write path
//! must agree, and each must match an independent brute-force reference.
//!
//! `availability()` (read) tells a client what is bookable; admission (rules +
//! allocation conflict) decides whether a booking is accepted. If they disagree,
//! a client is shown a free slot it cannot book (or vice versa). GAP-12 was such
//! a disagreement on the buffer dimension and T-03 on the rule dimension; these
//! properties regression-lock the contract across thousands of generated states.

use super::availability::availability;
use super::conflict::{check_no_conflict, check_rules_admit};
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
    build_with_rules(allocs, capacity, buffer, open_window, &[])
}

fn build_with_rules(
    allocs: &[GenAlloc],
    capacity: u32,
    buffer: Ms,
    open_window: Option<Span>,
    blocking: &[Span],
) -> ResourceState {
    let mut rs = ResourceState::new(ulid::Ulid::new(), None, None, capacity, Some(buffer));
    if let Some(w) = open_window {
        rs.insert_interval(Interval {
            id: ulid::Ulid::new(),
            span: w,
            kind: IntervalKind::NonBlocking,
        });
    }
    for b in blocking {
        rs.insert_interval(Interval {
            id: ulid::Ulid::new(),
            span: *b,
            kind: IntervalKind::Blocking,
        });
    }
    for a in allocs {
        rs.insert_interval(a.to_interval());
    }
    rs
}

/// Open windows biased wide (so probes regularly land inside) but never whole-range only,
/// so the closed edges are exercised too.
fn open_window_strategy() -> impl Strategy<Value = Span> {
    (0i64..RANGE - 1, 8i64..=RANGE).prop_map(|(start, len)| Span::new(start, (start + len).min(RANGE)))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]

    /// The read path and the write path agree across the RULE dimension (T-03, closed) and the
    /// allocation/capacity/buffer dimension. States carry a partial open window, blocking rules,
    /// and allocations; admission = rules (raw span inside the open windows) + allocation
    /// conflict (symmetric buffered footprint, B1). Two directions:
    ///  - ACCEPTED probes read free over their whole raw span (a write availability reports as
    ///    unavailable, rule-blocked included, is never admitted);
    ///  - probes whose whole buffered footprint reads free are ACCEPTED (no phantom conflicts).
    /// Between the two sits exactly the documented buffer exemption: a probe whose raw span is
    /// open but whose turnaround tail leaves the open windows is admitted although the tail
    /// reads closed (cleanup may run outside open hours).
    #[test]
    fn read_path_agrees_with_write_path(
        allocs in prop::collection::vec(alloc_strategy(), 0..10),
        capacity in 1u32..=3,
        buffer in 0i64..=8,
        open in open_window_strategy(),
        blocking in prop::collection::vec(span_strategy(), 0..3),
    ) {
        let query = Span::new(0, RANGE);
        let rs = build_with_rules(&allocs, capacity, buffer, Some(open), &blocking);

        let free = availability(&rs, &query, &[], &[], NOW);
        let last = (RANGE - buffer).max(0);
        for t in 0..last {
            let probe = Span::new(t, t + 1);
            let write_ok = check_rules_admit(&rs, &probe, &[], &[], false).is_ok()
                && check_no_conflict(&rs, &probe, NOW).is_ok();
            let raw_free = free.iter().any(|s| s.contains_instant(t));
            let footprint_free =
                (t..t + 1 + buffer).all(|u| free.iter().any(|s| s.contains_instant(u)));
            if write_ok {
                prop_assert!(
                    raw_free,
                    "admitted a probe the read path reports unavailable at t={}", t
                );
            }
            if footprint_free {
                prop_assert!(
                    write_ok,
                    "rejected a probe whose whole buffered footprint reads free at t={}", t
                );
            }
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
