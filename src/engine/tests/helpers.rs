//! Shared fixtures for the engine test suite. The interval builders are also used by
//! the availability unit tests.

use crate::engine::*;

pub(crate) const H: Ms = 3_600_000; // 1 hour in ms
pub(crate) const M: Ms = 60_000; // 1 minute in ms

/// Helper to build a ResourceState with intervals for pure-function tests.
pub(crate) fn make_resource(intervals: Vec<Interval>) -> ResourceState {
    make_resource_with_capacity(intervals, 1, None)
}

pub(crate) fn make_resource_with_capacity(intervals: Vec<Interval>, capacity: u32, buffer_after: Option<Ms>) -> ResourceState {
    let mut rs = ResourceState::new(Ulid::new(), None, None, capacity, buffer_after);
    for i in intervals {
        rs.insert_interval(i);
    }
    rs
}

pub(crate) fn rule(start: Ms, end: Ms, blocking: bool) -> Interval {
    Interval {
        id: Ulid::new(),
        span: Span::new(start, end),
        kind: if blocking {
            IntervalKind::Blocking
        } else {
            IntervalKind::NonBlocking
        },
    }
}

pub(crate) fn booking(start: Ms, end: Ms) -> Interval {
    Interval {
        id: Ulid::new(),
        span: Span::new(start, end),
        kind: IntervalKind::Booking { label: None },
    }
}

pub(crate) fn hold(start: Ms, end: Ms, expires_at: Ms) -> Interval {
    Interval {
        id: Ulid::new(),
        span: Span::new(start, end),
        kind: IntervalKind::Hold { expires_at },
    }
}

pub(crate) fn test_wal_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("deltat_test_engine");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    let _ = std::fs::remove_file(&path);
    path
}
