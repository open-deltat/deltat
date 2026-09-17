//! The error type returned by the engine's read and write paths.

use ulid::Ulid;

use crate::model::Span;

#[derive(Debug)]
pub enum EngineError {
    NotFound(Ulid),
    AlreadyExists(Ulid),
    Conflict(Ulid),
    NotCoveredByParent {
        rule_span: Span,
        uncovered: Vec<Span>,
    },
    /// T-03: the candidate span leaves the effective open windows (outside the schedule's
    /// non-blocking base, or inside a blocking window, own or inherited).
    ClosedBySchedule {
        span: Span,
        closed: Vec<Span>,
    },
    CycleDetected(Ulid),
    HasChildren(Ulid),
    CapacityExceeded(u32),
    LimitExceeded(&'static str),
    WalError(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::NotFound(id) => write!(f, "not found: {id}"),
            EngineError::AlreadyExists(id) => write!(f, "already exists: {id}"),
            // The winner's id is deliberately NOT rendered. It used to be, which handed a losing
            // caller the identifier of an allocation it has no other way to see: cheap failing
            // INSERTs then enumerate every allocation on a resource you cannot read, and
            // `cancel_booking` takes an id and nothing else, so what you can enumerate you can
            // cancel. The id stays in the variant because the engine and its tests use it; it just
            // stops crossing the wire. What the caller actually needed is in DETAIL instead.
            EngineError::Conflict(_) => write!(f, "span is already allocated"),
            EngineError::NotCoveredByParent {
                rule_span,
                uncovered,
            } => {
                // Counts, not `{:?}` over a Vec<Span>. A psql user used to read
                // `uncovered: [Span { start: 1800200000000, end: 2000003600000 }]`, which is a
                // debug dump wearing an error message. The spans themselves belong in DETAIL where
                // a client can parse them.
                write!(
                    f,
                    "rule [{}, {}) not covered by parent availability; {} uncovered window(s)",
                    rule_span.start,
                    rule_span.end,
                    uncovered.len()
                )
            }
            EngineError::ClosedBySchedule { span, closed } => {
                write!(
                    f,
                    "span [{}, {}) is outside open windows or blocked; {} closed window(s)",
                    span.start,
                    span.end,
                    closed.len()
                )
            }
            EngineError::CycleDetected(id) => write!(f, "cycle detected at resource: {id}"),
            EngineError::HasChildren(id) => {
                write!(f, "cannot delete resource {id}: has children")
            }
            EngineError::CapacityExceeded(cap) => {
                write!(f, "capacity {cap} exceeded: all slots occupied")
            }
            EngineError::LimitExceeded(msg) => write!(f, "limit exceeded: {msg}"),
            EngineError::WalError(e) => write!(f, "WAL error: {e}"),
        }
    }
}

impl EngineError {
    /// Short, stable label for metrics and logs. Bounded cardinality: one per variant.
    pub fn kind(&self) -> &'static str {
        match self {
            EngineError::NotFound(_) => "not_found",
            EngineError::AlreadyExists(_) => "already_exists",
            EngineError::Conflict(_) => "conflict",
            EngineError::NotCoveredByParent { .. } => "not_covered_by_parent",
            EngineError::ClosedBySchedule { .. } => "closed_by_schedule",
            EngineError::CycleDetected(_) => "cycle_detected",
            EngineError::HasChildren(_) => "has_children",
            EngineError::CapacityExceeded(_) => "capacity_exceeded",
            EngineError::LimitExceeded(_) => "limit_exceeded",
            EngineError::WalError(_) => "wal",
        }
    }

    /// SQLSTATE for the wire. Clients branch on this, so the split that matters is
    /// retryable contention against everything else.
    ///
    /// `Conflict` and `CapacityExceeded` map to 40001 (serialization_failure), the code
    /// PostgreSQL drivers already treat as "lost a race, try again": the caller should pick
    /// another span or retry, not surface a failure. Everything else is a client mistake or
    /// a server fault and must not be retried blindly.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            EngineError::Conflict(_) | EngineError::CapacityExceeded(_) => "40001",
            EngineError::NotFound(_) => "42704",
            EngineError::AlreadyExists(_) => "23505",
            EngineError::NotCoveredByParent { .. } | EngineError::ClosedBySchedule { .. } => "23514",
            EngineError::CycleDetected(_) | EngineError::HasChildren(_) => "23503",
            EngineError::LimitExceeded(_) => "54000",
            EngineError::WalError(_) => "58030",
        }
    }

    /// Whether this refusal means "not this span", the case where alternatives help.
    ///
    /// Deliberately independent of `is_retryable`. `ClosedBySchedule` must never be retried as a
    /// lost race, and it is exactly where a counter-offer is most useful: the caller asked outside
    /// opening hours and the useful reply is when the doors are open.
    pub fn is_offerable(&self) -> bool {
        matches!(
            self,
            EngineError::Conflict(_)
                | EngineError::CapacityExceeded(_)
                | EngineError::ClosedBySchedule { .. }
        )
    }

    /// True when the caller may retry the same statement and reasonably expect a different
    /// outcome once the competing allocation clears.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            EngineError::Conflict(_) | EngineError::CapacityExceeded(_)
        )
    }
}

impl std::error::Error for EngineError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// One representative per variant. A new variant fails to compile here, which is the
    /// point: the taxonomy must stay exhaustive or the metric label silently lies.
    fn one_of_each() -> Vec<EngineError> {
        let id = Ulid::nil();
        let span = Span::new(0, 1);
        vec![
            EngineError::NotFound(id),
            EngineError::AlreadyExists(id),
            EngineError::Conflict(id),
            EngineError::NotCoveredByParent {
                rule_span: span,
                uncovered: vec![span],
            },
            EngineError::ClosedBySchedule {
                span,
                closed: vec![span],
            },
            EngineError::CycleDetected(id),
            EngineError::HasChildren(id),
            EngineError::CapacityExceeded(1),
            EngineError::LimitExceeded("test"),
            EngineError::WalError("test".into()),
        ]
    }

    #[test]
    fn every_variant_has_a_distinct_kind() {
        let kinds: Vec<_> = one_of_each().iter().map(|e| e.kind()).collect();
        let mut sorted = kinds.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), kinds.len(), "kind labels collide: {kinds:?}");
    }

    #[test]
    fn kinds_are_metric_safe() {
        for e in one_of_each() {
            let k = e.kind();
            assert!(
                !k.is_empty() && k.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "kind {k:?} is not a bare lowercase label"
            );
        }
    }

    #[test]
    fn contention_is_retryable_and_everything_else_is_not() {
        for e in one_of_each() {
            let expected = matches!(
                e,
                EngineError::Conflict(_) | EngineError::CapacityExceeded(_)
            );
            assert_eq!(e.is_retryable(), expected, "{} misclassified", e.kind());
            assert_eq!(
                e.sqlstate() == "40001",
                expected,
                "{}: SQLSTATE 40001 must mark exactly the retryable set",
                e.kind()
            );
        }
    }

    #[test]
    fn sqlstates_are_five_character_codes() {
        for e in one_of_each() {
            let s = e.sqlstate();
            assert_eq!(s.len(), 5, "{}: {s:?} is not a SQLSTATE", e.kind());
            assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
        }
    }
}
