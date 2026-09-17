//! The transport-neutral command vocabulary.
//!
//! `Command` is the boundary between an adapter and the kernel. SQL/pgwire is one adapter today
//! (`sql::parse_sql` produces a `Command`); the framed, HTTP, and MCP transports (PROTO-01/03/04)
//! will be siblings that build the same `Command` and hand it to `wire::execute_command`. It depends
//! only on the kernel value types, never on a specific transport (no `sqlparser`), so adding a
//! transport never drags another transport's parser along, and the kernel can be carved into its own
//! crate without this seam coming with it.

use ulid::Ulid;

use crate::model::*;

/// Which span column a [`SpanFilter`] compares against.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SpanColumn {
    Start,
    End,
}

/// The comparison a [`SpanFilter`] applies.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SpanOp {
    Lt,
    Lte,
    Gt,
    Gte,
    Eq,
}

/// One `WHERE` comparison against a span column on a bookings/holds read.
///
/// Kept as the literal predicate the caller wrote rather than normalised into a window, because
/// `start >= A AND "end" <= B` (containment) and `start < B AND "end" > A` (overlap) are different
/// questions and SQL already distinguishes them. Re-interpreting one as the other would be a
/// quieter version of the bug this type exists to fix: the engine silently answering a question
/// nobody asked.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct SpanFilter {
    pub column: SpanColumn,
    pub op: SpanOp,
    pub value: Ms,
}

impl SpanFilter {
    /// Whether an interval with this `[start, end)` satisfies the predicate.
    pub fn matches(&self, start: Ms, end: Ms) -> bool {
        let lhs = match self.column {
            SpanColumn::Start => start,
            SpanColumn::End => end,
        };
        match self.op {
            SpanOp::Lt => lhs < self.value,
            SpanOp::Lte => lhs <= self.value,
            SpanOp::Gt => lhs > self.value,
            SpanOp::Gte => lhs >= self.value,
            SpanOp::Eq => lhs == self.value,
        }
    }
}

/// Whether an interval satisfies every predicate (an empty list matches everything).
pub fn span_filters_match(filters: &[SpanFilter], start: Ms, end: Ms) -> bool {
    filters.iter().all(|f| f.matches(start, end))
}

/// A parsed, transport-neutral request to the engine.
#[derive(Debug, PartialEq)]
pub enum Command {
    InsertResource {
        id: Ulid,
        parent_id: Option<Ulid>,
        name: Option<String>,
        capacity: u32,
        buffer_after: Option<Ms>,
    },
    BatchInsertResources {
        resources: Vec<ResourceRow>,
    },
    /// Partial update: a field is `None` when the UPDATE omitted that column (leave unchanged). The
    /// inner `Option` on the nullable fields carries the value to set, so `name: Some(None)` sets
    /// NULL whereas `name: None` leaves the current name untouched.
    UpdateResource {
        id: Ulid,
        name: Option<Option<String>>,
        capacity: Option<u32>,
        buffer_after: Option<Option<Ms>>,
    },
    DeleteResource {
        id: Ulid,
    },
    InsertRule {
        id: Ulid,
        resource_id: Ulid,
        start: Ms,
        end: Ms,
        blocking: bool,
    },
    BatchInsertRules {
        rules: Vec<(Ulid, Ulid, Ms, Ms, bool)>, // (id, resource_id, start, end, blocking)
    },
    UpdateRule {
        id: Ulid,
        start: Ms,
        end: Ms,
        blocking: bool,
    },
    DeleteRule {
        id: Ulid,
    },
    InsertHold {
        id: Ulid,
        resource_id: Ulid,
        start: Ms,
        end: Ms,
        expires_at: Ms,
    },
    DeleteHold {
        id: Ulid,
    },
    /// Atomically convert a live hold into a booking (AVAIL-07). The booking takes exactly the
    /// held span on the hold's resource; the hold is excluded from its own conflict check, so
    /// there is no release-then-rebook window a competing booker could win.
    CommitHold {
        hold_id: Ulid,
        booking_id: Ulid,
        label: Option<String>,
    },
    InsertBooking {
        id: Ulid,
        resource_id: Ulid,
        start: Ms,
        end: Ms,
        label: Option<String>,
    },
    BatchInsertBookings {
        bookings: Vec<(Ulid, Ulid, Ms, Ms, Option<String>)>, // (id, resource_id, start, end, label)
    },
    DeleteBooking {
        id: Ulid,
    },
    SelectResources {
        parent_id: Option<Option<Ulid>>, // None = no filter, Some(None) = root only, Some(Some(id)) = children of id
    },
    SelectRules {
        resource_id: Ulid,
    },
    SelectBookings {
        resource_id: Ulid,
        filters: Vec<SpanFilter>,
    },
    SelectHolds {
        resource_id: Ulid,
        filters: Vec<SpanFilter>,
    },
    SelectAvailability {
        resource_id: Ulid,
        start: Ms,
        end: Ms,
        min_duration: Option<Ms>,
    },
    SelectMultiAvailability {
        resource_ids: Vec<Ulid>,
        start: Ms,
        end: Ms,
        min_available: usize,
        min_duration: Option<Ms>,
    },
    /// Per-resource availability for several resources in one request: each row keeps its own
    /// resource_id so the caller can regroup (unlike SelectMultiAvailability, which merges).
    SelectAvailabilityMulti {
        resource_ids: Vec<Ulid>,
        start: Ms,
        end: Ms,
        min_duration: Option<Ms>,
    },
    SelectBookingsMulti {
        resource_ids: Vec<Ulid>,
        filters: Vec<SpanFilter>,
    },
    SelectHoldsMulti {
        resource_ids: Vec<Ulid>,
        filters: Vec<SpanFilter>,
    },
    Listen {
        channel: String,
    },
    Unlisten {
        channel: String,
    },
    UnlistenAll,
}
