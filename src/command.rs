//! The transport-neutral command vocabulary.
//!
//! `Command` is the boundary between an adapter and the kernel. SQL/pgwire is one adapter today
//! (`sql::parse_sql` produces a `Command`); the framed, HTTP, and MCP transports (PROTO-01/03/04)
//! will be siblings that build the same `Command` and hand it to `wire::execute_command`. It depends
//! only on the kernel value types — never on a specific transport (no `sqlparser`) — so adding a
//! transport never drags another transport's parser along, and the kernel can be carved into its own
//! crate without this seam coming with it.

use ulid::Ulid;

use crate::model::*;

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
    UpdateResource {
        id: Ulid,
        name: Option<String>,
        capacity: u32,
        buffer_after: Option<Ms>,
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
    },
    SelectHolds {
        resource_id: Ulid,
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
    },
    SelectHoldsMulti {
        resource_ids: Vec<Ulid>,
    },
    Listen {
        channel: String,
    },
    Unlisten {
        channel: String,
    },
    UnlistenAll,
}
