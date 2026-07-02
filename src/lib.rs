//! deltat is a time-allocation database built around one primitive: the half-open interval
//! `[start, end)` on a line of `i64` Unix milliseconds. Every rule, hold, and booking is an
//! interval, and the question the engine answers in sub-millisecond time is whether intervals
//! collide. Resources form a tree; availability is inherited down it and checked against
//! per-resource capacity and turnaround buffers.
//!
//! The kernel is transport-neutral. An adapter translates an external protocol into a
//! [`command::Command`] and hands it to the [`wire`] server; SQL over pgwire is the adapter
//! that ships today.
//!
//! Module map:
//! - [`engine`]: the state machine (availability, conflict detection, mutations, queries).
//! - [`model`]: the core value types (`Span`, `Interval`, `Event`, `ResourceState`).
//! - [`command`]: the transport-neutral command vocabulary.
//! - [`sql`]: SQL text into a `Command`, the one adapter that exists now.
//! - [`wire`]: the pgwire server that runs commands and streams results.
//! - `wal`, `tenant`, `notify`, `reaper`, `clock`, `auth`, `tls`, `observability`, `limits`:
//!   durability, isolation, and the supporting machinery around the kernel.

pub mod auth;
pub mod clock;
pub mod command;
pub mod engine;
pub mod limits;
pub mod model;
pub mod notify;
pub mod observability;
pub mod reaper;
pub mod sql;
pub mod tenant;
pub mod tls;
pub mod wal;
pub mod wire;
