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

#[doc(hidden)]
pub mod auth;
#[doc(hidden)]
pub mod clock;
pub mod command;
pub mod engine;
#[doc(hidden)]
pub mod limits;
pub mod model;
#[doc(hidden)]
pub mod notify;
#[doc(hidden)]
pub mod observability;
#[doc(hidden)]
pub mod reaper;
pub mod sql;
#[doc(hidden)]
pub mod tenant;
#[doc(hidden)]
pub mod tls;
#[doc(hidden)]
pub mod wal;
pub mod wire;
