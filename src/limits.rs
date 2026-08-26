//! Hard bounds on everything the transport boundary accepts.
//!
//! Untrusted input (SQL length, batch sizes, hierarchy depth, name lengths) is validated
//! against these constants before it reaches the engine, so one request cannot exhaust memory
//! or drive an unbounded allocation. Test builds shrink several bounds to keep the limit paths
//! cheap to exercise.

pub const MAX_QUERY_WINDOW_MS: i64 = 90 * 86_400_000; // 90 days
pub const MAX_SPAN_DURATION_MS: i64 = 3650 * 86_400_000; // ~10 years
pub const MIN_VALID_TIMESTAMP_MS: i64 = 0; // epoch
pub const MAX_VALID_TIMESTAMP_MS: i64 = 32_503_680_000_000; // year 3000
/// Ceiling on how far past the server's own clock a hold may expire (AVAIL-08). `place_hold`
/// clamps the client-requested `expires_at` to `now + this`; `DELTAT_MAX_HOLD_TTL_MS` overrides
/// it at startup.
pub const DEFAULT_MAX_HOLD_TTL_MS: i64 = 3_600_000; // 1 hour
pub const MAX_BATCH_SIZE: usize = 1_000;
#[cfg(not(test))]
pub const MAX_IN_CLAUSE_IDS: usize = 1_000;
#[cfg(test)]
pub const MAX_IN_CLAUSE_IDS: usize = 200;
#[cfg(not(test))]
pub const MAX_INTERVALS_PER_RESOURCE: usize = 100_000;
#[cfg(test)]
pub const MAX_INTERVALS_PER_RESOURCE: usize = 200;

#[cfg(not(test))]
pub const MAX_RESOURCES_PER_TENANT: usize = 100_000;
#[cfg(test)]
pub const MAX_RESOURCES_PER_TENANT: usize = 200;
pub const MAX_TENANTS: usize = 1_000;
pub const MAX_HIERARCHY_DEPTH: usize = 50;
pub const MAX_NAME_LEN: usize = 1_000;
pub const MAX_LABEL_LEN: usize = 10_000;
pub const MAX_TENANT_NAME_LEN: usize = 256;
pub const MAX_QUERY_LEN: usize = 1_048_576; // 1MB
/// Highest `$N` placeholder index a statement may name. Postgres's Bind message carries the
/// parameter count as a u16, so no real client can exceed this; a larger index in the SQL text is
/// hostile input and is rejected before it can size an allocation.
pub const MAX_PARAMS: usize = 65_535;
pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 100;
