//! Prometheus metrics plus the tracing subscriber setup.
//!
//! RED counters and latencies for requests, USE gauges for connections and tenants, and WAL
//! flush timings. `init` starts the exporter endpoint when a port is configured; `init_tracing`
//! installs the log subscriber selected by `DELTAT_LOG_FORMAT` and `RUST_LOG`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::command::Command;

// ── RED metrics (request-driven) ────────────────────────────────

/// Counter: total queries executed. Labels: command, status.
pub const QUERIES_TOTAL: &str = "deltat_queries_total";

/// Histogram: query latency in seconds. Labels: command.
pub const QUERY_DURATION_SECONDS: &str = "deltat_query_duration_seconds";

// ── USE metrics (resource utilization) ──────────────────────────

/// Gauge: active TCP connections.
pub const CONNECTIONS_ACTIVE: &str = "deltat_connections_active";

/// Counter: total connections accepted.
pub const CONNECTIONS_TOTAL: &str = "deltat_connections_total";

/// Counter: connections rejected due to limit.
pub const CONNECTIONS_REJECTED_TOTAL: &str = "deltat_connections_rejected_total";

/// Gauge: number of active tenants (loaded engines).
pub const TENANTS_ACTIVE: &str = "deltat_tenants_active";

/// Counter: startup/auth failures.
pub const AUTH_FAILURES_TOTAL: &str = "deltat_auth_failures_total";

/// Histogram: WAL group-commit flush duration in seconds.
pub const WAL_FLUSH_DURATION_SECONDS: &str = "deltat_wal_flush_duration_seconds";

/// Histogram: WAL group-commit batch size (events per flush).
pub const WAL_FLUSH_BATCH_SIZE: &str = "deltat_wal_flush_batch_size";

/// Histogram: WAL compaction duration in seconds. Compaction runs inline in the writer
/// task, so this is latency every write on the tenant queues behind.
pub const WAL_COMPACTION_DURATION_SECONDS: &str = "deltat_wal_compaction_duration_seconds";

/// Counter: WAL append or flush failures. Labels: kind.
pub const WAL_ERRORS_TOTAL: &str = "deltat_wal_errors_total";

/// Gauge: 1 while a tenant's WAL is poisoned (every append fails), 0 otherwise. Labels: tenant.
pub const WAL_POISONED: &str = "deltat_wal_poisoned";

// ── Error taxonomy ──────────────────────────────────────────────

/// Counter: engine errors by variant. Labels: kind (see `EngineError::kind`).
///
/// Separates the expected steady state (a `conflict` when two clients race for one span)
/// from real failures (`wal`), which `deltat_queries_total{status="error"}` cannot.
pub const ENGINE_ERRORS_TOTAL: &str = "deltat_engine_errors_total";

/// Counter: statements rejected before execution (unparseable SQL, oversized statements).
///
/// Machine clients generate malformed SQL at a nonzero rate, and those statements never
/// reach the query counter because they die in the parser.
pub const PARSE_ERRORS_TOTAL: &str = "deltat_parse_errors_total";

/// Counter: queries exceeding the slow-query threshold. Labels: command.
pub const SLOW_QUERIES_TOTAL: &str = "deltat_slow_queries_total";

// ── Booking-domain counters ─────────────────────────────────────

/// Counter: holds accepted by the engine.
pub const HOLDS_PLACED_TOTAL: &str = "deltat_holds_placed_total";

/// Counter: holds converted into bookings via commit_hold.
pub const HOLDS_COMMITTED_TOTAL: &str = "deltat_holds_committed_total";

/// Counter: holds released explicitly by a client.
pub const HOLDS_RELEASED_TOTAL: &str = "deltat_holds_released_total";

/// Counter: holds reaped after expiry.
///
/// Placed minus committed minus released minus expired is the abandonment rate: how often a
/// client takes a slot out of circulation and never comes back for it.
pub const HOLDS_EXPIRED_TOTAL: &str = "deltat_holds_expired_total";

/// Counter: bookings confirmed.
pub const BOOKINGS_CREATED_TOTAL: &str = "deltat_bookings_created_total";

/// Counter: bookings deleted.
pub const BOOKINGS_DELETED_TOTAL: &str = "deltat_bookings_deleted_total";

/// Counter: past intervals removed by GC.
pub const GC_INTERVALS_COLLECTED_TOTAL: &str = "deltat_gc_intervals_collected_total";

// ── Connection lifecycle and notifications ──────────────────────

/// Histogram: connection lifetime in seconds.
pub const CONNECTION_DURATION_SECONDS: &str = "deltat_connection_duration_seconds";

/// Counter: connections closed, by reason. Labels: reason.
pub const CONNECTIONS_CLOSED_TOTAL: &str = "deltat_connections_closed_total";

/// Counter: LISTEN notifications a subscriber missed because its channel lagged.
///
/// A silent drop here means a client's view of a resource is stale with no error anywhere.
pub const NOTIFICATIONS_LAGGED_TOTAL: &str = "deltat_notifications_lagged_total";

static ENABLED: AtomicBool = AtomicBool::new(cfg!(test));

/// Whether a recorder is installed and per-query recording is worth doing.
///
/// The metrics macros build their key, including every label value, before they consult the
/// recorder, so a labelled call on the query path allocates on every statement even when the
/// exporter is switched off. Unlabelled counters compile to a static key and cost nothing, so
/// only the labelled hot-path calls need this guard.
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Install Prometheus metrics exporter on the given port. No-op if port is None.
pub fn init(port: Option<u16>) {
    let Some(port) = port else { return };
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .expect("failed to install Prometheus metrics exporter");
    ENABLED.store(true, Ordering::Relaxed);
    tracing::info!("metrics endpoint: http://0.0.0.0:{port}/metrics");
}


// ── Tracing subscriber setup ────────────────────────────────────

/// Log output format, selected by `DELTAT_LOG_FORMAT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

/// Parse `DELTAT_LOG_FORMAT`. Anything but "json" means text, so a typo degrades to the
/// human-readable default instead of killing startup.
pub fn parse_log_format(raw: Option<&str>) -> LogFormat {
    match raw {
        Some(v) if v.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Text,
    }
}

/// Build the level filter from `RUST_LOG`, defaulting to info when unset or unparseable.
pub fn log_env_filter(rust_log: Option<&str>) -> tracing_subscriber::EnvFilter {
    rust_log
        .and_then(|raw| raw.parse().ok())
        .unwrap_or_else(|| tracing_subscriber::EnvFilter::new("info"))
}

/// Install the global tracing subscriber from `DELTAT_LOG_FORMAT` and `RUST_LOG`.
pub fn init_tracing() {
    let format = parse_log_format(std::env::var("DELTAT_LOG_FORMAT").ok().as_deref());
    let filter = log_env_filter(std::env::var("RUST_LOG").ok().as_deref());
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Text => builder.init(),
    }
}

/// Map a Command variant to a short label for metrics.
pub fn command_label(cmd: &Command) -> &'static str {
    match cmd {
        Command::InsertResource { .. } => "insert_resource",
        Command::UpdateResource { .. } => "update_resource",
        Command::DeleteResource { .. } => "delete_resource",
        Command::BatchInsertResources { .. } => "batch_insert_resources",
        Command::InsertRule { .. } => "insert_rule",
        Command::BatchInsertRules { .. } => "batch_insert_rules",
        Command::UpdateRule { .. } => "update_rule",
        Command::DeleteRule { .. } => "delete_rule",
        Command::InsertHold { .. } => "insert_hold",
        Command::DeleteHold { .. } => "delete_hold",
        Command::CommitHold { .. } => "commit_hold",
        Command::InsertBooking { .. } => "insert_booking",
        Command::BatchInsertBookings { .. } => "batch_insert_bookings",
        Command::DeleteBooking { .. } => "delete_booking",
        Command::SelectResources { .. } => "select_resources",
        Command::SelectRules { .. } => "select_rules",
        Command::SelectBookings { .. } => "select_bookings",
        Command::SelectHolds { .. } => "select_holds",
        Command::SelectAvailability { .. } => "select_availability",
        Command::SelectMultiAvailability { .. } => "select_multi_availability",
        Command::SelectAvailabilityMulti { .. } => "select_availability_multi",
        Command::SelectBookingsMulti { .. } => "select_bookings_multi",
        Command::SelectHoldsMulti { .. } => "select_holds_multi",
        Command::Listen { .. } => "listen",
        Command::Unlisten { .. } => "unlisten",
        Command::UnlistenAll => "unlisten_all",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_format_defaults_to_text() {
        assert_eq!(parse_log_format(None), LogFormat::Text);
    }

    #[test]
    fn log_format_selects_json() {
        assert_eq!(parse_log_format(Some("json")), LogFormat::Json);
    }

    #[test]
    fn log_format_ignores_ascii_case() {
        assert_eq!(parse_log_format(Some("JSON")), LogFormat::Json);
    }

    #[test]
    fn log_format_falls_back_to_text_on_unknown_value() {
        assert_eq!(parse_log_format(Some("yaml")), LogFormat::Text);
    }

    #[test]
    fn env_filter_defaults_to_info() {
        assert_eq!(log_env_filter(None).to_string(), "info");
    }

    #[test]
    fn env_filter_honors_rust_log() {
        assert_eq!(
            log_env_filter(Some("deltat=debug")).to_string(),
            "deltat=debug"
        );
    }

    #[test]
    fn env_filter_falls_back_to_info_on_unparseable_rust_log() {
        assert_eq!(log_env_filter(Some("deltat=notalevel")).to_string(), "info");
    }
}
