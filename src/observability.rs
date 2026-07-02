//! Prometheus metrics: the metric-name constants plus the exporter setup.
//!
//! RED counters and latencies for requests, USE gauges for connections and tenants, and WAL
//! flush timings. `init` starts the exporter endpoint when a port is configured.

use std::net::SocketAddr;

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

/// Install Prometheus metrics exporter on the given port. No-op if port is None.
pub fn init(port: Option<u16>) {
    let Some(port) = port else { return };
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .expect("failed to install Prometheus metrics exporter");
    tracing::info!("metrics endpoint: http://0.0.0.0:{port}/metrics");
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
