use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::info;

use deltat::tenant::TenantManager;
use deltat::wire;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    deltat::observability::init_tracing();

    // A panic in a spawned connection task dies on stderr outside the log stream, invisible to a
    // JSON log collector. Route it through tracing first, then run the default hook so backtrace
    // printing and abort/unwind behavior stay exactly as before.
    let default_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic payload");
        let location = info
            .location()
            .map_or_else(|| "unknown".to_string(), |l| l.to_string());
        tracing::error!(panic.message = message, panic.location = %location, "panic");
        default_panic_hook(info);
    }));

    let metrics_port: Option<u16> = std::env::var("DELTAT_METRICS_PORT")
        .ok()
        .and_then(|s| s.parse().ok());
    deltat::observability::init(metrics_port);

    let port = std::env::var("DELTAT_PORT").unwrap_or_else(|_| "5433".into());
    let bind = std::env::var("DELTAT_BIND").unwrap_or_else(|_| "0.0.0.0".into());
    let data_dir = std::env::var("DELTAT_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let password = match deltat::auth::resolve_password(std::env::var("DELTAT_PASSWORD").ok()) {
        deltat::auth::ServerPassword::Configured(p) => p,
        deltat::auth::ServerPassword::Generated(p) => {
            // Printed to stdout exactly once so the quickstart works without shipping the old
            // known default "deltat". Set DELTAT_PASSWORD to skip this.
            println!("----------------------------------------------------------------");
            println!("  DELTAT_PASSWORD is not set. Generated a random password:");
            println!();
            println!("      {p}");
            println!();
            println!("  It changes on every restart and is not shown again.");
            println!("  Set DELTAT_PASSWORD to use a stable password.");
            println!("----------------------------------------------------------------");
            p
        }
    };
    // Optional per-tenant credentials: tenants listed here accept only their own password.
    // Malformed input is a startup error, never a silently weaker auth config.
    let tenant_passwords = match std::env::var("DELTAT_TENANT_PASSWORDS") {
        Ok(raw) => deltat::auth::parse_tenant_passwords(&raw)
            .map_err(|e| format!("invalid DELTAT_TENANT_PASSWORDS: {e}"))?,
        Err(_) => std::collections::HashMap::new(),
    };
    let auth_source = Arc::new(deltat::auth::DeltaTAuthSource::with_tenant_passwords(
        password,
        tenant_passwords,
    ));
    let max_connections: usize = std::env::var("DELTAT_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);
    let compact_threshold: u64 = std::env::var("DELTAT_COMPACT_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let gc_retention_ms: i64 = std::env::var("DELTAT_GC_RETENTION_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|v| *v >= 0) // a negative retention would push the GC cutoff into the future
        .unwrap_or(604_800_000); // 7 days
    let max_hold_ttl_ms: i64 = std::env::var("DELTAT_MAX_HOLD_TTL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|v| *v >= 0) // a negative ceiling would clamp every hold into the past
        .unwrap_or(deltat::limits::DEFAULT_MAX_HOLD_TTL_MS);
    // Post-auth connection lifetime guards (0 = disabled, the default; long-lived LISTEN is a
    // legitimate product use). A public deployment sets these to bound idle/squatting streams.
    let max_conn_age_ms: u64 = std::env::var("DELTAT_MAX_CONN_AGE_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let max_idle_ms: u64 = std::env::var("DELTAT_MAX_IDLE_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let tls_cert = std::env::var("DELTAT_TLS_CERT").ok();
    let tls_key = std::env::var("DELTAT_TLS_KEY").ok();
    let tls_acceptor =
        deltat::tls::load_tls_acceptor(tls_cert.as_deref(), tls_key.as_deref())?;

    // Ensure data directory exists
    std::fs::create_dir_all(&data_dir)?;

    let tenant_manager = Arc::new(
        TenantManager::new(PathBuf::from(&data_dir), compact_threshold, gc_retention_ms)
            .with_max_hold_ttl(max_hold_ttl_ms),
    );
    let semaphore = Arc::new(Semaphore::new(max_connections));

    let addr = format!("{bind}:{port}");
    let listener = TcpListener::bind(&addr).await?;
    info!("deltat listening on {addr}");
    info!("  data_dir: {data_dir}");
    info!("  max_connections: {max_connections}");
    info!("  tls: {}", if tls_acceptor.is_some() { "enabled" } else { "disabled" });
    info!("  metrics: {}", metrics_port.map_or("disabled".to_string(), |p| format!("http://0.0.0.0:{p}/metrics")));

    // Graceful shutdown: stop accepting on SIGTERM/ctrl-c, drain in-flight connections
    let shutdown = async {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
        }
    };
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, peer) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::error!("accept error: {e}");
                        continue;
                    }
                };

                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        tracing::warn!("connection limit reached, rejecting {peer}");
                        metrics::counter!(deltat::observability::CONNECTIONS_REJECTED_TOTAL).increment(1);
                        drop(socket);
                        continue;
                    }
                };

                info!("connection from {peer}");
                metrics::counter!(deltat::observability::CONNECTIONS_TOTAL).increment(1);
                metrics::gauge!(deltat::observability::CONNECTIONS_ACTIVE).increment(1.0);
                let tm = tenant_manager.clone();
                let auth = auth_source.clone();
                let tls = tls_acceptor.clone();

                tokio::spawn(async move {
                    let _permit = permit; // held until connection closes
                    let started = std::time::Instant::now();
                    // Err surfaces only from the startup/TLS phase; post-auth failures and the
                    // idle/max-age guards close inside the wire loop and return Ok, so those
                    // count as "normal" here. Startup failures log at warn with the peer since
                    // a debug-level auth failure would hide credential stuffing.
                    let reason = match wire::process_connection_with_auth(socket, tm, auth, tls, max_conn_age_ms, max_idle_ms).await {
                        Ok(()) => "normal",
                        Err(e) => {
                            tracing::warn!("connection failed from {peer}: {e}");
                            "error"
                        }
                    };
                    metrics::histogram!(deltat::observability::CONNECTION_DURATION_SECONDS)
                        .record(started.elapsed().as_secs_f64());
                    metrics::counter!(deltat::observability::CONNECTIONS_CLOSED_TOTAL, "reason" => reason)
                        .increment(1);
                    metrics::gauge!(deltat::observability::CONNECTIONS_ACTIVE).decrement(1.0);
                });
            }
            _ = &mut shutdown => {
                info!("shutdown signal received, stopping accept loop");
                break;
            }
        }
    }

    // Wait for in-flight connections to finish (up to 10s)
    info!("draining connections...");
    let drain_deadline = tokio::time::sleep(std::time::Duration::from_secs(10));
    tokio::pin!(drain_deadline);

    loop {
        if semaphore.available_permits() == max_connections {
            info!("all connections drained");
            break;
        }
        tokio::select! {
            _ = &mut drain_deadline => {
                let remaining = max_connections - semaphore.available_permits();
                tracing::warn!("drain timeout, {remaining} connections still open");
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        }
    }

    info!("deltat stopped");
    Ok(())
}
