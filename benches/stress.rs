use std::time::{Duration, Instant};

use tokio_postgres::{Config, NoTls};
use ulid::Ulid;

const HOUR: i64 = 3_600_000; // 1 hour in ms

/// A client on a fresh tenant of its own.
async fn connect(host: &str, port: u16) -> tokio_postgres::Client {
    connect_to(host, port, &format!("bench_{}", Ulid::new())).await
}

/// A client on a named tenant, so several clients can contend for the same resources.
async fn connect_to(host: &str, port: u16, tenant: &str) -> tokio_postgres::Client {
    let mut config = Config::new();
    config
        .host(host)
        .port(port)
        .dbname(tenant)
        .user("deltat")
        .password("deltat");

    let (client, conn) = config.connect(NoTls).await.expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64) * p / 100.0) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn print_latency(label: &str, latencies: &mut [Duration]) {
    latencies.sort();
    let total: Duration = latencies.iter().sum();
    let avg = total / latencies.len() as u32;
    println!("  {label}:");
    println!(
        "    n={}, avg={:.2}ms, p50={:.2}ms, p95={:.2}ms, p99={:.2}ms, max={:.2}ms",
        latencies.len(),
        avg.as_secs_f64() * 1000.0,
        percentile(latencies, 50.0).as_secs_f64() * 1000.0,
        percentile(latencies, 95.0).as_secs_f64() * 1000.0,
        percentile(latencies, 99.0).as_secs_f64() * 1000.0,
        latencies.last().unwrap().as_secs_f64() * 1000.0,
    );
}

struct Resource {
    id: Ulid,
    capacity: u32,
}

async fn setup(client: &tokio_postgres::Client) -> Vec<Resource> {
    let capacities = [1, 1, 1, 1, 1, 5, 5, 5, 10, 10];
    let mut resources = Vec::new();

    for &cap in &capacities {
        let rid = Ulid::new();
        client
            .batch_execute(&format!(
                "INSERT INTO resources (id, capacity) VALUES ('{rid}', {cap})"
            ))
            .await
            .unwrap();

        let rule_id = Ulid::new();
        client
            .batch_execute(&format!(
                r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', 0, 31536000000, false)"#
            ))
            .await
            .unwrap();

        resources.push(Resource { id: rid, capacity: cap });
    }

    println!("  created {} resources", resources.len());
    resources
}

async fn phase1_sequential(host: &str, port: u16, resource: &Resource) {
    let client = connect(host, port).await;
    let rid = resource.id;

    // Re-create resource in this tenant
    client
        .batch_execute(&format!(
            "INSERT INTO resources (id, capacity) VALUES ('{rid}', {})",
            resource.capacity
        ))
        .await
        .unwrap();
    let rule_id = Ulid::new();
    client
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', 0, 31536000000, false)"#
        ))
        .await
        .unwrap();

    let n = 2000;
    let mut latencies = Vec::with_capacity(n);
    let start = Instant::now();

    for i in 0..n {
        let bid = Ulid::new();
        let s = (i as i64) * HOUR;
        let e = s + HOUR;
        let t = Instant::now();
        client
            .batch_execute(&format!(
                r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('{bid}', '{rid}', {s}, {e})"#
            ))
            .await
            .unwrap();
        latencies.push(t.elapsed());
    }

    let elapsed = start.elapsed();
    let ops = n as f64 / elapsed.as_secs_f64();
    println!("  {n} bookings in {:.2}s = {ops:.0} ops/sec", elapsed.as_secs_f64());
    print_latency("write latency", &mut latencies);
}

async fn phase2_concurrent(host: &str, port: u16, resources: &[Resource]) {
    let n_tasks = 10;
    let n_per_task = 200;

    let start = Instant::now();
    let mut handles = Vec::new();

    for i in 0..n_tasks {
        let host = host.to_string();
        let rid = resources[i % resources.len()].id;
        let cap = resources[i % resources.len()].capacity;

        handles.push(tokio::spawn(async move {
            let client = connect(&host, port).await;

            // Each task uses its own tenant (unique dbname from connect())
            client
                .batch_execute(&format!(
                    "INSERT INTO resources (id, capacity) VALUES ('{rid}', {cap})"
                ))
                .await
                .unwrap();
            let rule_id = Ulid::new();
            client
                .batch_execute(&format!(
                    r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', 0, 31536000000, false)"#
                ))
                .await
                .unwrap();

            for j in 0..n_per_task {
                let bid = Ulid::new();
                let s = (j as i64) * HOUR;
                let e = s + HOUR;
                client
                    .batch_execute(&format!(
                        r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('{bid}', '{rid}', {s}, {e})"#
                    ))
                    .await
                    .unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total = n_tasks * n_per_task;
    let ops = total as f64 / elapsed.as_secs_f64();
    println!(
        "  {n_tasks} tasks x {n_per_task} bookings = {total} total in {:.2}s = {ops:.0} ops/sec",
        elapsed.as_secs_f64()
    );
}

/// Read latency on ONE resource while N clients book on that same resource, in one tenant
/// (issue #25). A reader and a writer meet on the resource's lock, which is the contention that
/// matters; giving every client its own tenant, as this phase used to, measured none of it.
async fn phase3_reads_beside_writers(host: &str, port: u16) {
    const DAY: i64 = 24 * HOUR;
    const READERS: usize = 10;
    const READS_PER_READER: usize = 300;
    // Inside the kernel's 90-day query cap; the old 365-day window was refused and panicked here.
    const WINDOW: i64 = 90 * DAY;
    // Writers book after the read window, so every read does the same work whatever they add.
    const WRITES_FROM: i64 = 100 * DAY;

    println!("  writers  read p50   read p99   reads/s   writes/s");
    for writers in [0usize, 1, 8] {
        let tenant = format!("bench_{}", Ulid::new());
        let rid = Ulid::new();
        let setup = connect_to(host, port, &tenant).await;
        setup
            .batch_execute(&format!("INSERT INTO resources (id, capacity) VALUES ('{rid}', 1)"))
            .await
            .unwrap();
        setup
            .batch_execute(&format!(
                r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{}', '{rid}', 0, {}, false)"#,
                Ulid::new(),
                3600 * DAY // just under the kernel's ten-year span cap
            ))
            .await
            .unwrap();
        for i in 0..50 {
            let s = i * HOUR * 3;
            setup
                .batch_execute(&format!(
                    r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('{}', '{rid}', {s}, {})"#,
                    Ulid::new(),
                    s + HOUR
                ))
                .await
                .unwrap();
        }

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let written = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut writer_handles = Vec::new();
        for w in 0..writers {
            let (host, tenant) = (host.to_string(), tenant.clone());
            let (stop, written) = (stop.clone(), written.clone());
            writer_handles.push(tokio::spawn(async move {
                let client = connect_to(&host, port, &tenant).await;
                let mut i = 0i64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // Each writer owns a disjoint run of hours, so no write conflicts with another.
                    let s = WRITES_FROM + (w as i64 * 8_000 + i) * HOUR;
                    let booked = client
                        .batch_execute(&format!(
                            r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('{}', '{rid}', {s}, {})"#,
                            Ulid::new(),
                            s + HOUR
                        ))
                        .await;
                    if booked.is_ok() {
                        written.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    i += 1;
                }
            }));
        }

        let started = Instant::now();
        let mut reader_handles = Vec::new();
        for _ in 0..READERS {
            let (host, tenant) = (host.to_string(), tenant.clone());
            reader_handles.push(tokio::spawn(async move {
                let client = connect_to(&host, port, &tenant).await;
                let mut latencies = Vec::with_capacity(READS_PER_READER);
                for _ in 0..READS_PER_READER {
                    let t = Instant::now();
                    client
                        .batch_execute(&format!(
                            r#"SELECT * FROM availability WHERE resource_id = '{rid}' AND start >= 0 AND "end" <= {WINDOW}"#
                        ))
                        .await
                        .unwrap();
                    latencies.push(t.elapsed());
                }
                latencies
            }));
        }
        let mut latencies = Vec::new();
        for h in reader_handles {
            latencies.extend(h.await.unwrap());
        }
        let elapsed = started.elapsed().as_secs_f64();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for h in writer_handles {
            let _ = h.await;
        }

        latencies.sort();
        println!(
            "  {writers:>7}  {:>6.2}ms  {:>7.2}ms  {:>8.0}  {:>8.0}",
            percentile(&latencies, 50.0).as_secs_f64() * 1000.0,
            percentile(&latencies, 99.0).as_secs_f64() * 1000.0,
            latencies.len() as f64 / elapsed,
            written.load(std::sync::atomic::Ordering::Relaxed) as f64 / elapsed,
        );
    }
}

async fn phase4_connection_storm(host: &str, port: u16) {
    let n_conns = 50;
    let ops_per_conn = 10;

    let start = Instant::now();
    let mut handles = Vec::new();
    let success = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    for _ in 0..n_conns {
        let host = host.to_string();
        let success = success.clone();
        handles.push(tokio::spawn(async move {
            let client = connect(&host, port).await;
            let rid = Ulid::new();
            client
                .batch_execute(&format!(
                    "INSERT INTO resources (id, capacity) VALUES ('{rid}', 10)"
                ))
                .await
                .unwrap();
            let rule_id = Ulid::new();
            client
                .batch_execute(&format!(
                    r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', 0, 31536000000, false)"#
                ))
                .await
                .unwrap();

            for i in 0..ops_per_conn {
                let bid = Ulid::new();
                let s = (i as i64) * HOUR;
                let e = s + HOUR;
                client
                    .batch_execute(&format!(
                        r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('{bid}', '{rid}', {s}, {e})"#
                    ))
                    .await
                    .unwrap();
            }
            success.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed();
    let ok = success.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "  {n_conns} connections, {ops_per_conn} ops each: {ok}/{n_conns} succeeded in {:.2}s",
        elapsed.as_secs_f64()
    );
}

#[tokio::main]
async fn main() {
    let host = std::env::var("DELTAT_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("DELTAT_PORT")
        .unwrap_or_else(|_| "5433".into())
        .parse()
        .expect("invalid DELTAT_PORT");

    println!("=== deltat stress benchmark ===");
    println!("target: {host}:{port}\n");

    // Each phase uses its own tenant (unique dbname) to avoid interference

    println!("[setup]");
    let setup_client = connect(&host, port).await;
    let resources = setup(&setup_client).await;
    drop(setup_client);

    println!("\n[phase 1] sequential write throughput");
    phase1_sequential(&host, port, &resources[9]).await; // cap=10 resource

    println!("\n[phase 2] concurrent write throughput");
    phase2_concurrent(&host, port, &resources).await;

    println!("\n[phase 3] reads on one resource while others book on it");
    phase3_reads_beside_writers(&host, port).await;

    println!("\n[phase 4] connection storm");
    phase4_connection_storm(&host, port).await;

    println!("\n=== benchmark complete ===");
}
