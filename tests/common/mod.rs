//! Shared harness for wire-level integration tests.
//!
//! Credentials are explicit constants here rather than server defaults or env vars, so these
//! tests keep checking the same contract even if the server's default auth setup changes.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, SimpleQueryRow};
use ulid::Ulid;

use deltat::tenant::TenantManager;
use deltat::wire;

pub const TEST_PASSWORD: &str = "wiretest-secret";

pub async fn start_test_server() -> (SocketAddr, Arc<TenantManager>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let dir = std::env::temp_dir().join(format!("deltat_int_test_{}", Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let tm = Arc::new(TenantManager::new(dir, 1000, 604_800_000));

    let tm2 = tm.clone();
    tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            let tm = tm2.clone();
            tokio::spawn(async move {
                let _ = wire::process_connection(
                    socket,
                    tm,
                    TEST_PASSWORD.to_string(),
                    None,
                    0,
                    0,
                )
                .await;
            });
        }
    });

    (addr, tm)
}

pub async fn try_connect(
    addr: SocketAddr,
    dbname: &str,
    password: &str,
) -> Result<Client, tokio_postgres::Error> {
    let mut config = Config::new();
    config
        .host(addr.ip().to_string())
        .port(addr.port())
        .dbname(dbname)
        .user("deltat")
        .password(password);

    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

pub async fn connect(addr: SocketAddr, dbname: &str) -> Client {
    try_connect(addr, dbname, TEST_PASSWORD).await.unwrap()
}

/// Run `sql` over the simple protocol and keep only the data rows.
pub async fn data_rows(client: &Client, sql: &str) -> Vec<SimpleQueryRow> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}
