//! Wire-level auth and tenant-boundary coverage. Nothing else in the suite presents a wrong
//! password or crosses dbnames, so a miswired startup handler (auth silently skipped) or a
//! dbname-to-tenant routing regression (all tenants sharing one engine) would otherwise keep
//! the whole suite green.

mod common;

use common::{connect, data_rows, start_test_server, try_connect, TEST_PASSWORD};
use tokio_postgres::error::SqlState;
use ulid::Ulid;

#[tokio::test]
async fn wrong_password_is_rejected() {
    let (addr, _tm) = start_test_server().await;

    let err = try_connect(addr, "test", "not-the-password")
        .await
        .expect_err("connection with a wrong password must be refused");
    assert_eq!(
        err.code(),
        Some(&SqlState::INVALID_PASSWORD),
        "expected 28P01 invalid_password, got: {err}"
    );
}

#[tokio::test]
async fn correct_password_is_accepted() {
    // Companion to wrong_password_is_rejected: proves the rejection there is the password check
    // firing, not the server refusing every connection.
    let (addr, _tm) = start_test_server().await;

    let client = try_connect(addr, "test", TEST_PASSWORD).await.unwrap();
    let rid = Ulid::new();
    client
        .batch_execute(&format!("INSERT INTO resources (id) VALUES ('{rid}')"))
        .await
        .unwrap();
}

#[tokio::test]
async fn tenant_data_is_invisible_across_dbnames() {
    let (addr, _tm) = start_test_server().await;
    let client_a = connect(addr, "tenant_a").await;
    let client_b = connect(addr, "tenant_b").await;

    let rid_a = Ulid::new().to_string();
    client_a
        .batch_execute(&format!("INSERT INTO resources (id) VALUES ('{rid_a}')"))
        .await
        .unwrap();

    let seen_by_a = data_rows(&client_a, "SELECT * FROM resources").await;
    assert!(
        seen_by_a.iter().any(|r| r.get(0) == Some(rid_a.as_str())),
        "tenant_a must see its own resource"
    );

    let seen_by_b = data_rows(&client_b, "SELECT * FROM resources").await;
    assert!(
        seen_by_b.is_empty(),
        "tenant_b must not see tenant_a's data, got {} rows",
        seen_by_b.len()
    );

    // And the reverse direction: a write in B stays in B.
    let rid_b = Ulid::new().to_string();
    client_b
        .batch_execute(&format!("INSERT INTO resources (id) VALUES ('{rid_b}')"))
        .await
        .unwrap();

    let seen_by_a = data_rows(&client_a, "SELECT * FROM resources").await;
    assert!(
        seen_by_a.iter().all(|r| r.get(0) != Some(rid_b.as_str())),
        "tenant_a must not see tenant_b's resource"
    );
}

#[tokio::test]
async fn same_dbname_shares_one_tenant() {
    // The isolation above must come from dbname routing, not per-connection state: two
    // connections naming the same dbname land in the same engine.
    let (addr, _tm) = start_test_server().await;
    let writer = connect(addr, "tenant_shared").await;
    let reader = connect(addr, "tenant_shared").await;

    let rid = Ulid::new().to_string();
    writer
        .batch_execute(&format!("INSERT INTO resources (id) VALUES ('{rid}')"))
        .await
        .unwrap();

    let seen = data_rows(&reader, "SELECT * FROM resources").await;
    assert!(
        seen.iter().any(|r| r.get(0) == Some(rid.as_str())),
        "a second connection to the same dbname must see the same data"
    );
}
