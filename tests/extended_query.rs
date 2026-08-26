//! End-to-end coverage of the extended query protocol (Parse/Bind/Describe/Execute) through a
//! real client. This is the surface the advertised SDK clients (Bun SQL, postgres.js) actually
//! use; the simple protocol is covered by tests/listen_notify.rs, and before these tests the
//! extended path was exercised only by in-process unit tests that hand-build a Portal.

mod common;

use common::{connect, data_rows, start_test_server};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Client;
use ulid::Ulid;

const INSERT_BOOKING: &str =
    r#"INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ($1, $2, $3, $4, $5)"#;

/// A resource with an open rule over 1000..2000 so bookings inside that span are admitted.
async fn create_bookable_resource(client: &Client) -> Ulid {
    let rid = Ulid::new();
    client
        .batch_execute(&format!("INSERT INTO resources (id) VALUES ('{rid}')"))
        .await
        .unwrap();
    let rule = Ulid::new();
    client
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule}', '{rid}', 1000, 2000, false)"#
        ))
        .await
        .unwrap();
    rid
}

#[tokio::test]
async fn prepared_insert_binds_params_and_roundtrips() {
    let (addr, _tm) = start_test_server().await;
    let client = connect(addr, "test").await;
    let rid = create_bookable_resource(&client).await;

    let stmt = client.prepare(INSERT_BOOKING).await.unwrap();
    // Describe over the wire: five placeholders, all declared VARCHAR (text substitution).
    assert_eq!(stmt.params(), &[Type::VARCHAR; 5]);

    let bid = Ulid::new().to_string();
    let rid_s = rid.to_string();
    // The quote exercises escaping through real Bind bytes, not a hand-built portal.
    let label = "O'Brien's slot";
    let params: [&(dyn ToSql + Sync); 5] = [&bid, &rid_s, &"1200", &"1400", &label];
    let inserted = client.execute(&stmt, &params).await.unwrap();
    assert_eq!(inserted, 1);

    // Verify through the independently covered simple protocol.
    let rows = data_rows(
        &client,
        &format!("SELECT * FROM bookings WHERE resource_id = '{rid_s}'"),
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(bid.as_str()));
    assert_eq!(rows[0].get(2), Some("1200"));
    assert_eq!(rows[0].get(3), Some("1400"));
    assert_eq!(rows[0].get(4), Some(label));
}

#[tokio::test]
async fn prepared_insert_with_null_parameter() {
    let (addr, _tm) = start_test_server().await;
    let client = connect(addr, "test").await;
    let rid = create_bookable_resource(&client).await;

    let stmt = client.prepare(INSERT_BOOKING).await.unwrap();
    let bid = Ulid::new().to_string();
    let rid_s = rid.to_string();
    let no_label: Option<&str> = None;
    let params: [&(dyn ToSql + Sync); 5] = [&bid, &rid_s, &"1200", &"1400", &no_label];
    let inserted = client.execute(&stmt, &params).await.unwrap();
    assert_eq!(inserted, 1);

    let rows = data_rows(
        &client,
        &format!("SELECT * FROM bookings WHERE resource_id = '{rid_s}'"),
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(bid.as_str()));
    assert_eq!(rows[0].get(4), None, "NULL bound parameter must stay a NULL label");
}

#[tokio::test]
async fn describe_select_reports_params_and_schema() {
    let (addr, _tm) = start_test_server().await;
    let client = connect(addr, "test").await;
    let rid = create_bookable_resource(&client).await;
    let rid_s = rid.to_string();

    let bid = Ulid::new();
    client
        .batch_execute(&format!(
            r#"INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ('{bid}', '{rid_s}', 1200, 1400, 'window seat')"#
        ))
        .await
        .unwrap();

    let stmt = client
        .prepare("SELECT * FROM bookings WHERE resource_id = $1")
        .await
        .unwrap();
    assert_eq!(stmt.params(), &[Type::VARCHAR]);
    let columns: Vec<(&str, Type)> = stmt
        .columns()
        .iter()
        .map(|c| (c.name(), c.type_().clone()))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("id", Type::VARCHAR),
            ("resource_id", Type::VARCHAR),
            ("start", Type::INT8),
            ("end", Type::INT8),
            ("label", Type::VARCHAR),
        ]
    );

    let rows = client.query(&stmt, &[&rid_s]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("id"), bid.to_string());
    assert_eq!(rows[0].get::<_, String>("label"), "window seat");
}

#[tokio::test]
async fn prepared_availability_query_matches_simple_protocol() {
    let (addr, _tm) = start_test_server().await;
    let client = connect(addr, "test").await;
    let rid = create_bookable_resource(&client).await;
    let rid_s = rid.to_string();

    let bid = Ulid::new();
    client
        .batch_execute(&format!(
            r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('{bid}', '{rid_s}', 1200, 1400)"#
        ))
        .await
        .unwrap();

    let stmt = client
        .prepare(r#"SELECT * FROM availability WHERE resource_id = $1 AND start >= $2 AND "end" <= $3"#)
        .await
        .unwrap();
    assert_eq!(stmt.params(), &[Type::VARCHAR; 3]);
    let params: [&(dyn ToSql + Sync); 3] = [&rid_s, &"1000", &"2000"];
    let extended = client.query(&stmt, &params).await.unwrap();

    let simple = data_rows(
        &client,
        &format!(
            r#"SELECT * FROM availability WHERE resource_id = '{rid_s}' AND start >= 1000 AND "end" <= 2000"#
        ),
    )
    .await;

    // The booking splits the 1000..2000 rule into two free windows.
    assert_eq!(simple.len(), 2);
    assert_eq!(simple[0].get(1), Some("1000"));
    assert_eq!(simple[0].get(2), Some("1200"));
    assert_eq!(simple[1].get(1), Some("1400"));
    assert_eq!(simple[1].get(2), Some("2000"));

    // The extended path must produce the same windows. start/end are INT8 and the server
    // text-encodes rows regardless of the requested result format, so numeric cells are
    // asserted via the simple protocol above and only VARCHAR cells are decoded here.
    assert_eq!(extended.len(), simple.len());
    for row in &extended {
        assert_eq!(row.get::<_, String>("resource_id"), rid_s);
    }
}
