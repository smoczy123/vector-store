/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::tests::*;
use httpclient::HttpClient;
use scylla::client::session_builder::SessionBuilder;
use std::sync::Arc;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, common::init)
        .with_cleanup(timeout, common::cleanup)
        .with_test("simple_create_drop_index", timeout, ser_de_all_types)
}

async fn ser_de_all_types(actors: TestActors) {
    info!("started");

    let session = Arc::new(
        SessionBuilder::new()
            .known_node(actors.services_subnet.ip(common::DB_OCTET).to_string())
            .build()
            .await
            .expect("failed to create session"),
    );
    let client =
        HttpClient::new((actors.services_subnet.ip(common::VS_OCTET), common::VS_PORT).into());

    let cases = vec![
        ("ascii", "random_text"),
        ("bigint", "1234567890123456789"),
        ("boolean", "true"),
        ("date", "2023-10-01"),
        ("decimal", "12345.6789"),
        ("double", "3.14159"),
        ("float", "2.71828"),
        ("int", "42"),
        ("smallint", "123"),
        ("tinyint", "7"),
        ("uuid", "550e8400-e29b-41d4-a716-446655440000"),
        ("timeuuid", "550e8400-e29b-41d4-a716-446655440000"),
        ("time", "12:34:56.789"),
        ("timestamp", "2023-10-01T12:34:56.789Z"),
        ("text", "some_text"),
    ];

    session.query_unpaged(
        "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        (),
    ).await.expect("failed to create a keyspace");

    session
        .use_keyspace("ks", false)
        .await
        .expect("failed to use a keyspace");

    for (typ, data) in &cases {
        session
            .query_unpaged(
                format!(
                    "CREATE TABLE tbl_{} (id {} PRIMARY KEY, vec vector<float, 3>) WITH cdc = {{'enabled': true }}",
                    typ, typ
                ),
                (),
            )
            .await
            .expect("failed to create a table");

        session
            .query_unpaged(
                format!("CREATE INDEX idx ON tbl_{}(vec) USING 'vector_index'", typ),
                (),
            )
            .await
            .expect("failed to create an index");

        while client.indexes().await.is_empty() {}
        let indexes = client.indexes().await;
        assert_eq!(indexes.len(), 1);
        session
            .query_unpaged(
                format!(
                    "INSERT INTO tbl_{} (id, vec) VALUES ('{}', [1.0, 2.0, 3.0])",
                    typ, data
                ),
                (),
            )
            .await
            .expect("failed to insert data");
        session
            .query_unpaged(
                format!(
                    "SELECT * FROM tbl_{} ORDER BY vec ANN OF [1.0, 2.0, 3.0] LIMIT 1",
                    typ
                ),
                (),
            )
            .await
            .expect("failed to select data");
        session
            .query_unpaged("DROP INDEX idx", ())
            .await
            .expect("failed to drop an index");

        while !client.indexes().await.is_empty() {}
    }

    session
        .query_unpaged("DROP KEYSPACE ks", ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
