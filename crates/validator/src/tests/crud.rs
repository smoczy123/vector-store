/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::tests::*;
use httpclient::HttpClient;
use scylla::client::session_builder::SessionBuilder;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, common::init)
        .with_cleanup(timeout, common::cleanup)
        .with_test(
            "simple_create_drop_index",
            timeout,
            simple_create_drop_index,
        )
}

async fn simple_create_drop_index(actors: TestActors) {
    info!("started");

    let session = Arc::new(
        SessionBuilder::new()
            .known_node(actors.services_subnet.ip(common::DB_OCTET).to_string())
            .build()
            .await
            .expect("failed to create session"),
    );
    let client = HttpClient::new((actors.services_subnet.ip(common::VS_OCTET), common::VS_PORT).into());

    session.query_unpaged(
        "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        (),
    ).await.expect("failed to create a keyspace");

    session
        .use_keyspace("ks", false)
        .await
        .expect("failed to use a keyspace");

    session
        .query_unpaged(
            "
            CREATE TABLE tbl (id BIGINT PRIMARY KEY, embedding VECTOR<FLOAT, 3>)
            WITH cdc = {'enabled': true }
            ",
            (),
        )
        .await
        .expect("failed to create a table");

    session
        .query_unpaged(
            "CREATE INDEX idx ON tbl(embedding) USING 'vector_index'",
            (),
        )
        .await
        .expect("failed to create an index");

    while client.indexes().await.is_empty() {}
    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].keyspace.as_ref(), "ks");
    assert_eq!(indexes[0].index.as_ref(), "idx");

    session
        .query_unpaged("DROP INDEX idx", ())
        .await
        .expect("failed to drop an index");

    while !client.indexes().await.is_empty() {}

    session
        .query_unpaged("DROP KEYSPACE ks", ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
