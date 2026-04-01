/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use tracing::info;
use vector_search_validator_tests::common;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, common::init)
        .with_cleanup(timeout, common::cleanup)
        .with_test(
            "global_ann_with_timestamp_eq_filter",
            timeout,
            global_ann_with_timestamp_eq_filter,
        )
        .with_test(
            "local_ann_with_timestamp_gte_filter",
            timeout,
            local_ann_with_timestamp_gte_filter,
        )
}

/// Reproducer for VECTOR-593: ANN query with global index and a timestamp
/// equality filter using a space-separated CQL timestamp must not fail.
#[framed]
async fn global_ann_with_timestamp_eq_filter(actors: TestActors) {
    info!("started");

    let (session, clients) = common::prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk TEXT, v VECTOR<FLOAT, 3>, created_at TIMESTAMP, PRIMARY KEY (pk, created_at)",
        None,
    )
    .await;

    info!("Insert rows with various timestamps");
    let rows = [
        ("a", [0.1, 0.2, 0.3], "2024-06-15 10:00:00.000Z"),
        ("b", [0.4, 0.5, 0.6], "1970-01-01 00:01:04.000Z"),
        ("c", [0.7, 0.8, 0.9], "2024-08-20 14:30:00.000Z"),
    ];
    for (pk, vec, ts) in &rows {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v, created_at) VALUES ('{pk}', {vec:?}, '{ts}')"),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    info!("Create a global ANN index");
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;
    for client in &clients {
        wait_for_index(client, &index).await;
    }

    info!("Query with space-separated timestamp equality filter");
    let results = get_query_results(
        format!(
            "SELECT pk FROM {table} \
             WHERE created_at = '1970-01-01 00:01:04' \
             ORDER BY v ANN OF [0.4, 0.5, 0.6] LIMIT 5 \
             ALLOW FILTERING"
        ),
        &session,
    )
    .await;
    let result_rows = results.rows::<(String,)>().expect("failed to get rows");
    assert_eq!(result_rows.rows_remaining(), 1, "Expected exactly one matching row");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

/// Reproducer for VECTOR-593: ANN query with local index and a timestamp
/// inequality filter using a date-only CQL timestamp must not fail.
#[framed]
async fn local_ann_with_timestamp_gte_filter(actors: TestActors) {
    info!("started");

    let (session, clients) = common::prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk TEXT, board_id INT, v VECTOR<FLOAT, 3>, created_at TIMESTAMP, \
         PRIMARY KEY ((pk, board_id), created_at)",
        None,
    )
    .await;

    info!("Insert rows with various timestamps");
    let rows = [
        ("alice", 42, [0.1, 0.2, 0.3], "2024-06-15 10:00:00.000Z"),
        ("alice", 42, [0.12, 0.34, 0.56], "2024-08-20 14:30:00.000Z"),
        ("alice", 42, [0.3, 0.3, 0.3], "2023-01-10 08:00:00.000Z"),
    ];
    for (pk, board, vec, ts) in &rows {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {table} (pk, board_id, v, created_at) \
                     VALUES ('{pk}', {board}, {vec:?}, '{ts}')"
                ),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    info!("Create a local ANN index");
    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .partition_columns(["pk", "board_id"]),
    )
    .await;
    for client in &clients {
        wait_for_index(client, &index).await;
    }

    info!("Query with timestamp inequality filter (>= '2024-01-01')");
    let results = get_query_results(
        format!(
            "SELECT pk FROM {table} \
             WHERE pk = 'alice' AND board_id = 42 \
             AND created_at >= '2024-01-01' \
             ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5"
        ),
        &session,
    )
    .await;
    let result_rows = results.rows::<(String,)>().expect("failed to get rows");
    assert_eq!(
        result_rows.rows_remaining(),
        2,
        "Expected two rows with created_at >= 2024-01-01"
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}
