/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::usearch::setup_store_and_wait_for_index;
use vector_store::httproutes::Status;

#[tokio::test]
async fn status_is_serving_after_creation() {
    crate::enable_tracing();
    let (_index, client, _db, _server, _node_state) = setup_store_and_wait_for_index().await;

    let result = client.status().await;
    assert_eq!(result.unwrap(), Status::Serving);
}
