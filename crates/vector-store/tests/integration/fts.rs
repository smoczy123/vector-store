/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::create_config_channels;
use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::db_basic::ScanFn;
use crate::db_basic::Table;
use crate::wait_for;
use httpapi::IndexStatus;
use httpclient::HttpClient;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use vector_store::ColumnName;
use vector_store::Config;
use vector_store::DbIndexPartitioning;
use vector_store::HttpServerExt;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsFts;
use vector_store::Timestamp;
use vector_store::node_state::NodeState;

async fn setup_fts_store(
    primary_keys: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    fullscan_fn: Option<ScanFn>,
    cdc_fn: Option<ScanFn>,
) -> (
    impl std::future::Future<Output = (HttpClient, impl Sized, impl Sized)>,
    IndexMetadata,
    DbBasic,
    Sender<NodeState>,
) {
    let config = Config {
        vector_store_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        fulltext_indexes: true,
        ..Default::default()
    };

    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();
    let (db_actor, db) = db_basic::new(node_state.clone());

    let columns: Arc<HashMap<_, _>> = Arc::new(columns.into_iter().collect());
    let index = IndexMetadata {
        keyspace_name: "fts_ks".into(),
        table_name: "documents".into(),
        index_name: "fts_idx".into(),
        target_column: "content".into(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new(columns.keys().cloned().collect()),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Fts(IndexOptionsFts {}),
    };

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: Arc::new(primary_keys.into_iter().collect()),
            partition_key_count,
            columns,
            dimensions: HashMap::new(),
        },
    )
    .unwrap();

    db.add_index(index.clone(), fullscan_fn, cdc_fn).unwrap();

    let (receivers, senders) = create_config_channels(config).await;
    let index_factory = vector_store::new_index_factory_usearch(receivers.config.clone()).unwrap();

    let run = {
        let node_state = node_state.clone();
        async move {
            let (server, _mtls) =
                vector_store::run(node_state, db_actor, internals, index_factory, receivers)
                    .await
                    .unwrap();
            let addr = (*server.address().await.borrow()).unwrap();

            (HttpClient::new(addr), server, senders)
        }
    };

    (run, index, db, node_state)
}

#[tokio::test]
async fn fts_index_returns_proper_count() {
    crate::enable_tracing();

    let (run, index, _db, _node_state) = setup_fts_store(
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        Some(db_basic::scan_fn_documents([
            (
                [CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
                Some("hello world".to_string()),
                Timestamp::from_unix_timestamp(10),
            ),
            (
                [CqlValue::Int(2), CqlValue::Text("two".to_string())].into(),
                Some("foo bar".to_string()),
                Timestamp::from_unix_timestamp(20),
            ),
        ])),
        None,
    )
    .await;

    let (client, _server, _config_tx) = run.await;

    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| status.status == IndexStatus::Serving && status.count == 2)
        },
        "Waiting for FTS index to be serving with count == 2",
    )
    .await;
}
