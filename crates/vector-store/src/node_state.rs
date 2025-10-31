/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexMetadata;
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    Initializing,
    ConnectingToDb,
    DiscoveringIndexes,
    IndexingEmbeddings,
    Serving,
}

pub enum Event {
    ConnectingToDb,
    ConnectedToDb,
    DiscoveringIndexes,
    IndexesDiscovered(HashSet<IndexMetadata>),
    FullScanFinished(IndexMetadata),
}

pub enum NodeState {
    SendEvent(Event),
    GetStatus(oneshot::Sender<NodeStatus>),
}

pub(crate) trait NodeStateExt {
    async fn send_event(&self, event: Event);
    async fn get_status(&self) -> NodeStatus;
}

impl NodeStateExt for mpsc::Sender<NodeState> {
    async fn send_event(&self, event: Event) {
        let msg = NodeState::SendEvent(event);
        self.send(msg)
            .await
            .expect("NodeStateExt::send_event: internal actor should receive event");
    }

    async fn get_status(&self) -> NodeStatus {
        let (tx, rx) = oneshot::channel();
        self.send(NodeState::GetStatus(tx))
            .await
            .expect("NodeStateExt::get_status: internal actor should receive request");
        rx.await
            .expect("NodeStateExt::get_status: failed to receive status")
    }
}

pub(crate) async fn new() -> mpsc::Sender<NodeState> {
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        async move {
            debug!("starting");

            let mut status = NodeStatus::Initializing;
            let mut idxs = HashSet::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    NodeState::SendEvent(event) => match event {
                        Event::ConnectingToDb => {
                            status = NodeStatus::ConnectingToDb;
                        }
                        Event::ConnectedToDb => {}
                        Event::DiscoveringIndexes => {
                            if status != NodeStatus::Serving {
                                status = NodeStatus::DiscoveringIndexes;
                            }
                        }
                        Event::IndexesDiscovered(indexes) => {
                            if indexes.is_empty() && status != NodeStatus::Serving {
                                status = NodeStatus::Serving;
                                info!("Service is running, no indexes to build");
                                continue;
                            }
                            if status == NodeStatus::DiscoveringIndexes {
                                status = NodeStatus::IndexingEmbeddings;
                                idxs = indexes;
                            }
                        }
                        Event::FullScanFinished(metadata) => {
                            idxs.remove(&metadata);
                            if idxs.is_empty() && status != NodeStatus::Serving {
                                status = NodeStatus::Serving;
                                info!("Service is running, finished building indexes");
                            }
                        }
                    },
                    NodeState::GetStatus(tx) => {
                        tx.send(status).unwrap_or_else(|_| {
                            tracing::debug!("Failed to send current state");
                        });
                    }
                }
            }
            debug!("finished");
        }
        .instrument(debug_span!("node_state")),
    );

    tx
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use uuid::Uuid;

    use super::*;
    use crate::ColumnName;
    use crate::Dimensions;
    use crate::IndexName;
    use crate::KeyspaceName;
    use crate::TableName;

    #[tokio::test]
    async fn test_node_state_changes_as_expected() {
        let node_state = new().await;
        let mut status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Initializing);
        node_state.send_event(Event::ConnectingToDb).await;
        status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::ConnectingToDb);
        node_state.send_event(Event::ConnectedToDb).await;
        node_state.send_event(Event::DiscoveringIndexes).await;
        status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::DiscoveringIndexes);
        let idx1 = IndexMetadata {
            keyspace_name: KeyspaceName("test_keyspace".to_string()),
            index_name: IndexName("test_index".to_string()),
            table_name: TableName("test_table".to_string()),
            target_column: ColumnName("test_column".to_string()),
            dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
            connectivity: Default::default(),
            expansion_add: Default::default(),
            expansion_search: Default::default(),
            space_type: Default::default(),
            version: Uuid::new_v4().into(),
        };
        let idx2 = IndexMetadata {
            keyspace_name: KeyspaceName("test_keyspace".to_string()),
            index_name: IndexName("test_index1".to_string()),
            table_name: TableName("test_table".to_string()),
            target_column: ColumnName("test_column".to_string()),
            dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
            connectivity: Default::default(),
            expansion_add: Default::default(),
            expansion_search: Default::default(),
            space_type: Default::default(),
            version: Uuid::new_v4().into(),
        };
        let idxs = HashSet::from([idx1.clone(), idx2.clone()]);
        node_state.send_event(Event::IndexesDiscovered(idxs)).await;
        status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);
        node_state.send_event(Event::FullScanFinished(idx1)).await;
        status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);
        node_state.send_event(Event::FullScanFinished(idx2)).await;
        status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);
    }

    #[tokio::test]
    async fn no_indexes_discovered() {
        let node_state = new().await;

        assert_eq!(node_state.get_status().await, NodeStatus::Initializing);

        node_state.send_event(Event::ConnectingToDb).await;
        assert_eq!(node_state.get_status().await, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::DiscoveringIndexes).await;
        assert_eq!(
            node_state.get_status().await,
            NodeStatus::DiscoveringIndexes
        );

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::new()))
            .await;
        assert_eq!(node_state.get_status().await, NodeStatus::Serving);
    }

    #[tokio::test]
    async fn status_remains_serving_when_discovering_indexes() {
        let node_state = new().await;
        // Move to Serving status
        node_state.send_event(Event::ConnectingToDb).await;
        node_state.send_event(Event::DiscoveringIndexes).await;
        node_state
            .send_event(Event::IndexesDiscovered(HashSet::new()))
            .await;
        assert_eq!(node_state.get_status().await, NodeStatus::Serving);

        // Try to trigger DiscoveringIndexes again
        node_state.send_event(Event::DiscoveringIndexes).await;
        // Status should remain Serving
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);

        let idx = IndexMetadata {
            keyspace_name: KeyspaceName("test_keyspace".to_string()),
            index_name: IndexName("test_index".to_string()),
            table_name: TableName("test_table".to_string()),
            target_column: ColumnName("test_column".to_string()),
            dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
            connectivity: Default::default(),
            expansion_add: Default::default(),
            expansion_search: Default::default(),
            space_type: Default::default(),
            version: Uuid::new_v4().into(),
        };

        // Simulate discovering an index
        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([idx])))
            .await;
        // Status should remain Serving
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);
    }
}
