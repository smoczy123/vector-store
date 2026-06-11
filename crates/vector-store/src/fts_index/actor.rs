/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::AsyncInProgress;
use crate::IndexKey;
use crate::table::PrimaryId;
use crate::vs_index::actor::CountR;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Actor messages for the full-text-search index.
#[allow(dead_code)]
pub(crate) enum FtsIndex {
    AddDocument {
        primary_id: PrimaryId,
        document: String,
        in_progress: Option<AsyncInProgress>,
    },
    RemoveDocument {
        primary_id: PrimaryId,
        in_progress: Option<AsyncInProgress>,
    },
    Count {
        index_key: IndexKey,
        tx: oneshot::Sender<CountR>,
    },
}

pub(crate) trait FtsIndexExt {
    async fn add_document(
        &self,
        primary_id: PrimaryId,
        document: String,
        in_progress: Option<AsyncInProgress>,
    );
    async fn remove_document(&self, primary_id: PrimaryId, in_progress: Option<AsyncInProgress>);
    async fn count(&self, index_key: IndexKey) -> CountR;
}

impl FtsIndexExt for mpsc::Sender<FtsIndex> {
    async fn add_document(
        &self,
        primary_id: PrimaryId,
        document: String,
        in_progress: Option<AsyncInProgress>,
    ) {
        self.send(FtsIndex::AddDocument {
            primary_id,
            document,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn remove_document(&self, primary_id: PrimaryId, in_progress: Option<AsyncInProgress>) {
        self.send(FtsIndex::RemoveDocument {
            primary_id,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn count(&self, index_key: IndexKey) -> CountR {
        let (tx, rx) = oneshot::channel();
        self.send(FtsIndex::Count { index_key, tx }).await?;
        rx.await?
    }
}
