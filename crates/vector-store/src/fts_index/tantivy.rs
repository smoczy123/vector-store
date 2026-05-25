/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::IndexKey;
use crate::fts_index::actor::FtsIndex;
use crate::fts_index::factory::FtsIndexFactory;
use crate::perf;
use std::collections::HashSet;
use tokio::sync::mpsc;
use tracing::debug;

pub(crate) struct TantivyIndexFactory;

impl FtsIndexFactory for TantivyIndexFactory {
    fn create_index(&self, key: IndexKey) -> mpsc::Sender<FtsIndex> {
        let (tx, mut rx) = mpsc::channel::<FtsIndex>(perf::channel_size().into());
        tokio::spawn(async move {
            debug!("fts index actor started for {key}");
            let mut documents: HashSet<crate::table::PrimaryId> = HashSet::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    FtsIndex::AddDocument {
                        primary_id,
                        in_progress,
                        ..
                    } => {
                        documents.insert(primary_id);
                        drop(in_progress);
                    }
                    FtsIndex::RemoveDocument {
                        primary_id,
                        in_progress,
                        ..
                    } => {
                        documents.remove(&primary_id);
                        drop(in_progress);
                    }
                    FtsIndex::Count { tx, .. } => {
                        _ = tx.send(Ok(documents.len()));
                    }
                }
            }
            debug!("fts index actor finished for {key}");
        });
        tx
    }
}
