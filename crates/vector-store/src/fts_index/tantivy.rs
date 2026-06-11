/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::IndexKey;
use crate::fts_index::actor::FtsIndex;
use crate::fts_index::factory::FtsIndexFactory;
use crate::perf;
use tokio::sync::mpsc;
use tracing::debug;

pub(crate) struct TantivyIndexFactory;

impl FtsIndexFactory for TantivyIndexFactory {
    fn create_index(&self, key: IndexKey) -> mpsc::Sender<FtsIndex> {
        let (tx, mut rx) = mpsc::channel::<FtsIndex>(perf::channel_size().into());
        tokio::spawn(async move {
            debug!("fts index actor started for {key}");
            while let Some(_msg) = rx.recv().await {
                debug!("fts index actor received message for {key}");
            }
            debug!("fts index actor finished for {key}");
        });
        tx
    }
}
