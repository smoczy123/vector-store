/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::IndexKey;
use crate::fts_index::actor::FtsIndex;
use tokio::sync::mpsc;

pub(crate) trait FtsIndexFactory {
    fn create_index(&self, key: IndexKey) -> mpsc::Sender<FtsIndex>;
}
