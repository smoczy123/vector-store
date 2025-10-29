/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod parquet;

use futures::stream::BoxStream;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

pub(crate) struct Query {
    pub(crate) query: Vec<f32>,
    pub(crate) neighbors: HashSet<i64>,
}

pub(crate) struct Data {
    path: Arc<PathBuf>,
    format: Format,
}

enum Format {
    Parquet,
}

impl Data {
    pub(crate) async fn dimension(&self) -> usize {
        let dim = match &self.format {
            Format::Parquet => parquet::dimension(Arc::clone(&self.path)).await,
        };
        info!("Found dimension {dim} for dataset at {:?}", self.path);
        dim
    }

    pub(crate) async fn queries(&self, limit: usize) -> Vec<Query> {
        match &self.format {
            Format::Parquet => parquet::queries(Arc::clone(&self.path), limit).await,
        }
    }

    pub(crate) async fn vector_stream(&self) -> BoxStream<'static, (i64, Vec<f32>)> {
        match &self.format {
            Format::Parquet => parquet::vector_stream(Arc::clone(&self.path)).await,
        }
    }
}

pub(crate) fn new(path: PathBuf) -> Data {
    Data {
        path: Arc::new(path),
        format: Format::Parquet,
    }
}
