/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod fbin;
mod parquet;

use futures::stream::BoxStream;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tracing::info;

const DATASET_FILENAME: &str = "dataset.toml";

pub(crate) struct Query {
    pub(crate) query: Vec<f32>,
    pub(crate) neighbors: HashSet<i64>,
}

pub(crate) struct Data {
    path: Arc<PathBuf>,
    format: Format,
}

enum Format {
    Parquet(Arc<parquet::Config>),
    Fbin(Arc<fbin::Config>),
}

impl Data {
    pub(crate) async fn dimension(&self) -> usize {
        let dim = match &self.format {
            Format::Parquet(config) => {
                parquet::dimension(Arc::clone(&self.path), Arc::clone(config)).await
            }
            Format::Fbin(config) => {
                fbin::dimension(Arc::clone(&self.path), Arc::clone(config)).await
            }
        };
        info!("Found dimension {dim} for dataset at {:?}", self.path);
        dim
    }

    pub(crate) async fn queries(&self, limit: usize) -> Vec<Query> {
        match &self.format {
            Format::Parquet(config) => {
                parquet::queries(Arc::clone(&self.path), Arc::clone(config), limit).await
            }
            Format::Fbin(config) => {
                fbin::queries(Arc::clone(&self.path), Arc::clone(config), limit).await
            }
        }
    }

    pub(crate) async fn vector_stream(&self) -> BoxStream<'static, (i64, Vec<f32>)> {
        match &self.format {
            Format::Parquet(config) => {
                parquet::vector_stream(Arc::clone(&self.path), Arc::clone(config)).await
            }
            Format::Fbin(config) => {
                fbin::vector_stream(Arc::clone(&self.path), Arc::clone(config)).await
            }
        }
    }
}

#[derive(Deserialize)]
struct Config {
    parquet: Option<parquet::Config>,
    fbin: Option<fbin::Config>,
}

pub(crate) async fn new(path: PathBuf) -> Data {
    let toml_path = path.join(DATASET_FILENAME);
    let Ok(config) = fs::read(&toml_path).await else {
        info!("Not found {DATASET_FILENAME} in {path:?}. Using default parquet format.");
        return Data {
            path: Arc::new(path),
            format: Format::Parquet(Arc::new(parquet::Config::default())),
        };
    };
    let config: Config = toml::from_slice(&config)
        .unwrap_or_else(|err| panic!("Failed to parse {toml_path:?}: {err}"));
    if let Some(config) = config.parquet {
        return Data {
            path: Arc::new(path),
            format: Format::Parquet(Arc::new(config)),
        };
    }
    if let Some(config) = config.fbin {
        return Data {
            path: Arc::new(path),
            format: Format::Fbin(Arc::new(config)),
        };
    }
    info!("Not found format type in {DATASET_FILENAME} in {path:?}. Using default parquet format.");
    Data {
        path: Arc::new(path),
        format: Format::Parquet(Arc::new(parquet::Config::default())),
    }
}
