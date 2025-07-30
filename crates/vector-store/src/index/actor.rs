/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Distance;
use crate::Limit;
use crate::PrimaryKey;
use crate::Vector;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) type AnnR = anyhow::Result<(Vec<PrimaryKey>, Vec<Distance>)>;
pub(crate) type CountR = anyhow::Result<usize>;

pub enum Index {
    AddOrReplace {
        primary_key: PrimaryKey,
        embedding: Vector,
    },
    Remove {
        primary_key: PrimaryKey,
    },
    Ann {
        embedding: Vector,
        limit: Limit,
        tx: oneshot::Sender<AnnR>,
    },
    Count {
        tx: oneshot::Sender<CountR>,
    },
}

pub(crate) trait IndexExt {
    async fn add_or_replace(&self, primary_key: PrimaryKey, embedding: Vector);
    async fn remove(&self, primary_key: PrimaryKey);
    async fn ann(&self, embedding: Vector, limit: Limit) -> AnnR;
    async fn count(&self) -> CountR;
}

impl IndexExt for mpsc::Sender<Index> {
    async fn add_or_replace(&self, primary_key: PrimaryKey, embedding: Vector) {
        self.send(Index::AddOrReplace {
            primary_key,
            embedding,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn remove(&self, primary_key: PrimaryKey) {
        self.send(Index::Remove { primary_key })
            .await
            .expect("internal actor should receive request");
    }

    async fn ann(&self, embedding: Vector, limit: Limit) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Ann {
            embedding,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn count(&self) -> CountR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Count { tx }).await?;
        rx.await?
    }
}
