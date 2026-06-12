/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Timestamp;
use crate::table::ETValue;
use crate::table::Idx;
use crate::table::PrimaryId;
use anyhow::anyhow;

/// ColumnVec is a wrapper around Vec and generic index type. It is used to safely access columns by specific index types.
#[derive(Debug)]
pub(super) struct ColumnVec<I, T> {
    vec: Vec<T>,
    _index: std::marker::PhantomData<I>,
}

impl<I: Idx, T> ColumnVec<I, T> {
    pub(super) fn new() -> Self {
        Self {
            vec: Vec::new(),
            _index: std::marker::PhantomData,
        }
    }

    pub(super) fn resize_with(&mut self, size: usize, f: impl FnMut() -> T) {
        self.vec.resize_with(size, f);
    }

    pub(super) fn get(&self, idx: I) -> Option<&T> {
        self.vec.get(idx.idx())
    }

    pub(super) fn get_mut(&mut self, idx: I) -> Option<&mut T> {
        self.vec.get_mut(idx.idx())
    }

    pub(super) fn update(&mut self, idx: I, value: T) -> anyhow::Result<()> {
        *self
            .get_mut(idx)
            .ok_or_else(|| anyhow!("Index out of ColumnVec bounds"))? = value;
        Ok(())
    }
}

impl<T> ColumnVec<PrimaryId, ETValue<T>> {
    pub(super) fn update_epoch_timestamp(
        &mut self,
        primary_id: PrimaryId,
        timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        self.get_mut(primary_id)
            .map(|value| {
                value.update_epoch_timestamp(primary_id.epoch(), timestamp);
            })
            .ok_or_else(|| anyhow!("Index out of ColumnVec bounds"))
    }
}
