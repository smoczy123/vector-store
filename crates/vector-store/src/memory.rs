/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::time::Duration;
use sysinfo::System;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;

const MEMORY_ALLOCATION_THRESHOLD: f64 = 0.05;

#[derive(PartialEq, Clone)]
pub enum Allocate {
    CanAllocate,
    CannotAllocate,
}

pub(crate) type AllocateR = anyhow::Result<Allocate>;

pub enum Memory {
    CanAllocate { tx: oneshot::Sender<AllocateR> },
}

pub(crate) trait MemoryExt {
    async fn can_allocate(&self) -> AllocateR;
}

impl MemoryExt for mpsc::Sender<Memory> {
    async fn can_allocate(&self) -> AllocateR {
        let (tx, rx) = oneshot::channel();
        self.send(Memory::CanAllocate { tx }).await?;
        rx.await?
    }
}

pub(crate) fn new() -> mpsc::Sender<Memory> {
    const MEMORY_INFO_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(async move {
        debug!("starting");

        let mut system_info = System::new_all();
        let mut interval = tokio::time::interval(MEMORY_INFO_REFRESH_INTERVAL);
        let mut allocate = Allocate::CanAllocate;

        loop {
            select! {
                _ = interval.tick() => {
                    system_info.refresh_memory();
                    allocate = can_allocate(&system_info)
                }

                Some(msg) =  rx.recv() => {
                    match msg {
                        Memory::CanAllocate { tx } => {
                            tx.send(Ok(allocate.clone())).unwrap_or_else(|_| trace!("can_allocate: unable to send response"));
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }

        debug!("finished");
    }.instrument(debug_span!("memory")));

    tx
}

fn can_allocate(system_info: &System) -> Allocate {
    let cgroup = system_info.cgroup_limits();
    let (available_memory, total_memory) = if let Some(cgroup) = cgroup {
        (cgroup.free_memory, cgroup.total_memory)
    } else {
        (system_info.available_memory(), system_info.total_memory())
    };

    if available_memory >= (MEMORY_ALLOCATION_THRESHOLD * total_memory as f64) as u64 {
        Allocate::CanAllocate
    } else {
        Allocate::CannotAllocate
    }
}
