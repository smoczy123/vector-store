/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn fullscan_add(c: &mut Criterion) {}

fn search(c: &mut Criterion) {}

criterion_group!(benches, fullscan_add, search);
criterion_main!(benches);
