/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
use dashmap::DashSet;
use prometheus::CounterVec;
use prometheus::GaugeVec;
use prometheus::HistogramVec;
use prometheus::Registry;
use std::sync::Arc;

pub const OP_INSERT: &str = "insert";
pub const OP_UPDATE: &str = "update";
pub const OP_REMOVE: &str = "remove";
pub const OPERATIONS: &[&str] = &[OP_INSERT, OP_UPDATE, OP_REMOVE];

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub latency: HistogramVec,
    pub size: GaugeVec,
    pub modified: CounterVec,
    dirty_indexes: Arc<DashSet<(String, String)>>,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        // Custom buckets: 0.1ms to 10s
        let buckets = vec![
            0.0001, // 0.1 ms
            0.0002, // 0.2 ms
            0.0005, // 0.5 ms
            0.001,  // 1 ms
            0.002,  // 2 ms
            0.005,  // 5 ms
            0.01,   // 10 ms
            0.02,   // 20 ms
            0.05,   // 50 ms
            0.1,    // 0.1 second
            0.2,    // 0.2 seconds
            0.5,    // 0.5 seconds
            1.0,    // 1 seconds
            2.0,    // 2 seconds
            5.0,    // 5 seconds
            10.0,   // 10 seconds
        ];

        let latency = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "request_latency_seconds",
                "Latency per index (seconds)",
            )
            .buckets(buckets),
            &["keyspace", "index_name"],
        )
        .unwrap();

        let size = GaugeVec::new(
            prometheus::Opts::new("index_size", "Number of Vector per index"),
            &["keyspace", "index_name"],
        )
        .unwrap();

        let modified: CounterVec = CounterVec::new(
            prometheus::Opts::new("index_modified", "Number of modified items per index"),
            &["keyspace", "index_name", "operation"],
        )
        .unwrap();

        registry.register(Box::new(latency.clone())).unwrap();
        registry.register(Box::new(size.clone())).unwrap();
        registry.register(Box::new(modified.clone())).unwrap();

        Self {
            registry,
            latency,
            size,
            modified,
            dirty_indexes: Arc::new(DashSet::new()),
        }
    }
    pub fn mark_dirty(&self, keyspace: &str, index_name: &str) {
        self.dirty_indexes
            .insert((keyspace.to_owned(), index_name.to_owned()));
    }
    pub fn take_dirty_indexes(&self) -> Vec<(String, String)> {
        // Collect, then remove.
        let keys: Vec<_> = self
            .dirty_indexes
            .iter()
            .map(|entry| entry.clone())
            .collect();
        for k in &keys {
            self.dirty_indexes.remove(k);
        }
        keys
    }

    pub fn remove_index_labels(&self, keyspace: &str, index_name: &str) {
        let _ = self.latency.remove_label_values(&[keyspace, index_name]);
        let _ = self.size.remove_label_values(&[keyspace, index_name]);
        for op in OPERATIONS {
            let _ = self
                .modified
                .remove_label_values(&[keyspace, index_name, op]);
        }
        self.dirty_indexes
            .remove(&(keyspace.to_owned(), index_name.to_owned()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Encoder;
    use prometheus::TextEncoder;

    fn metric_families_text(metrics: &Metrics) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&metrics.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn remove_index_labels_clears_all_metric_series_for_index() {
        let metrics = Metrics::new();

        metrics.size.with_label_values(&["ks", "idx"]).set(10.0);
        metrics
            .modified
            .with_label_values(&["ks", "idx", OP_INSERT])
            .inc();
        metrics
            .latency
            .with_label_values(&["ks", "idx"])
            .observe(0.001);

        metrics.remove_index_labels("ks", "idx");

        let output = metric_families_text(&metrics);
        assert!(
            !output.contains(r#"keyspace="ks""#),
            "metric output should not contain labels for deleted index, got:\n{output}"
        );
    }

    #[test]
    fn remove_index_labels_is_noop_when_index_has_no_metrics() {
        let metrics = Metrics::new();
        metrics.remove_index_labels("ks", "nonexistent");
        assert!(metric_families_text(&metrics).is_empty());
    }

    #[test]
    fn remove_index_labels_only_removes_target_index() {
        let metrics = Metrics::new();

        metrics.size.with_label_values(&["ks", "idx1"]).set(1.0);
        metrics.size.with_label_values(&["ks", "idx2"]).set(2.0);

        metrics.remove_index_labels("ks", "idx1");

        let output = metric_families_text(&metrics);
        assert!(
            !output.contains(r#"index_name="idx1""#),
            "idx1 labels should be removed"
        );
        assert!(
            output.contains(r#"index_name="idx2""#),
            "idx2 labels should remain"
        );
    }

    #[test]
    fn remove_index_labels_clears_dirty_index_entry() {
        let metrics = Metrics::new();

        metrics.mark_dirty("ks", "idx");
        assert_eq!(metrics.dirty_indexes.len(), 1);

        metrics.remove_index_labels("ks", "idx");
        assert!(metrics.dirty_indexes.is_empty());
    }
}
