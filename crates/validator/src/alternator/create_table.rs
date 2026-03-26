/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::alternator;
use crate::common;
use async_backtrace::framed;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::AfterDeserializationInterceptorContextRef;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use e2etest::TestCase;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::info;

/// An SDK interceptor that captures the `VectorIndexes` extension field from
/// the raw `DescribeTable` JSON response as a [`serde_json::Value`].  The
/// caller retrieves it via [`into_captured`] after the SDK call completes and
/// performs the assertions explicitly.
///
/// Attach this to a `client.describe_table().customize().interceptor(...)` call.
/// It fires in [`read_after_deserialization`], at which point the response body
/// has already been buffered by the SDK (via its internal `read_body()` call
/// that precedes deserialization of non-streaming operations).
///
/// [`into_captured`]: VectorIndexesCaptureInterceptor::into_captured
/// [`read_after_deserialization`]: Intercept::read_after_deserialization
#[derive(Debug, Clone)]
struct VectorIndexesCaptureInterceptor {
    captured: Arc<Mutex<Option<serde_json::Value>>>,
}

impl VectorIndexesCaptureInterceptor {
    fn new() -> Self {
        Self {
            captured: Arc::new(Mutex::new(None)),
        }
    }

    /// Consume the interceptor and return the `VectorIndexes` JSON value
    /// captured during the last `DescribeTable` call.  Returns `None` if
    /// `read_after_deserialization` has not fired yet (i.e. the call has not
    /// completed or failed before the response body was available).
    fn into_captured(self) -> Option<serde_json::Value> {
        // `unwrap`: poisoning here means a panic already occurred in the
        // interceptor thread, which would surface as a test failure anyway.
        Arc::try_unwrap(self.captured)
            .expect("no other Arc clones should remain after the SDK call")
            .into_inner()
            .unwrap()
    }
}

impl Intercept for VectorIndexesCaptureInterceptor {
    fn name(&self) -> &'static str {
        "VectorIndexesCaptureInterceptor"
    }

    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let bytes = context
            .response()
            .body()
            .bytes()
            .ok_or("expected buffered response body for DescribeTable")?;

        let json: serde_json::Value = serde_json::from_slice(bytes)?;

        let table = json
            .get("Table")
            .ok_or("raw DescribeTable should contain 'Table' key")?;

        // `VectorIndexes` is our extension field - capture it as-is so the
        // test can assert on the full value.  Absence is stored as None and
        // surfaced by the test assertion rather than aborting the SDK call.
        *self.captured.lock().unwrap() = table.get("VectorIndexes").cloned();

        Ok(())
    }
}

/// Asserts that `actual` JSON contains all fields from `expected`, allowing
/// additional attributes in objects. Arrays must match in length and each
/// element is checked recursively. Scalar values must be exactly equal.
fn assert_json_includes(actual: &serde_json::Value, expected: &serde_json::Value, ctx: &str) {
    match (actual, expected) {
        (serde_json::Value::Object(a), serde_json::Value::Object(e)) => {
            for (k, v) in e {
                let a_v = a
                    .get(k)
                    .unwrap_or_else(|| panic!("{ctx}: missing key {k:?}"));
                assert_json_includes(a_v, v, &format!("{ctx}.{k}"));
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(e)) => {
            assert_eq!(a.len(), e.len(), "{ctx}: array length mismatch");
            for (i, (a_v, e_v)) in a.iter().zip(e.iter()).enumerate() {
                assert_json_includes(a_v, e_v, &format!("{ctx}[{i}]"));
            }
        }
        _ => assert_eq!(actual, expected, "{ctx}: value mismatch"),
    }
}

/// Creates, describes, and deletes Alternator tables for every entry in
/// [`alternator::name_patterns`] (the 2x2 matrix of plain/special x HASH-only/HASH+RANGE).
/// For each shape the test:
/// 1. Calls `CreateTable` with a vector index.
/// 2. Waits for Vector Store to discover the index.
/// 3. Calls `DescribeTable` and verifies the `VectorIndexes` extension field.
/// 4. Calls `DeleteTable` and waits for Vector Store to drop the index.
#[framed]
async fn create_describe_and_delete_table_with_vector_index(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = alternator::make_clients(&actors).await;

    let patterns = alternator::name_patterns();
    for (i, shape) in patterns.iter().enumerate() {
        info!("NAME_PATTERNS[{i}]: {shape:?}");

        let vec_attr = shape.vec().expect("NAME_PATTERNS entries always have vec");
        let (table_name, index) = alternator::resolve_table_names(shape);

        info!(
            "Creating Alternator table '{table_name}' with VectorIndex '{}'",
            index.index
        );
        alternator::create_table(
            &client,
            &table_name,
            shape.pk(),
            shape.pk_type.clone(),
            shape.sk(),
            &[(index.index.as_ref(), vec_attr, 3)],
        )
        .await
        .expect("CreateTable with VectorIndex should succeed");

        info!(
            "Waiting for Vector Store to discover index '{}/{}'",
            index.keyspace, index.index
        );
        alternator::wait_for_index(&vs_clients, &index).await;

        info!("Describing Alternator table '{table_name}' and asserting VectorIndexes");
        let interceptor = VectorIndexesCaptureInterceptor::new();
        client
            .describe_table()
            .table_name(&table_name)
            .customize()
            .interceptor(interceptor.clone())
            .send()
            .await
            .expect("DescribeTable should succeed");

        let ctx = format!("NAME_PATTERNS[{i}]");
        let captured = interceptor
            .into_captured()
            .expect("VectorIndexesCaptureInterceptor should have fired");
        assert_json_includes(
            &captured,
            &serde_json::json!([{
                "IndexName": index.index.as_ref(),
                "VectorAttribute": { "AttributeName": vec_attr, "Dimensions": 3 },
                "IndexStatus": "ACTIVE",
                "Projection": { "ProjectionType": "KEYS_ONLY" }
            }]),
            &ctx,
        );

        info!("Deleting Alternator table '{table_name}'");
        alternator::delete_table(&client, &table_name).await;

        info!(
            "Waiting for Vector Store to drop index '{}/{}'",
            index.keyspace, index.index
        );
        alternator::wait_for_no_index(&vs_clients, &index).await;

        info!("NAME_PATTERNS[{i}] passed");
    }

    info!("finished");
}

pub(super) async fn new() -> TestCase<TestActors> {
    TestCase::empty()
        .with_init(common::DEFAULT_TEST_TIMEOUT, alternator::init)
        .with_cleanup(common::DEFAULT_TEST_TIMEOUT, common::cleanup)
        .with_test(
            "create_describe_and_delete_table_with_vector_index",
            common::DEFAULT_TEST_TIMEOUT,
            create_describe_and_delete_table_with_vector_index,
        )
}
