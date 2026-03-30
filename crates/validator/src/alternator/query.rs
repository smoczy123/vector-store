/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_sdk_dynamodb::client::customize::CustomizableOperation;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::query::QueryOutput;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use e2etest::TestCase;
use tracing::info;

use crate::alternator;
use crate::alternator::Item;
use crate::alternator::JsonBodyInjectInterceptor;
use crate::alternator::TableContext;
use crate::alternator::TableShape;
use aws_sdk_dynamodb::types::ScalarAttributeType;

/// Extension trait that adds Alternator `VectorSearch` to [`QueryFluentBuilder`].
pub(super) trait QueryBuilderExt {
    fn vector_search(
        self,
        vector: impl IntoIterator<Item = f32>,
    ) -> CustomizableOperation<QueryOutput, QueryError, QueryFluentBuilder>;
}

impl QueryBuilderExt for QueryFluentBuilder {
    fn vector_search(
        self,
        vector: impl IntoIterator<Item = f32>,
    ) -> CustomizableOperation<QueryOutput, QueryError, QueryFluentBuilder> {
        let json = serde_json::json!({
            "QueryVector": {
                "L": vector
                    .into_iter()
                    .map(|v| serde_json::json!({ "N": v.to_string() }))
                    .collect::<Vec<_>>()
            }
        });
        self.customize()
            .interceptor(JsonBodyInjectInterceptor::new([("VectorSearch", json)]))
    }
}

/// Verifies basic VectorSearch query: results returned, limit respected, nearest item first.
#[framed]
async fn query_with_vector_search(actors: TestActors) {
    info!("started");

    let shapes = [
        TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "Pk-QVS".into(),
            sk_name: None,
            vec_name: Some("Vec-QVS".into()),
            pk_type: ScalarAttributeType::S,
        },
        TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "Pk-QVS".into(),
            sk_name: Some("Sk-QVS".into()),
            vec_name: Some("Vec-QVS".into()),
            pk_type: ScalarAttributeType::S,
        },
    ];

    for shape in &shapes {
        info!("Testing shape: {shape:?}");

        let dataset = [
            Item::new(shape.pk(), AttributeValue::S("pk-a".into()))
                .maybe_sk(shape.sk(), AttributeValue::S("sk-1".into()))
                .vec(shape.vec().unwrap(), [1.0, 1.0, 1.0]),
            Item::new(shape.pk(), AttributeValue::S("pk-b".into()))
                .maybe_sk(shape.sk(), AttributeValue::S("sk-2".into()))
                .vec(shape.vec().unwrap(), [1.0, 2.0, 4.0]),
            Item::new(shape.pk(), AttributeValue::S("pk-c".into()))
                .maybe_sk(shape.sk(), AttributeValue::S("sk-3".into()))
                .vec(shape.vec().unwrap(), [4.0, 1.0, 2.0]),
        ];

        let ctx = TableContext::create_with_data(&actors, shape, &dataset).await;

        info!(
            "Issuing Query with VectorSearch Limit=2 on '{}'",
            ctx.table_name
        );
        let items = ctx
            .client
            .query()
            .table_name(&ctx.table_name)
            .index_name(ctx.index.index.as_ref())
            .limit(2)
            .vector_search([1.0, 1.0, 1.0])
            .send()
            .await
            .expect("Query with VectorSearch should succeed")
            .items()
            .to_vec();

        assert!(
            !items.is_empty(),
            "Query with VectorSearch should return at least one item"
        );
        assert!(
            items.len() <= 2,
            "Query with VectorSearch Limit=2 should return at most 2 items, got {}",
            items.len()
        );
        assert_eq!(
            items[0].get(shape.pk()),
            Some(&AttributeValue::S("pk-a".into())),
            "closest result should be pk-a"
        );
        if let Some(sk_name) = shape.sk() {
            assert_eq!(
                items[0].get(sk_name),
                Some(&AttributeValue::S("sk-1".into())),
                "closest result should have sk-1"
            );
        }

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

pub(super) async fn new() -> TestCase<TestActors> {
    TestCase::empty()
        .with_init(common::DEFAULT_TEST_TIMEOUT, alternator::init)
        .with_cleanup(common::DEFAULT_TEST_TIMEOUT, common::cleanup)
        .with_test(
            "query_with_vector_search",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_vector_search,
        )
}
