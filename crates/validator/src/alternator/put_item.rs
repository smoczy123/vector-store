/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::alternator;
use crate::alternator::Item;
use crate::alternator::TableContext;
use crate::common;
use aws_sdk_dynamodb::types::AttributeValue;
use std::sync::Arc;
use tracing::info;

/// Inserts items via `PutItem` and verifies the VS index is updated.
///
/// Loops [`alternator::name_patterns`] so that every combination
/// of key schema (HASH-only / HASH+RANGE) and naming style (plain /
/// special) is covered.
#[e2etest::test(group = put_item)]
async fn put_item_updates_index(actors: Arc<TestActors>) {
    info!("started");

    for shape in &alternator::name_patterns() {
        info!("Testing shape: {shape:?}");

        let ctx = TableContext::create(&actors, shape).await;

        let vec_attr = ctx.shape.vec().expect("TableContext has no vec_attr");

        let a = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]);
        let b = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]);
        let c = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "c").vec(vec_attr, [1.0, 4.0, 8.0]);

        for item in [&a, &b, &c] {
            ctx.put(item).await;
        }
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[a, b.clone(), c.clone()])
            .await;

        // Replace item a with a vector pointing in the opposite direction -
        // a_replaced=[-1,-1,-1] has cosine=-1.0 (antipodal to [1,1,1]) so it falls
        // to last position.  b=[1,2,4] and c=[1,4,8] retain their order.
        let a_replaced =
            Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "a").vec(vec_attr, [-1.0, -1.0, -1.0]);
        ctx.put(&a_replaced).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[b, c, a_replaced]).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

/// Inserts items with various invalid vector attributes and verifies VS does
/// not index them.  Only the valid item should appear in the index.
#[e2etest::test(group = put_item)]
async fn put_item_with_invalid_vector_is_not_indexed(actors: Arc<TestActors>) {
    info!("started");

    let shape = &alternator::name_patterns()[0]; // plain names, HASH-only
    let pk = shape.pk();
    let vec_attr = shape.vec().expect("NAME_PATTERNS[0] always has vec");

    let ctx = TableContext::create(&actors, shape).await;

    let vec_with_string_elem = AttributeValue::L(vec![
        AttributeValue::N("1.0".into()),
        AttributeValue::N("2.0".into()),
        AttributeValue::S("3.0".into()),
    ]);
    let vec_with_null_elem = AttributeValue::L(vec![
        AttributeValue::N("1.0".into()),
        AttributeValue::N("2.0".into()),
        AttributeValue::Null(true),
    ]);

    let no_vec = Item::key(pk, None, "pk", "no-vec");
    ctx.put(&no_vec).await;

    let wrong_type_string = Item::key(pk, None, "pk", "wrong-type-string")
        .attr(vec_attr, AttributeValue::S("not-a-vector".into()));
    ctx.put_expecting_error(&wrong_type_string, "ValidationException")
        .await;

    let wrong_type_mostly_float_one_s = Item::key(pk, None, "pk", "wrong-type-mostly-float-one-s")
        .attr(vec_attr, vec_with_string_elem);
    ctx.put_expecting_error(&wrong_type_mostly_float_one_s, "ValidationException")
        .await;

    let wrong_type_mostly_float_one_null =
        Item::key(pk, None, "pk", "wrong-type-mostly-float-one-null")
            .attr(vec_attr, vec_with_null_elem);
    ctx.put_expecting_error(&wrong_type_mostly_float_one_null, "ValidationException")
        .await;

    let wrong_type_too_short = Item::key(pk, None, "pk", "wrong-type-too-short")
        .attr(vec_attr, alternator::float_list([1.0_f32, 1.0]));
    ctx.put_expecting_error(&wrong_type_too_short, "ValidationException")
        .await;

    let wrong_type_too_long = Item::key(pk, None, "pk", "wrong-type-too-long")
        .attr(vec_attr, alternator::float_list([1.0_f32, 1.0, 1.0, 1.0]));
    ctx.put_expecting_error(&wrong_type_too_long, "ValidationException")
        .await;

    // valid is put last so that wait_for_ann acts as a sequencing barrier:
    // when VS has indexed valid it must have already processed the no_vec CDC
    // event before it (CDC events are ordered), proving that the item without
    // a vector attribute was correctly ignored.
    let valid = Item::key(pk, None, "pk", "valid").vec(vec_attr, [1.0, 1.0, 1.0]);
    ctx.put(&valid).await;
    ctx.wait_for_ann([1.0, 1.0, 1.0], &[valid]).await;

    let valid_no_vec = Item::key(pk, None, "pk", "valid");
    ctx.put(&valid_no_vec).await;
    ctx.wait_for_count(0).await;

    let wrong_type2 =
        Item::key(pk, None, "pk", "valid").attr(vec_attr, AttributeValue::S("bad".into()));
    ctx.put_expecting_error(&wrong_type2, "ValidationException")
        .await;
    ctx.wait_for_count(0).await;

    ctx.done().await;
    info!("finished");
}

e2etest::group!(
    name = put_item,
    fixtures = (Fixture),
    parent = alternator::alternator
);

struct Fixture {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        setup.setup::<TestActors>().await;
        let actors = setup.get::<TestActors>().await.unwrap();
        alternator::init(&actors).await;
        Self { actors }
    }

    async fn teardown(self) {
        common::cleanup(&self.actors).await;
    }
}
