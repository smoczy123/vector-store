/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Dimensions;
use crate::IndexName;
use crate::KeyspaceName;
use crate::TableName;
use futures::TryStreamExt;
use regex::Regex;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

pub(crate) struct IndexLocation {
    pub keyspace: KeyspaceName,
    pub table: TableName,
    pub index: IndexName,
}

/// Retrieves the vector dimensions for the given index, dispatching to the
/// appropriate strategy based on whether the keyspace is Alternator- or CQL-backed.
pub(crate) async fn get_dimensions(
    target_column: &ColumnName,
    session: &Session,
    st_get_index_target_type: &PreparedStatement,
    re_get_index_target_type: &Regex,
    st_get_index_options: &PreparedStatement,
    location: IndexLocation,
) -> anyhow::Result<Option<Dimensions>> {
    if location.keyspace.is_alternator() {
        get_dimensions_from_index_options(session, st_get_index_options, location).await
    } else {
        get_dimensions_from_column_type(
            target_column,
            session,
            st_get_index_target_type,
            re_get_index_target_type,
            location,
        )
        .await
    }
}

/// Retrieves the vector dimensions for a CQL-native table by parsing the column type.
async fn get_dimensions_from_column_type(
    target_column: &ColumnName,
    session: &Session,
    st_get_index_target_type: &PreparedStatement,
    re_get_index_target_type: &Regex,
    location: IndexLocation,
) -> anyhow::Result<Option<Dimensions>> {
    let column_type = session
        .execute_iter(
            st_get_index_target_type.clone(),
            (location.keyspace, location.table, target_column.clone()),
        )
        .await?
        .rows_stream::<(String,)>()?
        .try_next()
        .await?;
    let dimensions = column_type
        .and_then(|(typ,)| {
            re_get_index_target_type
                .captures(&typ)
                .and_then(|captures| captures["dimensions"].parse::<usize>().ok())
        })
        .and_then(|dimensions| NonZeroUsize::new(dimensions).map(|dimensions| dimensions.into()));
    Ok(dimensions)
}

/// Retrieves the vector dimensions for an Alternator table from the index options.
///
/// In Alternator, the schema has no native `VECTOR` type, so the dimension
/// is stored in the index option `"dimensions"`.
async fn get_dimensions_from_index_options(
    session: &Session,
    st_get_index_options: &PreparedStatement,
    location: IndexLocation,
) -> anyhow::Result<Option<Dimensions>> {
    let index_options = session
        .execute_iter(
            st_get_index_options.clone(),
            (location.keyspace, location.table, location.index),
        )
        .await?
        .rows_stream::<(BTreeMap<String, String>,)>()?
        .try_next()
        .await?;
    let dimensions = index_options
        .and_then(|(mut options,)| {
            options
                .remove("dimensions")
                .and_then(|s| s.parse::<usize>().ok())
        })
        .and_then(|dimensions| NonZeroUsize::new(dimensions).map(|dimensions| dimensions.into()));
    Ok(dimensions)
}
