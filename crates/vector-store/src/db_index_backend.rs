/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Dimensions;
use crate::IndexMetadata;
use crate::IndexName;
use crate::KeyspaceName;
use crate::TableName;
use crate::Vector;
use crate::vector;
use futures::TryStreamExt;
use regex::Regex;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

pub(crate) struct IndexLocation {
    pub keyspace: KeyspaceName,
    pub table: TableName,
    pub index: IndexName,
}

pub(crate) enum DbIndexBackend {
    Cql { target_column: ColumnName },
    Alternator { target_column: ColumnName },
}

impl From<&IndexMetadata> for DbIndexBackend {
    fn from(metadata: &IndexMetadata) -> Self {
        let target_column = metadata.target_column.clone();
        if metadata.keyspace_name.is_alternator() {
            Self::Alternator { target_column }
        } else {
            Self::Cql { target_column }
        }
    }
}

impl DbIndexBackend {
    pub fn vector_column_name(&self) -> &str {
        match self {
            Self::Cql { target_column } => target_column.as_ref(),
            Self::Alternator { .. } => ":attrs",
        }
    }

    pub fn extract_vector(&self, value: CqlValue) -> anyhow::Result<Option<Vector>> {
        match self {
            Self::Cql { .. } => Vector::try_from(value).map(Some),
            Self::Alternator { target_column } => vector::AlternatorAttrs {
                attrs: value,
                target_column: target_column.as_ref(),
            }
            .try_into(),
        }
    }
}

/// Builds the CQL range scan query appropriate for the given keyspace.
///
/// For CQL-native tables, selects the vector column directly.
/// For Alternator tables, selects from the `:attrs` map column.
pub(crate) fn range_scan_query(
    keyspace: &KeyspaceName,
    table: &TableName,
    target_column: &ColumnName,
    primary_key_list: &str,
    partition_key_list: &str,
) -> String {
    if keyspace.is_alternator() {
        let vector = target_column.as_ref();
        format!(
            "
            SELECT {primary_key_list}, \":attrs\"['{vector}'], writetime(\":attrs\"['{vector}'])
            FROM {keyspace}.{table}
            WHERE
                token({partition_key_list}) >= ?
                AND token({partition_key_list}) <= ?
            BYPASS CACHE
            "
        )
    } else {
        let vector = target_column.as_ref();
        format!(
            "
            SELECT {primary_key_list}, {vector}, writetime({vector})
            FROM {keyspace}.{table}
            WHERE
                token({partition_key_list}) >= ?
                AND token({partition_key_list}) <= ?
            BYPASS CACHE
            "
        )
    }
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
