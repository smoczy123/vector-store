/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::ColumnName;
use crate::Config;
use crate::DbEmbedding;
use crate::IndexMetadata;
use crate::internals::Internals;
use crate::internals::InternalsExt;
use ::time::Date;
use ::time::Month;
use ::time::OffsetDateTime;
use ::time::PrimitiveDateTime;
use ::time::Time;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::value::CqlValue;
use scylla_cdc::consumer::CDCRow;
use scylla_cdc::consumer::Consumer;
use scylla_cdc::consumer::ConsumerFactory;
use scylla_cdc::log_reader::CDCLogReaderBuilder;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tap::Pipe;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::error;
use tracing::error_span;
use tracing::info;
use tracing::warn;

const CHECKPOINT_TIMESTAMP_OFFSET: Duration = Duration::from_mins(10);

pub(crate) enum DbCdc {}

/// Spawns a CDC actor that watches for session changes and manages a CDC reader.
pub(crate) fn new(
    config_rx: watch::Receiver<Arc<Config>>,
    mut session_rx: watch::Receiver<Option<Arc<Session>>>,
    metadata: IndexMetadata,
    internals: Sender<Internals>,
    tx_embeddings: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
) -> mpsc::Sender<DbCdc> {
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    let (tx, mut rx) = mpsc::channel::<DbCdc>(10);

    // Mark the receiver to ensure first session update is visible
    session_rx.mark_changed();

    let mut reader = CdcReaderState::new();
    let actor_key = metadata.key();
    tokio::spawn(
        async move {
            debug!("starting");

            loop {
                tokio::select! {
                    // Shut down when all senders are dropped
                    _ = rx.recv() => { break; }

                    // Wait for session changes
                    result = session_rx.changed() => {
                        if result.is_err() {
                            break;
                        }

                        let session_opt = session_rx.borrow_and_update().clone();
                        reader.handle_session_change(
                            session_opt, &config_rx, &metadata,
                            &tx_embeddings, &internals,
                        ).await;
                    }

                    _ = reader.error_notify.notified() => {
                        break;
                    }
                }
            }

            // Cleanup
            reader.stop().await;

            debug!("finished");
        }
        .instrument(error_span!("db_cdc", "{}", actor_key)),
    );

    tx
}

/// State for managing a CDC reader's lifecycle.
struct CdcReaderState {
    reader: Option<scylla_cdc::log_reader::CDCLogReader>,
    handler_task: Option<tokio::task::JoinHandle<Duration>>,
    shutdown_notify: Arc<Notify>,
    error_notify: Arc<Notify>,
    start: Duration,
}

impl CdcReaderState {
    fn new() -> Self {
        Self {
            reader: None,
            handler_task: None,
            shutdown_notify: Arc::new(Notify::new()),
            error_notify: Arc::new(Notify::new()),
            start: cdc_now(),
        }
    }

    /// Stops the current CDC reader and handler task, preserving the last checkpoint.
    async fn stop(&mut self) {
        if let Some(mut reader) = self.reader.take() {
            reader.stop();
        }
        if let Some(task) = self.handler_task.take() {
            self.shutdown_notify.notify_one();
            self.start = task.await.unwrap_or(cdc_now());
        }
    }

    /// Stops the current reader, drains stale notifications, and starts a new reader with the given parameters.
    async fn restart(
        &mut self,
        config: &Arc<Config>,
        session: &Arc<Session>,
        metadata: &IndexMetadata,
        tx_embeddings: &mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
        internals: &Sender<Internals>,
    ) {
        self.stop().await;
        drain_pending_notifications(&self.shutdown_notify).await;

        match create_cdc_reader(
            self.start,
            Arc::clone(config),
            Arc::clone(session),
            metadata.clone(),
            tx_embeddings.clone(),
        )
        .await
        {
            Ok((reader, handler)) => {
                self.reader = Some(reader);
                self.handler_task = Some(spawn_handler_task(
                    handler,
                    Arc::clone(&self.shutdown_notify),
                    Arc::clone(&self.error_notify),
                    internals.clone(),
                    metadata,
                ));

                info!(
                    "CDC reader created successfully for {} (safety: {:?}, sleep: {:?})",
                    metadata.key(),
                    config.cdc_safety_interval,
                    config.cdc_sleep_interval
                );
            }
            Err(e) => {
                error!("Failed to create CDC reader: {e}");
            }
        }
    }

    /// Handles a session change by restarting or stopping the CDC reader.
    async fn handle_session_change(
        &mut self,
        session: Option<Arc<Session>>,
        config_rx: &watch::Receiver<Arc<Config>>,
        metadata: &IndexMetadata,
        tx_embeddings: &mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
        internals: &Sender<Internals>,
    ) {
        match session {
            Some(session) => {
                info!(
                    "Session available, creating CDC reader for {}",
                    metadata.key()
                );

                let config = config_rx.borrow().clone();

                self.restart(&config, &session, metadata, tx_embeddings, internals)
                    .await;
            }
            None => {
                info!(
                    "Session became None, stopping CDC reader for {}",
                    metadata.key()
                );

                self.stop().await;
            }
        }
    }
}

fn cdc_now() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

/// Drains any pending shutdown notifications to avoid stale wakeups.
async fn drain_pending_notifications(notify: &Notify) {
    if pin!(notify.notified()).enable() {
        while pin!(notify.notified()).enable() {
            error!("Internal error: unable to cleanup CDC reader. Retrying.");
            time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// Creates a CDC log reader and its handler future.
async fn create_cdc_reader(
    cdc_start: Duration,
    config: Arc<Config>,
    session: Arc<Session>,
    metadata: IndexMetadata,
    tx_embeddings: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
) -> anyhow::Result<(
    scylla_cdc::log_reader::CDCLogReader,
    impl std::future::Future<Output = anyhow::Result<()>>,
)> {
    let consumer_factory = CdcConsumerFactory::new(Arc::clone(&session), &metadata, tx_embeddings)?;

    let cdc_start = cdc_start - CHECKPOINT_TIMESTAMP_OFFSET;
    info!(
        "Creating CDC log reader for {} starting from {:?}",
        metadata.key(),
        OffsetDateTime::UNIX_EPOCH + cdc_start
    );
    CDCLogReaderBuilder::new()
        .session(session)
        .keyspace(metadata.keyspace_name.as_ref())
        .table_name(metadata.table_name.as_ref())
        .consumer_factory(Arc::new(consumer_factory))
        .start_timestamp(chrono::Duration::from_std(cdc_start)?)
        .pipe(|builder| {
            if let Some(interval) = config.cdc_safety_interval {
                info!("Setting CDC safety interval to {interval:?}");
                builder.safety_interval(interval)
            } else {
                builder
            }
        })
        .pipe(|builder| {
            if let Some(interval) = config.cdc_sleep_interval {
                info!("Setting CDC sleep interval to {interval:?}");
                builder.sleep_interval(interval)
            } else {
                builder
            }
        })
        .build()
        .await
        .context("Failed to build CDC log reader")
}

/// Spawns a task that runs the CDC handler future until completion or shutdown.
fn spawn_handler_task(
    handler: impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    shutdown_notify: Arc<Notify>,
    cdc_error_notify: Arc<Notify>,
    internals: Sender<Internals>,
    metadata: &IndexMetadata,
) -> tokio::task::JoinHandle<Duration> {
    let handler_key = metadata.key();
    let span_name = format!("cdc_handler");
    let counter_name = format!("{handler_key}-cdc-handler-errors");

    tokio::spawn(
        async move {
            tokio::select! {
                result = handler => {
                    if let Err(err) = result {
                        warn!("CDC handler error: {err}");
                        internals.increment_counter(counter_name).await;
                        cdc_error_notify.notify_one();
                    }
                }
                _ = shutdown_notify.notified() => {
                    debug!("CDC handler: shutdown requested");
                }
            }
            debug!("CDC handler finished");
            cdc_now()
        }
        .instrument(error_span!("cdc_handler", "{}", span_name)),
    )
}

struct CdcConsumerData {
    primary_key_columns: Vec<ColumnName>,
    target_column: ColumnName,
    tx: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
    gregorian_epoch: PrimitiveDateTime,
}

struct CdcConsumer(Arc<CdcConsumerData>);

#[async_trait]
impl Consumer for CdcConsumer {
    async fn consume_cdc(&mut self, mut row: CDCRow<'_>) -> anyhow::Result<()> {
        if self.0.tx.is_closed() {
            // a consumer should be closed now, some concurrent tasks could stay in a pipeline
            return Ok(());
        }

        let target_column = self.0.target_column.as_ref();
        if !row.column_deletable(target_column) {
            bail!("CDC error: target column {target_column} should be deletable");
        }

        let embedding = row
            .take_value(target_column)
            .map(|value| {
                let CqlValue::Vector(value) = value else {
                    bail!("CDC error: target column {target_column} should be VECTOR type");
                };
                value
                    .into_iter()
                    .map(|value| {
                        value.as_float().ok_or(anyhow!(
                            "CDC error: target column {target_column} should be VECTOR<float> type"
                        ))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .transpose()?
            .map(|embedding| embedding.into());

        let primary_key = self
            .0
            .primary_key_columns
            .iter()
            .map(|column| {
                if !row.column_exists(column.as_ref()) {
                    bail!("CDC error: primary key column {column} should exist");
                }
                if row.column_deletable(column.as_ref()) {
                    bail!("CDC error: primary key column {column} should not be deletable");
                }
                row.take_value(column.as_ref()).ok_or(anyhow!(
                    "CDC error: primary key column {column} value should exist"
                ))
            })
            .collect::<anyhow::Result<_>>()?;

        const HUNDREDS_NANOS_TO_MICROS: u64 = 10;
        let timestamp = (self.0.gregorian_epoch
            + Duration::from_micros(
                row.time
                    .get_timestamp()
                    .ok_or(anyhow!("CDC error: time has no timestamp"))?
                    .to_gregorian()
                    .0
                    / HUNDREDS_NANOS_TO_MICROS,
            ))
        .into();

        _ = self
            .0
            .tx
            .send((
                DbEmbedding {
                    primary_key,
                    embedding,
                    timestamp,
                },
                None,
            ))
            .await;
        Ok(())
    }
}

struct CdcConsumerFactory(Arc<CdcConsumerData>);

#[async_trait]
impl ConsumerFactory for CdcConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(CdcConsumer(Arc::clone(&self.0)))
    }
}

impl CdcConsumerFactory {
    fn new(
        session: Arc<Session>,
        metadata: &IndexMetadata,
        tx: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
    ) -> anyhow::Result<Self> {
        let cluster_state = session.get_cluster_state();
        let table = cluster_state
            .get_keyspace(metadata.keyspace_name.as_ref())
            .ok_or_else(|| anyhow!("keyspace {} does not exist", metadata.keyspace_name))?
            .tables
            .get(metadata.table_name.as_ref())
            .ok_or_else(|| anyhow!("table {} does not exist", metadata.table_name))?;

        let primary_key_columns = table
            .partition_key
            .iter()
            .chain(table.clustering_key.iter())
            .cloned()
            .map(ColumnName::from)
            .collect();

        let gregorian_epoch = PrimitiveDateTime::new(
            Date::from_calendar_date(1582, Month::October, 15)?,
            Time::MIDNIGHT,
        );

        Ok(Self(Arc::new(CdcConsumerData {
            primary_key_columns,
            target_column: metadata.target_column.clone(),
            tx,
            gregorian_epoch,
        })))
    }
}
