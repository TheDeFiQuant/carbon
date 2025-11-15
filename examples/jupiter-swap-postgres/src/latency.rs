use {
    carbon_core::{
        datasource::{Datasource, DatasourceId, Update, UpdateType},
        error::{CarbonResult, Error as CarbonError},
        metrics::MetricsCollection,
    },
    chrono::{DateTime, Utc},
    sqlx::PgPool,
    std::{collections::HashMap, sync::Arc},
    tokio::{
        sync::{mpsc, Mutex},
        task::JoinHandle,
    },
    tokio_util::sync::CancellationToken,
};

const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

pub struct BlockLatencyRecorder {
    pool: PgPool,
}

impl BlockLatencyRecorder {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn record_block_arrival(
        &self,
        signature: &str,
        slot: u64,
        arrival_ts: DateTime<Utc>,
    ) -> CarbonResult<()> {
        let slot_value = i64::try_from(slot).map_err(|err| {
            CarbonError::Custom(format!(
                "slot value {slot} is too large for Postgres: {err}"
            ))
        })?;

        sqlx::query(
            r#"
            INSERT INTO pipeline_latency_measurements (
                __signature,
                slot,
                block_arrival_ts,
                updated_at
            )
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (__signature)
            DO UPDATE SET
                slot = EXCLUDED.slot,
                block_arrival_ts = LEAST(
                    COALESCE(pipeline_latency_measurements.block_arrival_ts, EXCLUDED.block_arrival_ts),
                    EXCLUDED.block_arrival_ts
                ),
                updated_at = NOW()
            "#,
        )
        .bind(signature)
        .bind(slot_value)
        .bind(arrival_ts)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|err| {
            CarbonError::Custom(format!(
                "Failed to record block arrival latency for {signature}: {err}"
            ))
        })
    }

    pub async fn record_data_inserted(&self, signature: &str, slot: u64) -> CarbonResult<()> {
        let slot_value = i64::try_from(slot).map_err(|err| {
            CarbonError::Custom(format!(
                "slot value {slot} is too large for Postgres: {err}"
            ))
        })?;

        sqlx::query(
            r#"
            INSERT INTO pipeline_latency_measurements (
                __signature,
                slot,
                data_inserted_ts,
                updated_at
            )
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (__signature)
            DO UPDATE SET
                slot = EXCLUDED.slot,
                data_inserted_ts = NOW(),
                updated_at = NOW()
            "#,
        )
        .bind(signature)
        .bind(slot_value)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|err| {
            CarbonError::Custom(format!(
                "Failed to record insert latency for {signature}: {err}"
            ))
        })
    }

    pub async fn record_aggregation_refreshed(
        &self,
        signature: &str,
        slot: u64,
        refreshed_at: DateTime<Utc>,
    ) -> CarbonResult<()> {
        let slot_value = i64::try_from(slot).map_err(|err| {
            CarbonError::Custom(format!(
                "slot value {slot} is too large for Postgres: {err}"
            ))
        })?;

        sqlx::query(
            r#"
            INSERT INTO pipeline_latency_measurements (
                __signature,
                slot,
                aggregation_refreshed_ts,
                updated_at
            )
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (__signature)
            DO UPDATE SET
                slot = EXCLUDED.slot,
                aggregation_refreshed_ts = EXCLUDED.aggregation_refreshed_ts,
                updated_at = NOW()
            "#,
        )
        .bind(signature)
        .bind(slot_value)
        .bind(refreshed_at)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|err| {
            CarbonError::Custom(format!(
                "Failed to record aggregation refresh for {signature}: {err}"
            ))
        })
    }
}

pub struct SlotArrivalTracker {
    slots: Mutex<HashMap<u64, DateTime<Utc>>>,
}

impl SlotArrivalTracker {
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
        }
    }

    pub async fn record_slot_arrival(&self, slot: u64) {
        let mut guard = self.slots.lock().await;
        guard.entry(slot).or_insert_with(Utc::now);
    }

    pub async fn take_slot_arrival(&self, slot: u64) -> Option<DateTime<Utc>> {
        let mut guard = self.slots.lock().await;
        guard.remove(&slot)
    }
}

pub struct InstrumentedDatasource<D> {
    inner: D,
    tracker: Arc<SlotArrivalTracker>,
    channel_capacity: usize,
}

impl<D> InstrumentedDatasource<D> {
    pub fn new(inner: D, tracker: Arc<SlotArrivalTracker>) -> Self {
        Self {
            inner,
            tracker,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

#[async_trait::async_trait]
impl<D> Datasource for InstrumentedDatasource<D>
where
    D: Datasource + Send + Sync + 'static,
{
    async fn consume(
        &self,
        id: DatasourceId,
        sender: mpsc::Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> CarbonResult<()> {
        let (forward_sender, mut forward_receiver) = mpsc::channel(self.channel_capacity);
        let user_sender = sender.clone();
        let tracker = self.tracker.clone();

        let forwarder: JoinHandle<()> = tokio::spawn(async move {
            while let Some((update, datasource_id)) = forward_receiver.recv().await {
                if let Update::Transaction(tx_update) = &update {
                    tracker.record_slot_arrival(tx_update.slot).await;
                }

                if user_sender.send((update, datasource_id)).await.is_err() {
                    break;
                }
            }
        });

        let result = self
            .inner
            .consume(
                id.clone(),
                forward_sender,
                cancellation_token.clone(),
                metrics,
            )
            .await;

        forwarder.await.ok();
        result
    }

    fn update_types(&self) -> Vec<UpdateType> {
        self.inner.update_types()
    }
}
