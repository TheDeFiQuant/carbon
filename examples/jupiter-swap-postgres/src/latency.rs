use {
    carbon_core::{
        datasource::{Datasource, DatasourceId, Update, UpdateType},
        error::{CarbonResult, Error as CarbonError},
        metrics::MetricsCollection,
    },
    sqlx::PgPool,
    std::sync::Arc,
    tokio::{
        sync::mpsc::{self, Sender},
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

    pub async fn record_block_arrival(&self, signature: &str, slot: u64) -> CarbonResult<()> {
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
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (__signature)
            DO UPDATE SET
                slot = EXCLUDED.slot,
                block_arrival_ts = COALESCE(
                    pipeline_latency_measurements.block_arrival_ts,
                    EXCLUDED.block_arrival_ts
                ),
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
                "Failed to record block arrival latency for {signature}: {err}"
            ))
        })
    }
}

pub struct InstrumentedDatasource<D> {
    inner: D,
    recorder: Arc<BlockLatencyRecorder>,
    channel_capacity: usize,
}

impl<D> InstrumentedDatasource<D> {
    pub fn new(inner: D, recorder: Arc<BlockLatencyRecorder>) -> Self {
        Self {
            inner,
            recorder,
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
        sender: Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> CarbonResult<()> {
        let (instrumented_sender, mut instrumented_receiver) = mpsc::channel(self.channel_capacity);
        let forward_sender = sender.clone();
        let recorder = self.recorder.clone();

        let forwarder: JoinHandle<()> = tokio::spawn(async move {
            while let Some((update, datasource_id)) = instrumented_receiver.recv().await {
                if let Update::Transaction(tx_update) = &update {
                    if let Err(err) = recorder
                        .record_block_arrival(&tx_update.signature.to_string(), tx_update.slot)
                        .await
                    {
                        log::error!("Failed to record block arrival: {err}");
                    }
                }

                if forward_sender.send((update, datasource_id)).await.is_err() {
                    break;
                }
            }
        });

        let result = self
            .inner
            .consume(
                id.clone(),
                instrumented_sender,
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
