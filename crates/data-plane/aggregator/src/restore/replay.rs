//! Archived event replay for aggregator restore.
//!
//! Provides the event source abstraction used by state rebuilds and streams
//! archive event objects from S3 in bounded Parquet batches.

use super::catalog::{ArchiveObject, ArchiveRecoveryConfig, S3ArchiveRecovery};
use crate::partition::{PartitionId, partition_for};
use async_trait::async_trait;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use platform_archive::{ArchiveKind, ArchiveParquet, ArchivedMarketEvent, ParquetReader};
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::info;

/// A stream of raw market events from the archive.
#[async_trait]
pub trait ArchivedEventSource: Send {
    async fn next_event(&mut self) -> BpResult<Option<ArchivedMarketEvent>>;
}

/// Archive source used when recovery from archived events is not configured.
pub struct EmptyArchivedEventSource;

#[async_trait]
impl ArchivedEventSource for EmptyArchivedEventSource {
    async fn next_event(&mut self) -> BpResult<Option<ArchivedMarketEvent>> {
        Ok(None)
    }
}

pub(crate) fn event_timestamp(event: &MarketEvent) -> TimestampMilliseconds {
    event.exchange_timestamp.unwrap_or(event.received_at)
}

pub(crate) fn decoder_for(
    decoders: &mut HashMap<CexId, SharedTradeDecoder>,
    exchange: CexId,
) -> BpResult<SharedTradeDecoder> {
    if let Some(decoder) = decoders.get(&exchange) {
        return Ok(decoder.clone());
    }
    let decoder = TradeDecoderFactory::for_exchange(exchange)?;
    decoders.insert(exchange, decoder.clone());
    Ok(decoder)
}

/// Replays archive event objects from S3 for one partition.
///
/// Objects are read in bounded Parquet batches through ranged S3 requests, so
/// recovery never downloads or decodes a whole object at once. A
/// process-wide semaphore bounds how many archive objects are streamed
/// concurrently across all partitions.
pub struct S3ArchivedEventSource {
    transfer_client: TransferClient,
    partition: PartitionId,
    partition_count: usize,
    objects: VecDeque<ArchiveObject>,
    reader: Option<platform_archive::ArchiveParquetStream<ParquetReader>>,
    /// Keeps the recovery slot held for as long as an object is being streamed.
    _reader_permit: Option<OwnedSemaphorePermit>,
    recovery_slots: Arc<Semaphore>,
    events: VecDeque<ArchivedMarketEvent>,
    decoders: HashMap<CexId, SharedTradeDecoder>,
}

impl S3ArchivedEventSource {
    pub(crate) fn from_objects(
        transfer_client: TransferClient,
        partition: PartitionId,
        partition_count: usize,
        objects: VecDeque<ArchiveObject>,
        recovery_slots: Arc<Semaphore>,
    ) -> Self {
        Self {
            transfer_client,
            partition,
            partition_count,
            objects,
            reader: None,
            _reader_permit: None,
            recovery_slots,
            events: VecDeque::new(),
            decoders: HashMap::new(),
        }
    }

    pub async fn connect(
        config: &ArchiveRecoveryConfig,
        partition: PartitionId,
        partition_count: usize,
    ) -> BpResult<Self> {
        S3ArchiveRecovery::new(config.clone())
            .connect(partition, partition_count)
            .await
    }

    async fn load_next_batch(&mut self) -> BpResult<bool> {
        loop {
            if self.reader.is_none() {
                let Some(object) = self.objects.pop_front() else {
                    return Ok(false);
                };
                let file_size = u64::try_from(object.size).map_err(|error| {
                    BpError::external(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "archive object {} has invalid size {}: {error}",
                            object.key, object.size
                        ),
                    ))
                })?;
                let permit =
                    self.recovery_slots
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|error| {
                            BpError::external(std::io::Error::other(format!(
                                "archive recovery limiter closed: {error}"
                            )))
                        })?;
                let reader = ParquetReader::new(
                    &self.transfer_client,
                    &object.bucket,
                    &object.key,
                    file_size,
                );
                self.reader =
                    Some(ArchiveParquet::decode_reader_batches(ArchiveKind::Events, reader).await?);
                self._reader_permit = Some(permit);
                info!(
                    key = %object.key,
                    "streaming archive object for recovery"
                );
            }

            let Some(batch) = self
                .reader
                .as_mut()
                .expect("archive reader is initialized")
                .next_batch()
                .await?
            else {
                self.reader = None;
                self._reader_permit = None;
                continue;
            };
            let mut events = batch
                .into_records()
                .into_iter()
                .filter_map(|record| match record {
                    platform_archive::ArchiveRecord::MarketEvent(event) => Some(event),
                    platform_archive::ArchiveRecord::ClosedCandle(_) => None,
                })
                .collect::<Vec<_>>();
            events.sort_by_key(|event| {
                (
                    event_timestamp(&event.event).0,
                    event.event.sequence.unwrap_or_default(),
                    event.state_epoch,
                )
            });
            if events.is_empty() {
                continue;
            }
            self.events.extend(events);
            info!(
                queued_events = self.events.len(),
                "loaded archive batch for recovery"
            );
            return Ok(true);
        }
    }
}

#[async_trait]
impl ArchivedEventSource for S3ArchivedEventSource {
    async fn next_event(&mut self) -> BpResult<Option<ArchivedMarketEvent>> {
        loop {
            if let Some(event) = self.events.pop_front() {
                let decoder = decoder_for(&mut self.decoders, event.exchange)?;
                let trades = decoder.decode(&event.event)?;
                if trades.iter().any(|trade| {
                    partition_for(trade.exchange, trade.trading_pair, self.partition_count)
                        == self.partition
                }) {
                    return Ok(Some(event));
                }
                continue;
            }
            if !self.load_next_batch().await? {
                return Ok(None);
            }
        }
    }
}
