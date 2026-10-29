use crate::{
    checkpoint::{CheckpointRecord, InMemoryStateStore, StateStore},
    config::AggregatorConfig,
    lease::{InMemoryLeaseStore, LeaseStore, WriterLease, new_writer_id},
    partition::PartitionId,
    processor::{CandleProcessor, ProcessorUpdate},
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::{MarketInput, MarketSource, SourceRecord},
    state::AggregatorState,
};
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::{GapMarker, MarketEvent},
};
use platform_metrics::descriptions::{DECODE_KIND_TRADE, DROP_REASON_NO_DECODER, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_runtime::Runtime;
use platform_types::new_types::TimestampMilliseconds;
use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

/// Processes one partition's ordered inputs.
///
/// The worker acquires a fenced writer lease, applies inputs through the pure
/// [`CandleProcessor`], publishes outputs, checkpoints the keyed state
/// periodically and at shutdown, and only then acknowledges the source records.
///
/// TODO: a rebalance coordinator should signal this worker to stop accepting
/// new keys, drain the revoked key range through [`ProcessorRunner::commit`],
/// and only then move the lease. Today the shutdown path already drains and
/// checkpoints pending inputs, which the coordinator can use as that hook.
pub struct ProcessorRunner<S, K> {
    config: AggregatorConfig,
    source: S,
    sink: K,
    checkpoints: Arc<dyn StateStore>,
    leases: Arc<dyn LeaseStore>,
    partition: PartitionId,
    state: AggregatorState,
    /// When set, inputs at or before this watermark are already represented by
    /// state rebuilt from the archive and are only used to advance the source
    /// position.
    replay_watermark: Option<TimestampMilliseconds>,
    writer_id: Option<String>,
}

impl<S, K> ProcessorRunner<S, K>
where
    S: MarketSource,
    K: OpenCandleStore + ClosedCandlePublisher,
{
    pub fn new(config: AggregatorConfig, source: S, sink: K) -> Self {
        Self {
            config,
            source,
            sink,
            checkpoints: Arc::new(InMemoryStateStore::new()),
            leases: Arc::new(InMemoryLeaseStore::new()),
            partition: PartitionId::new(0),
            state: AggregatorState::new(),
            replay_watermark: None,
            writer_id: None,
        }
    }

    pub fn with_checkpoint_store(
        mut self,
        checkpoints: Arc<dyn StateStore>,
        partition: PartitionId,
    ) -> Self {
        self.checkpoints = checkpoints;
        self.partition = partition;
        self
    }

    pub fn with_lease_store(mut self, leases: Arc<dyn LeaseStore>) -> Self {
        self.leases = leases;
        self
    }

    pub fn with_writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Restores keyed state, for example from a checkpoint or archive rebuild.
    pub fn with_state(mut self, state: AggregatorState) -> Self {
        self.state = state;
        self
    }

    /// Ignores live inputs at or before `watermark` because their effects are
    /// already present in restored state.
    pub fn with_replay_watermark(mut self, watermark: Option<TimestampMilliseconds>) -> Self {
        self.replay_watermark = watermark;
        self
    }

    pub fn new_partition(
        config: AggregatorConfig,
        source: S,
        sink: K,
        checkpoints: Arc<dyn StateStore>,
        partition: PartitionId,
    ) -> Self {
        Self {
            config,
            source,
            sink,
            checkpoints,
            leases: Arc::new(InMemoryLeaseStore::new()),
            partition,
            state: AggregatorState::new(),
            replay_watermark: None,
            writer_id: None,
        }
    }

    pub async fn run(self) -> BpResult<()> {
        let Self {
            config,
            mut source,
            sink,
            checkpoints,
            leases,
            partition,
            state,
            replay_watermark,
            writer_id,
        } = self;
        let mut processor = CandleProcessor::from_state(config.processor_config(), state)?;
        let writer_id = writer_id.unwrap_or_else(new_writer_id);
        let Some(mut lease) = leases
            .acquire(partition, &writer_id, config.lease_ttl)
            .await?
        else {
            return Err(BpError::bad_value(
                "aggregator lease",
                partition.to_string(),
                "another writer already holds the partition lease",
            ));
        };

        info!(
            partition = %partition,
            writer = %writer_id,
            fencing_token = lease.fencing_token,
            intervals = ?config
                .intervals
                .iter()
                .map(|interval| interval.label())
                .collect::<Vec<_>>(),
            "aggregator partition processor started"
        );

        let result = Self::process_inputs(
            &config,
            &mut source,
            &sink,
            &checkpoints,
            &leases,
            partition,
            &mut processor,
            replay_watermark,
            &mut lease,
            &writer_id,
        )
        .await;
        let release_result = leases.release(&lease).await;

        result?;
        release_result?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_inputs(
        config: &AggregatorConfig,
        source: &mut S,
        sink: &K,
        checkpoints: &Arc<dyn StateStore>,
        leases: &Arc<dyn LeaseStore>,
        partition: PartitionId,
        processor: &mut CandleProcessor,
        replay_watermark: Option<TimestampMilliseconds>,
        lease: &mut WriterLease,
        writer_id: &str,
    ) -> BpResult<()> {
        let mut decoders: HashMap<CexId, SharedTradeDecoder> = HashMap::new();
        let mut pending: Vec<SourceRecord> = Vec::new();
        let mut last_record: Option<SourceRecord> = None;
        let mut dirty = false;
        let mut checkpoint_ticker = time::interval(config.checkpoint_interval);
        checkpoint_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        checkpoint_ticker.tick().await;
        let shutdown = Runtime::shutdown_signal();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                maybe_record = source.next_input() => {
                    match maybe_record {
                        Ok(Some(record)) => {
                            let is_replay = replay_watermark.is_some_and(|watermark| {
                                input_timestamp(&record.input) <= watermark
                            });
                            if !is_replay {
                                let update = Self::process_input(
                                    processor,
                                    &mut decoders,
                                    &record.input,
                                );
                                Self::publish_update(sink, &update).await?;
                                dirty = true;
                            }
                            if let Some(sequence) = record.message.stream_sequence {
                                processor.state_mut().mark_processed(sequence);
                                dirty = true;
                            }
                            last_record = Some(record.clone());
                            pending.push(record);
                        }
                        Ok(None) => {
                            info!("aggregator input source ended; draining partition");
                            Self::commit(
                                config,
                                source,
                                checkpoints,
                                leases,
                                partition,
                                processor,
                                lease,
                                writer_id,
                                &mut pending,
                                last_record.as_ref(),
                                dirty,
                            )
                            .await?;
                            break;
                        }
                        Err(error) => {
                            error!(%error, "aggregator input source stopped");
                            return Err(error);
                        }
                    }
                }
                _ = checkpoint_ticker.tick() => {
                    Self::commit(
                        config,
                        source,
                        checkpoints,
                        leases,
                        partition,
                        processor,
                        lease,
                        writer_id,
                        &mut pending,
                        last_record.as_ref(),
                        dirty,
                    )
                    .await?;
                    dirty = false;
                }
                signal = &mut shutdown => {
                    signal?;
                    info!("shutdown requested; draining and checkpointing aggregator partition");
                    Self::commit(
                        config,
                        source,
                        checkpoints,
                        leases,
                        partition,
                        processor,
                        lease,
                        writer_id,
                        &mut pending,
                        last_record.as_ref(),
                        dirty,
                    )
                    .await?;
                    break;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit(
        config: &AggregatorConfig,
        source: &mut S,
        checkpoints: &Arc<dyn StateStore>,
        leases: &Arc<dyn LeaseStore>,
        partition: PartitionId,
        processor: &CandleProcessor,
        lease: &mut WriterLease,
        writer_id: &str,
        pending: &mut Vec<SourceRecord>,
        last_record: Option<&SourceRecord>,
        dirty: bool,
    ) -> BpResult<()> {
        let Some(renewed) = leases.renew(lease, config.lease_ttl).await? else {
            return Err(BpError::bad_value(
                "aggregator lease",
                partition.to_string(),
                format!("writer {writer_id} lost its fencing lease"),
            ));
        };
        *lease = renewed;

        if !dirty && pending.is_empty() {
            return Ok(());
        }

        let Some(record) = last_record else {
            return Err(BpError::bad_value(
                "aggregator checkpoint",
                partition.to_string(),
                "cannot checkpoint without a source record",
            ));
        };
        let Some(checkpoint) =
            CheckpointRecord::from_message(&record.message, partition, processor.state().clone())?
        else {
            return Err(BpError::bad_value(
                "aggregator checkpoint",
                partition.to_string(),
                "source record has no JetStream sequence to checkpoint",
            ));
        };
        checkpoints.save(&checkpoint.with_lease(lease)).await?;

        for record in pending.drain(..) {
            source.ack(&record).await?;
        }
        Ok(())
    }

    fn process_input(
        processor: &mut CandleProcessor,
        decoders: &mut HashMap<CexId, SharedTradeDecoder>,
        input: &MarketInput,
    ) -> ProcessorUpdate {
        match input {
            MarketInput::Event(event) => Self::process_event(processor, decoders, event),
            MarketInput::Gap(gap) => Self::process_gap(processor, gap),
        }
    }

    fn process_gap(processor: &mut CandleProcessor, gap: &GapMarker) -> ProcessorUpdate {
        let update = processor.apply_gap(gap);
        if !update.closed.is_empty() || !update.incomplete_windows.is_empty() {
            info!(
                exchange = %gap.exchange,
                stream = %gap.stream,
                missing_from = gap.missing_from,
                missing_to = gap.missing_to,
                closed = update.closed.len(),
                incomplete_windows = update.incomplete_windows.len(),
                "recorded gap and closed candle windows at the event-time watermark"
            );
        }
        update
    }

    fn process_event(
        processor: &mut CandleProcessor,
        decoders: &mut HashMap<CexId, SharedTradeDecoder>,
        event: &MarketEvent,
    ) -> ProcessorUpdate {
        let decoder = match decoders.entry(event.exchange) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => match TradeDecoderFactory::for_exchange(event.exchange) {
                Ok(decoder) => entry.insert(decoder),
                Err(error) => {
                    MarketDataMetrics::record_dropped_event(
                        STAGE_AGGREGATOR,
                        DROP_REASON_NO_DECODER,
                    );
                    warn!(%error, exchange = %event.exchange, "no trade decoder for exchange");
                    return processor.apply_event(event, &[]);
                }
            },
        };
        match decoder.decode(event) {
            Ok(trades) => processor.apply_event(event, &trades),
            Err(error) => {
                // Undecodable exchange streams still advance the event-time
                // watermark and are deduplicated, but they contribute no trades.
                MarketDataMetrics::record_decode_failure(STAGE_AGGREGATOR, DECODE_KIND_TRADE);
                warn!(%error, stream = %event.stream, "skipping unreadable market event");
                processor.apply_event(event, &[])
            }
        }
    }

    async fn publish_update(sink: &K, update: &ProcessorUpdate) -> BpResult<()> {
        if !update.closed.is_empty() {
            sink.publish_closed(&update.closed).await?;
        }
        if !update.open.is_empty() {
            sink.update_open(&update.open).await?;
        }
        Ok(())
    }
}

fn input_timestamp(input: &MarketInput) -> TimestampMilliseconds {
    match input {
        MarketInput::Event(event) => event.exchange_timestamp.unwrap_or(event.received_at),
        MarketInput::Gap(gap) => gap.detected_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::StateStore;
    use crate::state::AggregatorState;
    use async_trait::async_trait;
    use platform_errors::BpError;
    use platform_market_data::candle::{Candle, ClosedCandle};
    use platform_nats::Message;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeSource {
        records: Vec<SourceRecord>,
        fail: bool,
        acked: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl MarketSource for FakeSource {
        async fn next_input(&mut self) -> BpResult<Option<SourceRecord>> {
            if self.fail {
                return Err(BpError::external("source failed"));
            }
            if self.records.is_empty() {
                Ok(None)
            } else {
                Ok(Some(self.records.remove(0)))
            }
        }

        async fn ack(&mut self, _record: &SourceRecord) -> BpResult<()> {
            *self.acked.lock().expect("acked lock") += 1;
            Ok(())
        }
    }

    #[derive(Default, Clone)]
    struct FakeSink {
        open_updates: Arc<Mutex<usize>>,
        closed_updates: Arc<Mutex<usize>>,
        fail: bool,
    }

    impl FakeSink {
        fn open_updates(&self) -> usize {
            *self.open_updates.lock().expect("open updates lock")
        }

        fn closed_updates(&self) -> usize {
            *self.closed_updates.lock().expect("closed updates lock")
        }
    }

    #[async_trait]
    impl OpenCandleStore for FakeSink {
        async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
            if self.fail {
                return Err(BpError::bad_value("sink", "test", "write failed"));
            }
            *self.open_updates.lock().expect("open updates lock") += candles.len();
            Ok(())
        }
    }

    #[async_trait]
    impl ClosedCandlePublisher for FakeSink {
        async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
            if self.fail {
                return Err(BpError::bad_value("sink", "test", "write failed"));
            }
            *self.closed_updates.lock().expect("closed updates lock") += candles.len();
            Ok(())
        }
    }

    struct FailingCheckpointStore;

    #[async_trait]
    impl StateStore for FailingCheckpointStore {
        async fn load(&self, _partition: PartitionId) -> BpResult<Option<CheckpointRecord>> {
            Ok(None)
        }

        async fn save(&self, _checkpoint: &CheckpointRecord) -> BpResult<()> {
            Err(BpError::bad_value("checkpoint", "test", "write failed"))
        }
    }

    fn config() -> AggregatorConfig {
        AggregatorConfig {
            log_filter: "info".to_string(),
            nats_url: "nats://localhost:4222".to_string(),
            event_subject_prefix: "market.events".to_string(),
            gap_subject_prefix: "market.gaps".to_string(),
            open_candle_subject_prefix: "market.candles.open".to_string(),
            closed_candle_subject_prefix: "market.candles.closed".to_string(),
            shard_subject_prefix: "aggregator.shards".to_string(),
            shard_count: 4,
            shard_index: None,
            intervals: vec![CandleInterval::from_label("1m").unwrap()],
            allowed_lateness: std::time::Duration::from_millis(5_000),
            checkpoint_interval: std::time::Duration::from_secs(1),
            lease_ttl: std::time::Duration::from_secs(30),
        }
    }

    use platform_market_data::candle::CandleInterval;

    fn trade_event(timestamp_ms: i64) -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(1),
            exchange_timestamp: Some(TimestampMilliseconds(timestamp_ms)),
            received_at: TimestampMilliseconds(timestamp_ms + 1),
            payload: "eyJlIjoidHJhZGUiLCJzIjoiQlRDVVNEVCIsInAiOiIxMDAiLCJxIjoiMSIsIlQiOjYxMDAwfQ=="
                .to_string(),
        }
    }

    fn record(input: MarketInput, sequence: u64) -> SourceRecord {
        SourceRecord::new(
            input,
            Message {
                subject: format!("aggregator.shards.0.{sequence}"),
                stream: Some("aggregator-shards".to_string()),
                stream_sequence: Some(sequence),
                consumer_sequence: Some(sequence),
                ..Message::default()
            },
        )
    }

    #[tokio::test]
    async fn runs_with_injected_source_and_sink() {
        let sink = FakeSink::default();
        let observed = sink.clone();
        let runner = ProcessorRunner::new(config(), FakeSource::default(), sink);

        runner.run().await.unwrap();

        assert_eq!(observed.open_updates(), 0);
        assert_eq!(observed.closed_updates(), 0);
    }

    #[tokio::test]
    async fn checkpoints_before_acknowledging_a_trade() {
        let sink = FakeSink::default();
        let observed = sink.clone();
        let acked = Arc::new(Mutex::new(0));
        let source = FakeSource {
            records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
            acked: acked.clone(),
            ..FakeSource::default()
        };
        let checkpoints = Arc::new(InMemoryStateStore::new());
        let runner = ProcessorRunner::new(config(), source, sink)
            .with_checkpoint_store(checkpoints.clone(), PartitionId::new(0));

        runner.run().await.unwrap();

        assert_eq!(observed.open_updates(), 1);
        assert_eq!(*acked.lock().unwrap(), 1);
        let checkpoint = checkpoints.get(PartitionId::new(0)).unwrap();
        assert_eq!(checkpoint.next_sequence, 2);
        assert_eq!(checkpoint.resume_sequence(), 2);
        assert_eq!(checkpoint.source_stream, "aggregator-shards");
        assert_eq!(checkpoint.partition, PartitionId::new(0));
        assert_eq!(checkpoint.watermark, Some(TimestampMilliseconds(61_000)));
        assert_eq!(checkpoint.state.last_processed_sequence, Some(1));
        assert_eq!(checkpoint.state.open_candles_len(), 1);
        assert_eq!(checkpoint.fencing_token, 1);
        assert!(!checkpoint.writer_id.is_empty());
    }

    #[tokio::test]
    async fn does_not_acknowledge_when_applying_a_trade_fails() {
        let acked = Arc::new(Mutex::new(0));
        let source = FakeSource {
            records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
            acked: acked.clone(),
            ..FakeSource::default()
        };
        let sink = FakeSink {
            fail: true,
            ..FakeSink::default()
        };

        assert!(
            ProcessorRunner::new(config(), source, sink)
                .run()
                .await
                .is_err()
        );
        assert_eq!(*acked.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn does_not_acknowledge_when_checkpointing_fails() {
        let acked = Arc::new(Mutex::new(0));
        let source = FakeSource {
            records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
            acked: acked.clone(),
            ..FakeSource::default()
        };
        let runner = ProcessorRunner::new(config(), source, FakeSink::default())
            .with_checkpoint_store(Arc::new(FailingCheckpointStore), PartitionId::new(0));

        assert!(runner.run().await.is_err());
        assert_eq!(*acked.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn gap_markers_close_expired_candle_windows() {
        let sink = FakeSink::default();
        let observed = sink.clone();
        let acked = Arc::new(Mutex::new(0));
        let gap = GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 1,
            observed_sequence: 3,
            missing_from: 2,
            missing_to: 2,
            detected_at: TimestampMilliseconds(126_000),
        };
        let source = FakeSource {
            records: vec![
                record(MarketInput::Event(trade_event(61_000)), 1),
                record(MarketInput::Gap(gap), 2),
            ],
            acked: acked.clone(),
            ..FakeSource::default()
        };

        ProcessorRunner::new(config(), source, sink)
            .run()
            .await
            .unwrap();

        assert_eq!(observed.open_updates(), 1);
        assert_eq!(observed.closed_updates(), 1);
        assert_eq!(*acked.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn duplicate_events_are_applied_once_but_both_are_acknowledged() {
        let sink = FakeSink::default();
        let observed = sink.clone();
        let acked = Arc::new(Mutex::new(0));
        let source = FakeSource {
            records: vec![
                record(MarketInput::Event(trade_event(61_000)), 1),
                record(MarketInput::Event(trade_event(61_000)), 2),
            ],
            acked: acked.clone(),
            ..FakeSource::default()
        };

        ProcessorRunner::new(config(), source, sink)
            .run()
            .await
            .unwrap();

        assert_eq!(observed.open_updates(), 1);
        assert_eq!(*acked.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn restores_state_and_skips_inputs_already_in_the_archive() {
        let sink = FakeSink::default();
        let observed = sink.clone();
        let acked = Arc::new(Mutex::new(0));
        let source = FakeSource {
            records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
            acked: acked.clone(),
            ..FakeSource::default()
        };
        let checkpoints = Arc::new(InMemoryStateStore::new());
        let mut restored_state = AggregatorState::new();
        restored_state.advance_watermark(TimestampMilliseconds(61_000));

        ProcessorRunner::new(config(), source, sink)
            .with_checkpoint_store(checkpoints.clone(), PartitionId::new(0))
            .with_state(restored_state)
            .with_replay_watermark(Some(TimestampMilliseconds(61_000)))
            .run()
            .await
            .unwrap();

        assert_eq!(observed.open_updates(), 0);
        assert_eq!(*acked.lock().unwrap(), 1);
        let checkpoint = checkpoints.get(PartitionId::new(0)).unwrap();
        assert_eq!(checkpoint.next_sequence, 2);
        assert_eq!(checkpoint.watermark, Some(TimestampMilliseconds(61_000)));
        assert_eq!(checkpoint.state.open_candles_len(), 0);
    }

    #[tokio::test]
    async fn rejects_a_partition_that_already_has_an_active_writer() {
        let leases = Arc::new(InMemoryLeaseStore::new());
        let held = leases
            .acquire(
                PartitionId::new(0),
                "other-writer",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap()
            .unwrap();
        let runner = ProcessorRunner::new(config(), FakeSource::default(), FakeSink::default())
            .with_lease_store(leases.clone())
            .with_writer_id("this-writer");

        let error = runner.run().await.unwrap_err();

        assert!(error.to_string().contains("another writer"));
        assert_eq!(leases.current(held.partition).await.unwrap(), Some(held));
    }

    #[tokio::test]
    async fn does_not_acknowledge_a_record_without_checkpoint_metadata() {
        let acked = Arc::new(Mutex::new(0));
        let source = FakeSource {
            records: vec![SourceRecord::new(
                MarketInput::Event(trade_event(61_000)),
                Message {
                    subject: "aggregator.shards.0".to_string(),
                    ..Message::default()
                },
            )],
            acked: acked.clone(),
            ..FakeSource::default()
        };

        assert!(
            ProcessorRunner::new(config(), source, FakeSink::default())
                .run()
                .await
                .is_err()
        );
        assert_eq!(*acked.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn propagates_source_error() {
        let sink = FakeSink::default();
        let runner = ProcessorRunner::new(
            config(),
            FakeSource {
                records: Vec::new(),
                fail: true,
                ..FakeSource::default()
            },
            sink,
        );

        let error = runner.run().await.unwrap_err();

        assert_eq!(error.to_string(), "source failed");
    }
}
