use crate::{
    aggregator::CandleAggregator,
    checkpoint::{CheckpointStore, InMemoryCheckpointStore, SourceCheckpoint},
    config::AggregatorConfig,
    partition::PartitionId,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::{MarketInput, MarketSource, SourceRecord},
};
use platform_errors::BpResult;
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
/// A runner owns the in-memory candles for its partition, checkpoints the source
/// position after applying each input, and only then acknowledges the message.
pub struct AggregatorRunner<S, K> {
    config: AggregatorConfig,
    source: S,
    sink: K,
    checkpoints: Arc<dyn CheckpointStore>,
    partition: PartitionId,
}

impl<S, K> AggregatorRunner<S, K>
where
    S: MarketSource,
    K: OpenCandleStore + ClosedCandlePublisher,
{
    /// Creates a runner with an in-memory checkpoint store.
    ///
    /// Production wiring uses [`Self::with_checkpoint_store`] so checkpoints
    /// survive a restart.
    pub fn new(config: AggregatorConfig, source: S, sink: K) -> Self {
        Self {
            config,
            source,
            sink,
            checkpoints: Arc::new(InMemoryCheckpointStore::new()),
            partition: PartitionId::new(0),
        }
    }

    pub fn with_checkpoint_store(
        mut self,
        checkpoints: Arc<dyn CheckpointStore>,
        partition: PartitionId,
    ) -> Self {
        self.checkpoints = checkpoints;
        self.partition = partition;
        self
    }

    pub fn new_partition(
        config: AggregatorConfig,
        source: S,
        sink: K,
        checkpoints: Arc<dyn CheckpointStore>,
        partition: PartitionId,
    ) -> Self {
        Self {
            config,
            source,
            sink,
            checkpoints,
            partition,
        }
    }

    pub async fn run(self) -> BpResult<()> {
        let Self {
            config,
            mut source,
            sink,
            checkpoints,
            partition,
        } = self;
        let mut aggregator = CandleAggregator::new(config.intervals.clone());

        info!(
            partition = %partition,
            intervals = ?config
                .intervals
                .iter()
                .map(|interval| interval.label())
                .collect::<Vec<_>>(),
            "aggregator partition started"
        );

        let result = Runtime::run_until_shutdown(Self::process_inputs(
            &config,
            &mut source,
            &sink,
            &checkpoints,
            partition,
            &mut aggregator,
        ))
        .await;
        result?;

        let closed = aggregator.flush_closed_at_or_before(TimestampMilliseconds::now());
        if !closed.is_empty() {
            sink.publish_closed(&closed).await?;
        }
        Ok(())
    }

    async fn process_inputs(
        config: &AggregatorConfig,
        source: &mut S,
        sink: &K,
        checkpoints: &Arc<dyn CheckpointStore>,
        partition: PartitionId,
        aggregator: &mut CandleAggregator,
    ) -> BpResult<()> {
        let mut decoders: HashMap<CexId, SharedTradeDecoder> = HashMap::new();
        let mut flush_ticker = time::interval(config.flush_interval);
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe_record = source.next_input() => {
                    match maybe_record {
                        Ok(Some(record)) => {
                            Self::process_input(
                                sink,
                                aggregator,
                                &mut decoders,
                                &record.input,
                            )
                            .await?;
                            Self::checkpoint(source, checkpoints, partition, &record).await?;
                        }
                        Ok(None) => {
                            info!("aggregator input source ended");
                            break;
                        }
                        Err(error) => {
                            error!(%error, "aggregator input source stopped");
                            return Err(error);
                        }
                    }
                }
                _ = flush_ticker.tick() => {
                    let closed = aggregator
                        .flush_closed_at_or_before(TimestampMilliseconds::now());
                    if !closed.is_empty() {
                        sink.publish_closed(&closed).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Persists the source position before acknowledging the message.
    ///
    /// If the checkpoint write fails, the acknowledgement is skipped and the
    /// message is redelivered after a restart.
    async fn checkpoint(
        source: &mut S,
        checkpoints: &Arc<dyn CheckpointStore>,
        partition: PartitionId,
        record: &SourceRecord,
    ) -> BpResult<()> {
        if let Some(checkpoint) = SourceCheckpoint::from_message(&record.message)? {
            checkpoints.save(partition, &checkpoint).await?;
        }
        source.ack(record).await
    }

    async fn process_input(
        sink: &K,
        aggregator: &mut CandleAggregator,
        decoders: &mut HashMap<CexId, SharedTradeDecoder>,
        input: &MarketInput,
    ) -> BpResult<()> {
        match input {
            MarketInput::Event(event) => {
                Self::process_event(sink, aggregator, decoders, event).await
            }
            MarketInput::Gap(gap) => Self::process_gap(sink, aggregator, gap).await,
        }
    }

    async fn process_gap(
        sink: &K,
        aggregator: &mut CandleAggregator,
        gap: &GapMarker,
    ) -> BpResult<()> {
        let closed = aggregator.apply_gap(gap);
        if !closed.is_empty() {
            info!(
                exchange = %gap.exchange,
                stream = %gap.stream,
                missing_from = gap.missing_from,
                missing_to = gap.missing_to,
                closed = closed.len(),
                "closed candle windows at a gap marker"
            );
            sink.publish_closed(&closed).await?;
        }
        Ok(())
    }

    async fn process_event(
        sink: &K,
        aggregator: &mut CandleAggregator,
        decoders: &mut HashMap<CexId, SharedTradeDecoder>,
        event: &MarketEvent,
    ) -> BpResult<()> {
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
                    return Ok(());
                }
            },
        };
        match decoder.decode(event) {
            Ok(trades) => {
                for trade in trades {
                    let update = aggregator.apply_trade(&trade);
                    if update.is_empty() {
                        continue;
                    }
                    if !update.closed.is_empty() {
                        sink.publish_closed(&update.closed).await?;
                    }
                    if !update.open.is_empty() {
                        sink.update_open(&update.open).await?;
                    }
                }
            }
            Err(error) => {
                // Exchange streams can contain binary formats that this
                // first cut does not decode yet. Skip the event instead
                // of stopping aggregation for every market.
                MarketDataMetrics::record_decode_failure(STAGE_AGGREGATOR, DECODE_KIND_TRADE);
                warn!(%error, stream = %event.stream, "skipping unreadable market event");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                return Err(platform_errors::BpError::external("source failed"));
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
                return Err(platform_errors::BpError::bad_value(
                    "sink",
                    "test",
                    "write failed",
                ));
            }
            *self.open_updates.lock().expect("open updates lock") += candles.len();
            Ok(())
        }
    }

    #[async_trait]
    impl ClosedCandlePublisher for FakeSink {
        async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
            if self.fail {
                return Err(platform_errors::BpError::bad_value(
                    "sink",
                    "test",
                    "write failed",
                ));
            }
            *self.closed_updates.lock().expect("closed updates lock") += candles.len();
            Ok(())
        }
    }

    struct FailingCheckpointStore;

    #[async_trait]
    impl CheckpointStore for FailingCheckpointStore {
        async fn load(&self, _partition: PartitionId) -> BpResult<Option<SourceCheckpoint>> {
            Ok(None)
        }

        async fn save(
            &self,
            _partition: PartitionId,
            _checkpoint: &SourceCheckpoint,
        ) -> BpResult<()> {
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
            intervals: vec![
                platform_market_data::candle::CandleInterval::from_label("1m").unwrap(),
            ],
            flush_interval: std::time::Duration::from_secs(1),
        }
    }

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
        let runner = AggregatorRunner::new(config(), FakeSource::default(), sink);

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
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        let runner = AggregatorRunner::new(config(), source, sink)
            .with_checkpoint_store(checkpoints.clone(), PartitionId::new(0));

        runner.run().await.unwrap();

        assert_eq!(observed.open_updates(), 1);
        assert_eq!(*acked.lock().unwrap(), 1);
        let checkpoint = checkpoints.get(PartitionId::new(0)).unwrap();
        assert_eq!(checkpoint.stream_sequence, 1);
        assert_eq!(checkpoint.resume_sequence(), 2);
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
            AggregatorRunner::new(config(), source, sink)
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
        let runner = AggregatorRunner::new(config(), source, FakeSink::default())
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
            detected_at: TimestampMilliseconds(120_000),
        };
        let source = FakeSource {
            records: vec![
                record(MarketInput::Event(trade_event(61_000)), 1),
                record(MarketInput::Gap(gap), 2),
            ],
            acked: acked.clone(),
            ..FakeSource::default()
        };

        AggregatorRunner::new(config(), source, sink)
            .run()
            .await
            .unwrap();

        assert_eq!(observed.open_updates(), 1);
        assert_eq!(observed.closed_updates(), 1);
        assert_eq!(*acked.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn propagates_source_error() {
        let sink = FakeSink::default();
        let runner = AggregatorRunner::new(
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
