use crate::runner::QuiesceSignal;
use crate::{
    candle_sink::{ClosedCandlePublisher, OpenCandleStore},
    checkpoint_record::{CheckpointRecord, StateStore},
    config::AggregatorConfig,
    market_source::{MarketInput, MarketSource, SourceRecord},
    processor::ProcessorUpdate,
    shard_id::ShardId,
    state::AggregatorState,
};
use async_trait::async_trait;
use platform_errors::{BuildError, BuildResult};
use platform_market_data::{AlignedWindow, Candle, CandleKey, ClosedCandle, MarketEvent};
use platform_nats::Message;
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use stock_trek_types::cex::cex_id::CexId;

#[derive(Default)]
pub(super) struct FakeSource {
    pub(super) records: Vec<SourceRecord>,
    pub(super) fail: bool,
    pub(super) acked: Arc<Mutex<usize>>,
}

#[async_trait]
impl MarketSource for FakeSource {
    async fn next_input(&mut self) -> BuildResult<Option<SourceRecord>> {
        if self.fail {
            return Err(BuildError::external("source failed"));
        }
        if self.records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.records.remove(0)))
        }
    }

    async fn ack(&mut self, _record: &SourceRecord) -> BuildResult<()> {
        *self.acked.lock().expect("acked lock") += 1;
        Ok(())
    }
}

#[derive(Default, Clone)]
pub(super) struct FakeSink {
    pub(super) open_updates: Arc<Mutex<usize>>,
    pub(super) closed_updates: Arc<Mutex<usize>>,
    pub(super) deleted_open_updates: Arc<Mutex<usize>>,
    pub(super) fail: bool,
}

impl FakeSink {
    pub(super) fn open_updates(&self) -> usize {
        *self.open_updates.lock().expect("open updates lock")
    }

    pub(super) fn closed_updates(&self) -> usize {
        *self.closed_updates.lock().expect("closed updates lock")
    }

    pub(super) fn deleted_open_updates(&self) -> usize {
        *self
            .deleted_open_updates
            .lock()
            .expect("deleted open updates lock")
    }
}

#[async_trait]
impl OpenCandleStore for FakeSink {
    async fn update_open(&self, candles: &[Candle]) -> BuildResult<()> {
        if self.fail {
            return Err(BuildError::bad_value("sink", "test", "write failed"));
        }
        *self.open_updates.lock().expect("open updates lock") += candles.len();
        Ok(())
    }

    async fn delete_open(&self, keys: &[CandleKey]) -> BuildResult<()> {
        if self.fail {
            return Err(BuildError::bad_value("sink", "test", "write failed"));
        }
        *self
            .deleted_open_updates
            .lock()
            .expect("deleted open updates lock") += keys.len();
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for FakeSink {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BuildResult<()> {
        if self.fail {
            return Err(BuildError::bad_value("sink", "test", "write failed"));
        }
        *self.closed_updates.lock().expect("closed updates lock") += candles.len();
        Ok(())
    }
}

pub(super) struct FailingCheckpointStore;

#[async_trait]
impl StateStore for FailingCheckpointStore {
    async fn load(&self, _shard: ShardId) -> BuildResult<Option<CheckpointRecord>> {
        Ok(None)
    }

    async fn save(&self, _checkpoint: &CheckpointRecord) -> BuildResult<()> {
        Err(BuildError::bad_value("checkpoint", "test", "write failed"))
    }
}

pub(super) struct SignalingSource {
    pub(super) records: Vec<SourceRecord>,
    pub(super) signal: QuiesceSignal,
    pub(super) polls: Arc<AtomicUsize>,
    pub(super) acked: Arc<Mutex<usize>>,
}

#[async_trait]
impl MarketSource for SignalingSource {
    async fn next_input(&mut self) -> BuildResult<Option<SourceRecord>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.records.is_empty() {
            return std::future::pending().await;
        }
        let record = self.records.remove(0);
        self.signal.quiesce();
        Ok(Some(record))
    }

    async fn ack(&mut self, _record: &SourceRecord) -> BuildResult<()> {
        *self.acked.lock().expect("acked lock") += 1;
        Ok(())
    }
}

pub(super) struct Fixtures;

impl Fixtures {
    pub(super) fn config() -> AggregatorConfig {
        AggregatorConfig {
            log_filter: "info".to_string(),
            nats_url: "nats://localhost:4222".to_string(),
            event_subject_prefix: "market.events".to_string(),
            gap_subject_prefix: "market.gaps".to_string(),
            open_candle_subject_prefix: "market.candles.open".to_string(),
            closed_candle_subject_prefix: "market.candles.closed".to_string(),
            shard_subject_prefix: "aggregator.shards".to_string(),
            shard_count: 1,
            shard_index: None,
            intervals: vec![AlignedWindow::try_from("1m").unwrap()],
            allowed_lateness: std::time::Duration::from_millis(5_000),
            max_closed_candles_per_interval: std::collections::BTreeMap::from([(
                AlignedWindow::try_from("1m").unwrap(),
                usize::MAX,
            )]),
            correction_cutoff: DurationMilliseconds(i64::MAX),
            closed_candle_memory_budget_bytes: usize::MAX,
            checkpoint_interval: std::time::Duration::from_secs(1),
            checkpoint_bucket: "aggregator_shards_checkpoints".to_string(),
            checkpoint_key_prefix: "shard".to_string(),
            watermark_idle_timeout: std::time::Duration::from_secs(60),
            replay_start_policy: crate::config::ReplayStartPolicy::Checkpoint,
            state_schema_version: crate::state::STATE_SCHEMA_VERSION,
            consumer_prefix: "aggregator".to_string(),
            lease_ttl: std::time::Duration::from_secs(30),
        }
    }

    pub(super) fn trade_event(timestamp_ms: i64) -> MarketEvent {
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

    pub(super) fn non_trade_event(timestamp_ms: i64) -> MarketEvent {
        MarketEvent {
            stream: "btcusdt@depth".to_string(),
            sequence: Some(2),
            payload: String::new(),
            ..Self::trade_event(timestamp_ms)
        }
    }

    pub(super) fn record(input: MarketInput, sequence: u64) -> SourceRecord {
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

    pub(super) fn closed_open_keys(
        update: &ProcessorUpdate,
        state: &AggregatorState,
    ) -> Vec<CandleKey> {
        crate::runner::output_writer::CandleOutputWriter::closed_open_keys(update, state)
    }
}
