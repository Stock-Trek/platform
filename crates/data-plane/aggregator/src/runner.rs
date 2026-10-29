use crate::{
    checkpoint::{CheckpointRecord, InMemoryStateStore, StateStore},
    config::AggregatorConfig,
    lease::{InMemoryLeaseStore, LeaseStore, WriterLease, new_writer_id},
    partition::PartitionId,
    processor::{CandleProcessor, ProcessorUpdate},
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::{MarketInput, MarketSource, SourceRecord},
    state::{AggregatorState, DeduplicationKey},
};
use platform_errors::{BpError, BpResult, ErrorType};
use platform_market_data::{
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::{GapMarker, MarketEvent},
};
use platform_metrics::AggregatorMetrics;
use platform_metrics::descriptions::{DECODE_KIND_TRADE, DROP_REASON_NO_DECODER, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_runtime::Runtime;
use platform_types::new_types::TimestampMilliseconds;
use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;
use std::time::Instant;
use stock_trek_types::cex::cex_id::CexId;
use tokio::sync::watch;
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

/// A cloneable handle used by a rebalance coordinator to quiesce a worker.
///
/// The worker stops polling its source once quiesced, checkpoints and
/// acknowledges everything it already accepted, and then releases its lease so
/// the next owner can take over the partition.
#[derive(Debug, Clone)]
pub struct QuiesceSignal {
    sender: watch::Sender<bool>,
}

impl Default for QuiesceSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl QuiesceSignal {
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(false);
        Self { sender }
    }

    /// Creates a signal together with a dedicated receiver.
    pub fn channel() -> (Self, QuiesceReceiver) {
        let signal = Self::new();
        let receiver = signal.receiver();
        (signal, receiver)
    }

    /// Requests that the worker stops accepting new inputs and drains.
    pub fn quiesce(&self) {
        self.sender.send_replace(true);
    }

    /// Alias for [`Self::quiesce`].
    pub fn request_quiesce(&self) {
        self.quiesce();
    }

    /// Alias for [`Self::quiesce`].
    pub fn request(&self) {
        self.quiesce();
    }

    /// Alias for [`Self::quiesce`].
    pub fn trigger(&self) {
        self.quiesce();
    }

    pub fn is_quiesced(&self) -> bool {
        *self.sender.borrow()
    }

    pub fn receiver(&self) -> QuiesceReceiver {
        QuiesceReceiver {
            receiver: self.sender.subscribe(),
        }
    }

    /// Alias for [`Self::receiver`].
    pub fn subscribe(&self) -> QuiesceReceiver {
        self.receiver()
    }

    /// Resolves once quiescence has been requested.
    pub async fn wait(&self) {
        self.receiver().wait().await;
    }
}

/// Receives a quiesce request from a [`QuiesceSignal`].
#[derive(Debug, Clone)]
pub struct QuiesceReceiver {
    receiver: watch::Receiver<bool>,
}

impl QuiesceReceiver {
    pub fn is_quiesced(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Resolves once quiescence has been requested.
    pub async fn wait(&mut self) {
        loop {
            if self.is_quiesced() {
                return;
            }
            if self.receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl From<QuiesceSignal> for QuiesceReceiver {
    fn from(signal: QuiesceSignal) -> Self {
        signal.receiver()
    }
}

impl From<&QuiesceSignal> for QuiesceReceiver {
    fn from(signal: &QuiesceSignal) -> Self {
        signal.receiver()
    }
}

impl From<Arc<QuiesceSignal>> for QuiesceReceiver {
    fn from(signal: Arc<QuiesceSignal>) -> Self {
        signal.receiver()
    }
}

impl From<watch::Receiver<bool>> for QuiesceReceiver {
    fn from(receiver: watch::Receiver<bool>) -> Self {
        Self { receiver }
    }
}

impl From<watch::Sender<bool>> for QuiesceReceiver {
    fn from(sender: watch::Sender<bool>) -> Self {
        Self {
            receiver: sender.subscribe(),
        }
    }
}

/// Alias used by rebalance coordinators.
pub type RebalanceSignal = QuiesceSignal;
/// Alias used by rebalance coordinators.
pub type RebalanceReceiver = QuiesceReceiver;

#[derive(Debug, Clone)]
struct CheckpointPosition {
    source_stream: String,
    next_sequence: u64,
}

struct CommitState {
    pending: Vec<SourceRecord>,
    last_record: Option<SourceRecord>,
    dirty: bool,
    last_checkpoint_at: Instant,
    resume_position: Option<CheckpointPosition>,
}

impl CommitState {
    fn new(resume_position: Option<CheckpointPosition>) -> Self {
        Self {
            pending: Vec::new(),
            last_record: None,
            dirty: false,
            last_checkpoint_at: Instant::now(),
            resume_position,
        }
    }
}

/// Processes one partition's ordered inputs.
///
/// The worker acquires a fenced writer lease, applies inputs through the pure
/// [`CandleProcessor`], publishes outputs, checkpoints the keyed state
/// periodically and at shutdown, and only then acknowledges the source records.
///
/// A coordinator can install a [`QuiesceSignal`] with
/// [`ProcessorRunner::with_quiesce_signal`]. The running worker then stops
/// polling for new inputs, drains and checkpoints accepted records, and releases
/// its lease before exiting so ownership can move to another writer.
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
    quiesce: Option<QuiesceReceiver>,
}

impl<S, K> ProcessorRunner<S, K>
where
    S: MarketSource,
    K: OpenCandleStore + ClosedCandlePublisher,
{
    pub fn new(config: AggregatorConfig, source: S, sink: K) -> Self {
        let state = AggregatorState::new_with_schema_version(config.state_schema_version);
        Self {
            config,
            source,
            sink,
            checkpoints: Arc::new(InMemoryStateStore::new()),
            leases: Arc::new(InMemoryLeaseStore::new()),
            partition: PartitionId::new(0),
            state,
            replay_watermark: None,
            writer_id: None,
            quiesce: None,
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

    /// Installs the signal a rebalance coordinator uses to drain this worker.
    pub fn with_quiesce_signal(mut self, signal: impl Into<QuiesceReceiver>) -> Self {
        self.quiesce = Some(signal.into());
        self
    }

    /// Alias for [`Self::with_quiesce_signal`].
    pub fn with_quiesce(self, signal: impl Into<QuiesceReceiver>) -> Self {
        self.with_quiesce_signal(signal)
    }

    /// Alias for [`Self::with_quiesce_signal`].
    pub fn with_rebalance_signal(self, signal: impl Into<QuiesceReceiver>) -> Self {
        self.with_quiesce_signal(signal)
    }

    /// Alias for [`Self::with_quiesce_signal`].
    pub fn with_rebalance(self, signal: impl Into<QuiesceReceiver>) -> Self {
        self.with_quiesce_signal(signal)
    }

    /// Installs an already-created quiesce receiver.
    pub fn with_quiesce_receiver(self, receiver: QuiesceReceiver) -> Self {
        self.with_quiesce_signal(receiver)
    }

    /// Alias for [`Self::with_quiesce_receiver`].
    pub fn with_rebalance_receiver(self, receiver: QuiesceReceiver) -> Self {
        self.with_quiesce_signal(receiver)
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
        let state = AggregatorState::new_with_schema_version(config.state_schema_version);
        Self {
            config,
            source,
            sink,
            checkpoints,
            leases: Arc::new(InMemoryLeaseStore::new()),
            partition,
            state,
            replay_watermark: None,
            writer_id: None,
            quiesce: None,
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
            quiesce,
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

        AggregatorMetrics::record_rebalance(partition.get(), "lease_acquired");
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

        let resume_position =
            Self::resume_position(&checkpoints, partition, replay_watermark, processor.state())
                .await;
        let mut commit_state = CommitState::new(resume_position);
        let result = Runtime::run_until_shutdown(Self::process_inputs(
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
            quiesce,
            &mut commit_state,
        ))
        .await;
        let commit_result = if result.is_ok() {
            Self::commit(
                &config,
                &mut source,
                &checkpoints,
                &leases,
                partition,
                &processor,
                &mut lease,
                &writer_id,
                &mut commit_state,
            )
            .await
        } else {
            Ok(())
        };
        let release_result = leases.release(&lease).await;

        result?;
        commit_result?;
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
        mut quiesce: Option<QuiesceReceiver>,
        state: &mut CommitState,
    ) -> BpResult<()> {
        let mut decoders: HashMap<CexId, SharedTradeDecoder> = HashMap::new();
        let mut checkpoint_ticker = time::interval(config.checkpoint_interval);
        checkpoint_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        checkpoint_ticker.tick().await;
        let mut idle_ticker = time::interval(config.watermark_idle_timeout);
        idle_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        idle_ticker.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = Self::wait_for_quiesce(quiesce.as_mut()) => {
                    AggregatorMetrics::record_rebalance(
                        partition.get(),
                        "quiesce_requested",
                    );
                    info!(
                        partition = %partition,
                        writer = %writer_id,
                        "quiesce requested; draining and checkpointing aggregator partition"
                    );
                    break;
                }
                maybe_record = source.next_input() => {
                    match maybe_record {
                        Ok(Some(record)) => {
                            let is_replay =
                                Self::is_replay(processor, replay_watermark, &record.input);
                            // Mark the offset before folding the input so any
                            // candles closed by this record derive their
                            // revision from the source offset that produced
                            // them.
                            if let Some(sequence) = record.message.stream_sequence {
                                processor.state_mut().mark_processed(sequence);
                                state.dirty = true;
                            }
                            if !is_replay {
                                let update = Self::process_input(
                                    processor,
                                    &mut decoders,
                                    &record.input,
                                );
                                Self::record_update_metrics(&update);
                                Self::publish_update(sink, &update).await?;
                                state.dirty = true;
                            } else {
                                AggregatorMetrics::record_replay_events(
                                    partition.get(),
                                    "live",
                                    1,
                                );
                            }
                            Self::record_watermark_lag(partition, processor);
                            idle_ticker.reset();
                            state.last_record = Some(record.clone());
                            state.pending.push(record);
                        }
                        Ok(None) => {
                            info!("aggregator input source ended; draining partition");
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
                        state,
                    )
                    .await?;
                    state.dirty = false;
                }
                _ = idle_ticker.tick() => {
                    let previous_watermark = processor.state().watermark;
                    let update = Self::flush_idle(processor);
                    Self::publish_update(sink, &update).await?;
                    if !update.is_empty()
                        || processor.state().watermark != previous_watermark
                    {
                        state.dirty = true;
                    }
                    Self::record_watermark_lag(partition, processor);
                    Self::commit(
                        config,
                        source,
                        checkpoints,
                        leases,
                        partition,
                        processor,
                        lease,
                        writer_id,
                        state,
                    )
                    .await?;
                    state.dirty = false;
                }
            }
        }
        Ok(())
    }

    async fn resume_position(
        checkpoints: &Arc<dyn StateStore>,
        partition: PartitionId,
        replay_watermark: Option<TimestampMilliseconds>,
        state: &AggregatorState,
    ) -> Option<CheckpointPosition> {
        if replay_watermark.is_some() {
            return None;
        }
        match checkpoints.load(partition).await {
            Ok(Some(checkpoint))
                if checkpoint.next_sequence > 0
                    && !checkpoint.source_stream.is_empty()
                    && checkpoint.state.last_processed_sequence
                        == state.last_processed_sequence
                    && checkpoint.state.watermark == state.watermark
                    && checkpoint.state.state_epoch == state.state_epoch =>
            {
                Some(CheckpointPosition {
                    source_stream: checkpoint.source_stream,
                    next_sequence: checkpoint.next_sequence,
                })
            }
            Ok(_) => None,
            Err(error) => {
                warn!(
                    %error,
                    partition = %partition,
                    "failed to load checkpoint position for idle checkpoints"
                );
                None
            }
        }
    }

    async fn wait_for_quiesce(quiesce: Option<&mut QuiesceReceiver>) {
        match quiesce {
            Some(receiver) => receiver.wait().await,
            None => std::future::pending().await,
        }
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
        state: &mut CommitState,
    ) -> BpResult<()> {
        let Some(renewed) = leases.renew(lease, config.lease_ttl).await? else {
            return Err(BpError::bad_value(
                "aggregator lease",
                partition.to_string(),
                format!("writer {writer_id} lost its fencing lease"),
            ));
        };
        *lease = renewed;

        AggregatorMetrics::record_checkpoint_age(
            partition.get(),
            state.last_checkpoint_at.elapsed(),
        );
        if !state.dirty && state.pending.is_empty() {
            return Ok(());
        }

        let checkpoint = match state.last_record.as_ref() {
            Some(record) => {
                let Some(checkpoint) = CheckpointRecord::from_message(
                    &record.message,
                    partition,
                    processor.state().clone(),
                )?
                else {
                    return Err(BpError::bad_value(
                        "aggregator checkpoint",
                        partition.to_string(),
                        "source record has no JetStream sequence to checkpoint",
                    ));
                };
                checkpoint
            }
            None => {
                let Some(position) = state.resume_position.as_ref() else {
                    if state.pending.is_empty() {
                        return Ok(());
                    }
                    return Err(BpError::bad_value(
                        "aggregator checkpoint",
                        partition.to_string(),
                        "cannot checkpoint without a source record",
                    ));
                };
                CheckpointRecord::new(
                    partition,
                    position.source_stream.clone(),
                    position.next_sequence,
                    processor.state().clone(),
                )
            }
        };
        if let Err(error) = checkpoints.save(&checkpoint.with_lease(lease)).await {
            AggregatorMetrics::record_checkpoint_failure(partition.get(), error.error_type());
            return Err(error);
        }
        state.last_checkpoint_at = Instant::now();
        AggregatorMetrics::record_checkpoint_age(
            partition.get(),
            state.last_checkpoint_at.elapsed(),
        );

        for record in state.pending.drain(..) {
            source.ack(&record).await?;
        }
        Ok(())
    }

    fn is_replay(
        processor: &CandleProcessor,
        replay_watermark: Option<TimestampMilliseconds>,
        input: &MarketInput,
    ) -> bool {
        let Some(watermark) = replay_watermark else {
            return false;
        };
        if input_timestamp(input) > watermark {
            return false;
        }
        match input {
            MarketInput::Event(event) => event.sequence.is_none_or(|sequence| {
                processor.state().is_duplicate(&DeduplicationKey::new(
                    event.exchange,
                    &event.stream,
                    sequence,
                ))
            }),
            MarketInput::Gap(_) => true,
        }
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

    fn record_update_metrics(update: &ProcessorUpdate) {
        AggregatorMetrics::record_duplicate_events(update.duplicates as u64);
        AggregatorMetrics::record_incomplete_windows(update.incomplete_windows.len() as u64);
    }

    fn record_watermark_lag(partition: PartitionId, processor: &CandleProcessor) {
        let Some(watermark) = processor.state().watermark else {
            return;
        };
        let lag = TimestampMilliseconds::now() - watermark;
        AggregatorMetrics::record_watermark_lag(partition.get(), lag.0.max(0) as f64 / 1_000.0);
    }

    fn flush_idle(processor: &mut CandleProcessor) -> ProcessorUpdate {
        if processor.state().watermark.is_none() {
            return ProcessorUpdate::default();
        }
        let update = ProcessorUpdate {
            closed: processor.flush_closed_at_or_before(TimestampMilliseconds::now()),
            ..ProcessorUpdate::default()
        };
        Self::record_update_metrics(&update);
        update
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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
            checkpoint_bucket: None,
            checkpoint_key_prefix: "partition".to_string(),
            watermark_idle_timeout: std::time::Duration::from_secs(60),
            replay_start_policy: crate::config::ReplayStartPolicy::Checkpoint,
            state_schema_version: crate::state::STATE_SCHEMA_VERSION,
            consumer_prefix: "aggregator".to_string(),
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

    struct SignalingSource {
        records: Vec<SourceRecord>,
        signal: QuiesceSignal,
        polls: Arc<AtomicUsize>,
        acked: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl MarketSource for SignalingSource {
        async fn next_input(&mut self) -> BpResult<Option<SourceRecord>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if self.records.is_empty() {
                return std::future::pending().await;
            }
            let record = self.records.remove(0);
            self.signal.quiesce();
            Ok(Some(record))
        }

        async fn ack(&mut self, _record: &SourceRecord) -> BpResult<()> {
            *self.acked.lock().expect("acked lock") += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn quiesce_stops_new_inputs_and_drains_accepted_records() {
        let sink = FakeSink::default();
        let observed = sink.clone();
        let signal = QuiesceSignal::new();
        let polls = Arc::new(AtomicUsize::new(0));
        let acked = Arc::new(Mutex::new(0));
        let source = SignalingSource {
            records: vec![
                record(MarketInput::Event(trade_event(61_000)), 1),
                record(MarketInput::Event(trade_event(62_000)), 2),
            ],
            signal: signal.clone(),
            polls: polls.clone(),
            acked: acked.clone(),
        };
        let checkpoints = Arc::new(InMemoryStateStore::new());
        let leases = Arc::new(InMemoryLeaseStore::new());
        let runner = ProcessorRunner::new(config(), source, sink)
            .with_checkpoint_store(checkpoints.clone(), PartitionId::new(0))
            .with_lease_store(leases.clone())
            .with_writer_id("quiescing-writer")
            .with_quiesce_signal(signal);

        tokio::time::timeout(std::time::Duration::from_secs(1), runner.run())
            .await
            .expect("runner should stop after quiescing")
            .unwrap();

        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.open_updates(), 1);
        assert_eq!(*acked.lock().unwrap(), 1);
        let checkpoint = checkpoints.get(PartitionId::new(0)).unwrap();
        assert_eq!(checkpoint.next_sequence, 2);
        assert_eq!(checkpoint.state.last_processed_sequence, Some(1));
        let lease = leases.current(PartitionId::new(0)).await.unwrap().unwrap();
        assert!(lease.is_expired(TimestampMilliseconds::now()));
    }

    #[tokio::test]
    async fn an_already_quiesced_worker_does_not_poll_its_source() {
        let signal = QuiesceSignal::new();
        signal.quiesce();
        let polls = Arc::new(AtomicUsize::new(0));
        let acked = Arc::new(Mutex::new(0));
        let source = SignalingSource {
            records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
            signal: signal.clone(),
            polls: polls.clone(),
            acked: acked.clone(),
        };
        let runner =
            ProcessorRunner::new(config(), source, FakeSink::default()).with_quiesce_signal(signal);

        tokio::time::timeout(std::time::Duration::from_secs(1), runner.run())
            .await
            .expect("runner should stop immediately")
            .unwrap();

        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(*acked.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn a_watch_receiver_can_trigger_quiescence() {
        let (sender, receiver) = watch::channel(false);
        let polls = Arc::new(AtomicUsize::new(0));
        let acked = Arc::new(Mutex::new(0));
        let source = SignalingSource {
            records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
            signal: QuiesceSignal::new(),
            polls: polls.clone(),
            acked: acked.clone(),
        };
        let runner = ProcessorRunner::new(config(), source, FakeSink::default())
            .with_rebalance_signal(receiver);
        sender.send_replace(true);

        tokio::time::timeout(std::time::Duration::from_secs(1), runner.run())
            .await
            .expect("runner should stop after quiescing")
            .unwrap();

        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(*acked.lock().unwrap(), 0);
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

        // The trade publishes the open candle, then the gap republishes it
        // with the completeness flag cleared before the window closes.
        assert_eq!(observed.open_updates(), 2);
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
        restored_state.observe_sequence(&DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 1));

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
    async fn idle_flush_after_restore_does_not_fail_without_a_source_record() {
        let partition = PartitionId::new(0);
        let signal = QuiesceSignal::new();
        let trigger = signal.clone();
        let source = SignalingSource {
            records: Vec::new(),
            signal: signal.clone(),
            polls: Arc::new(AtomicUsize::new(0)),
            acked: Arc::new(Mutex::new(0)),
        };
        let mut config = config();
        config.watermark_idle_timeout = std::time::Duration::from_millis(10);
        let mut restored_state = AggregatorState::new();
        restored_state.advance_watermark(TimestampMilliseconds(61_000));
        let checkpoints = Arc::new(InMemoryStateStore::new());
        let runner = ProcessorRunner::new(config, source, FakeSink::default())
            .with_checkpoint_store(checkpoints.clone(), partition)
            .with_state(restored_state)
            .with_quiesce_signal(signal);

        let handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        trigger.quiesce();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("idle flush should not stop the runner")
            .expect("runner task should not panic")
            .unwrap();

        assert!(checkpoints.get(partition).is_none());
    }

    #[tokio::test]
    async fn idle_flush_after_checkpoint_restore_preserves_the_source_position() {
        let partition = PartitionId::new(0);
        let signal = QuiesceSignal::new();
        let trigger = signal.clone();
        let source = SignalingSource {
            records: Vec::new(),
            signal: signal.clone(),
            polls: Arc::new(AtomicUsize::new(0)),
            acked: Arc::new(Mutex::new(0)),
        };
        let mut restored_state = AggregatorState::new();
        restored_state.advance_watermark(TimestampMilliseconds(61_000));
        let checkpoints = Arc::new(InMemoryStateStore::new());
        let restored_checkpoint =
            CheckpointRecord::new(partition, "aggregator-shards", 42, restored_state.clone());
        checkpoints.save(&restored_checkpoint).await.unwrap();
        let mut config = config();
        config.watermark_idle_timeout = std::time::Duration::from_millis(10);
        let runner = ProcessorRunner::new(config, source, FakeSink::default())
            .with_checkpoint_store(checkpoints.clone(), partition)
            .with_state(restored_state)
            .with_quiesce_signal(signal);

        let handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        trigger.quiesce();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("idle flush should not stop the runner")
            .expect("runner task should not panic")
            .unwrap();

        let checkpoint = checkpoints.get(partition).unwrap();
        assert_eq!(checkpoint.next_sequence, 42);
        assert_eq!(checkpoint.source_stream, "aggregator-shards");
        assert!(
            checkpoint.watermark.unwrap() > TimestampMilliseconds(61_000),
            "{checkpoint:?}"
        );
    }

    #[tokio::test]
    async fn idle_flush_after_archive_restore_does_not_reuse_a_stale_checkpoint_position() {
        let partition = PartitionId::new(0);
        let signal = QuiesceSignal::new();
        let trigger = signal.clone();
        let source = SignalingSource {
            records: Vec::new(),
            signal: signal.clone(),
            polls: Arc::new(AtomicUsize::new(0)),
            acked: Arc::new(Mutex::new(0)),
        };
        let mut restored_state = AggregatorState::new();
        restored_state.advance_watermark(TimestampMilliseconds(61_000));
        let checkpoints = Arc::new(InMemoryStateStore::new());
        let stale_checkpoint =
            CheckpointRecord::new(partition, "aggregator-shards", 99, AggregatorState::new());
        checkpoints.save(&stale_checkpoint).await.unwrap();
        let mut config = config();
        config.watermark_idle_timeout = std::time::Duration::from_millis(10);
        let runner = ProcessorRunner::new(config, source, FakeSink::default())
            .with_checkpoint_store(checkpoints.clone(), partition)
            .with_state(restored_state)
            .with_replay_watermark(Some(TimestampMilliseconds(61_000)))
            .with_quiesce_signal(signal);

        let handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        trigger.quiesce();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("idle flush should not stop the runner")
            .expect("runner task should not panic")
            .unwrap();

        assert_eq!(checkpoints.get(partition).unwrap(), stale_checkpoint);
    }

    #[tokio::test]
    async fn restored_partitions_apply_new_backfills_older_than_the_replay_watermark() {
        let sink = FakeSink::default();
        let observed = sink.clone();
        let acked = Arc::new(Mutex::new(0));
        let mut backfill = trade_event(61_000);
        backfill.sequence = Some(2);
        let source = FakeSource {
            records: vec![record(MarketInput::Event(backfill), 2)],
            acked: acked.clone(),
            ..FakeSource::default()
        };
        let checkpoints = Arc::new(InMemoryStateStore::new());
        let mut restored_state = AggregatorState::new();
        restored_state.advance_watermark(TimestampMilliseconds(121_000));
        restored_state.observe_sequence(&DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 1));

        ProcessorRunner::new(config(), source, sink)
            .with_checkpoint_store(checkpoints.clone(), PartitionId::new(0))
            .with_state(restored_state)
            .with_replay_watermark(Some(TimestampMilliseconds(121_000)))
            .run()
            .await
            .unwrap();

        assert_eq!(observed.open_updates(), 1);
        assert_eq!(*acked.lock().unwrap(), 1);
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
