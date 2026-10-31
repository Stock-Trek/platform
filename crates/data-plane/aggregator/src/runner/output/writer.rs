//! Asynchronous candle sink writes.
//!
//! Open candles and their tombstones are a derived serving projection, so a
//! single writer task can coalesce them and only write the final value per
//! [`CandleKey`]. Closed candles are durable outputs and are never coalesced
//! away. The task is fed by a bounded channel and exposes a flush barrier that
//! completes only after every queued closed candle has been published, allowing
//! the source checkpoint to advance safely.
//!
//! The writer is drained before a shard is quiesced or its lease released.

use crate::{
    processor::ProcessorUpdate,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    state::AggregatorState,
};
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::{Candle, CandleKey, ClosedCandle};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};

/// Maximum number of updates waiting for the projection writer.
pub(super) const DEFAULT_CHANNEL_CAPACITY: usize = 1_024;
/// How long an open-candle write may wait for more updates to coalesce.
pub(super) const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
/// Maximum number of queued open mutations and closed candles before a flush.
pub(super) const DEFAULT_MAX_PENDING: usize = 4_096;

#[derive(Debug)]
enum OpenMutation {
    Put(Candle),
    Delete,
}

#[derive(Debug)]
enum OutputCommand {
    Update {
        closed: Vec<ClosedCandle>,
        open: Vec<(CandleKey, OpenMutation)>,
    },
    /// Publishes the closed candles queued before this command, then
    /// acknowledges the barrier. Open projections may still be pending.
    Barrier {
        reply: oneshot::Sender<BpResult<()>>,
    },
    /// Publishes everything queued before this command, then acknowledges the
    /// barrier. This is used before quiescing or releasing the lease.
    Flush {
        reply: oneshot::Sender<BpResult<()>>,
    },
    /// Flushes and stops the writer task.
    Shutdown {
        reply: oneshot::Sender<BpResult<()>>,
    },
}

/// Handle used by the hot loop to submit outputs to the projection writer.
#[derive(Debug)]
pub(crate) struct CandleOutputWriter {
    sender: mpsc::Sender<OutputCommand>,
    task: JoinHandle<()>,
}

impl CandleOutputWriter {
    pub(crate) fn start<K>(sink: K) -> Self
    where
        K: OpenCandleStore + ClosedCandlePublisher + 'static,
    {
        Self::start_with_limits(
            sink,
            DEFAULT_CHANNEL_CAPACITY,
            DEFAULT_FLUSH_INTERVAL,
            DEFAULT_MAX_PENDING,
        )
    }

    pub(crate) fn start_with_limits<K>(
        sink: K,
        channel_capacity: usize,
        flush_interval: Duration,
        max_pending: usize,
    ) -> Self
    where
        K: OpenCandleStore + ClosedCandlePublisher + 'static,
    {
        let (sender, receiver) = mpsc::channel(channel_capacity.max(1));
        let pending = PendingWrites::with_limits(flush_interval, max_pending);
        let task = tokio::spawn(Self::run_writer(sink, receiver, pending));
        Self { sender, task }
    }

    /// Queues one processor update without waiting for the open-candle write to
    /// complete. The writer task coalesces repeated writes for the same key.
    pub(crate) async fn enqueue(
        &self,
        update: ProcessorUpdate,
        state: &AggregatorState,
    ) -> BpResult<()> {
        let deletes = Self::closed_open_keys(&update, state);
        let mut open = Vec::with_capacity(update.open.len() + deletes.len());
        for candle in update.open {
            open.push((candle.key.clone(), OpenMutation::Put(candle)));
        }
        // Apply tombstones after puts. `closed_open_keys` only contains keys
        // with no open window in the post-update state, so an open value for
        // one of those keys is a stale intermediate that must not resurrect the
        // deleted projection entry.
        for key in deletes {
            open.push((key, OpenMutation::Delete));
        }

        if update.closed.is_empty() && open.is_empty() {
            return Ok(());
        }

        self.send(OutputCommand::Update {
            closed: update.closed,
            open,
        })
        .await
    }

    /// Waits until every closed candle submitted before this call has been
    /// published.
    ///
    /// Pending open projections do not delay the barrier: they are derived
    /// data and may continue coalescing after the source checkpoint advances.
    pub(crate) async fn barrier(&self) -> BpResult<()> {
        let (reply, receiver) = oneshot::channel();
        self.send(OutputCommand::Barrier { reply }).await?;
        receiver.await.map_err(|_| Self::writer_stopped())?
    }

    /// Waits until every update submitted before this call has been written.
    ///
    /// This drains both closed candles and the open projection and is used
    /// before quiescing or releasing the shard lease.
    pub(crate) async fn flush(&self) -> BpResult<()> {
        let (reply, receiver) = oneshot::channel();
        self.send(OutputCommand::Flush { reply }).await?;
        receiver.await.map_err(|_| Self::writer_stopped())?
    }

    /// Flushes the remaining outputs and waits for the writer task to stop.
    ///
    /// This must complete before the shard lease is released.
    pub(crate) async fn shutdown(self) -> BpResult<()> {
        let (reply, receiver) = oneshot::channel();
        let result = match self.sender.send(OutputCommand::Shutdown { reply }).await {
            Ok(()) => receiver.await.map_err(|_| Self::writer_stopped())?,
            Err(_) => Err(Self::writer_stopped()),
        };

        match self.task.await {
            Ok(()) => result,
            Err(error) => Err(BpError::bad_value(
                "aggregator output writer",
                "task",
                error.to_string(),
            )),
        }
    }

    async fn send(&self, command: OutputCommand) -> BpResult<()> {
        self.sender
            .send(command)
            .await
            .map_err(|_| Self::writer_stopped())
    }
}

impl CandleOutputWriter {
    fn writer_stopped() -> BpError {
        BpError::bad_value(
            "aggregator output writer",
            "channel",
            "projection writer stopped before acknowledging the output",
        )
    }
}

impl CandleOutputWriter {
    async fn run_writer<K>(
        sink: K,
        mut receiver: mpsc::Receiver<OutputCommand>,
        mut pending: PendingWrites,
    ) where
        K: OpenCandleStore + ClosedCandlePublisher,
    {
        let mut ticker = time::interval(pending.flush_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;

        loop {
            tokio::select! {
                biased;
                command = receiver.recv() => {
                    match command {
                        Some(OutputCommand::Update { closed, open }) => {
                            pending.push(closed, open);
                            if pending.pending_len() >= pending.max_pending {
                                // A transient failure leaves the batch queued. The
                                // next timer tick or confirmation barrier retries
                                // with the same deterministic output ids.
                                let _ = pending.flush(&sink).await;
                            }
                        }
                        Some(OutputCommand::Barrier { reply }) => {
                            let result = pending.publish_closed(&sink).await;
                            let _ = reply.send(result);
                        }
                        Some(OutputCommand::Flush { reply }) => {
                            let result = pending.flush(&sink).await;
                            let _ = reply.send(result);
                        }
                        Some(OutputCommand::Shutdown { reply }) => {
                            let result = pending.flush(&sink).await;
                            let _ = reply.send(result);
                            break;
                        }
                        None => {
                            // The handle was dropped without an explicit shutdown.
                            // Still make a best effort to drain accepted outputs.
                            let _ = pending.flush(&sink).await;
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if pending.should_flush(Instant::now()) {
                        let _ = pending.flush(&sink).await;
                    }
                }
            }
        }
    }
}

/// Buffers sink writes so stale open projections can be dropped.
#[derive(Debug)]
struct PendingWrites {
    open: HashMap<CandleKey, OpenMutation>,
    closed: Vec<ClosedCandle>,
    last_flush: Instant,
    flush_interval: Duration,
    max_pending: usize,
}

impl PendingWrites {
    fn with_limits(flush_interval: Duration, max_pending: usize) -> Self {
        Self {
            open: HashMap::new(),
            closed: Vec::new(),
            last_flush: Instant::now(),
            flush_interval,
            max_pending: max_pending.max(1),
        }
    }

    fn is_empty(&self) -> bool {
        self.open.is_empty() && self.closed.is_empty()
    }

    fn pending_len(&self) -> usize {
        self.open.len() + self.closed.len()
    }

    fn should_flush(&self, now: Instant) -> bool {
        !self.is_empty() && now.saturating_duration_since(self.last_flush) >= self.flush_interval
    }

    fn push(&mut self, closed: Vec<ClosedCandle>, open: Vec<(CandleKey, OpenMutation)>) {
        self.closed.extend(closed);
        for (key, mutation) in open {
            // Only the latest value for an open projection key is observable.
            self.open.insert(key, mutation);
        }
    }

    /// Publishes closed candles first, then flushes the open projection.
    ///
    /// Closed candles are the durable, revision-bearing output and must be
    /// acknowledged before a checkpoint can advance past their inputs.
    async fn flush<K>(&mut self, sink: &K) -> BpResult<()>
    where
        K: OpenCandleStore + ClosedCandlePublisher + ?Sized,
    {
        if self.is_empty() {
            self.last_flush = Instant::now();
            return Ok(());
        }

        self.publish_closed(sink).await?;
        self.flush_open(sink).await?;
        self.last_flush = Instant::now();
        Ok(())
    }

    /// Publishes only closed candles, leaving open projections pending.
    ///
    /// If publication fails, the batch is retained so a retry republishes the
    /// same deterministic closed-candle message ids.
    async fn publish_closed<K>(&mut self, sink: &K) -> BpResult<()>
    where
        K: ClosedCandlePublisher + ?Sized,
    {
        if self.closed.is_empty() {
            return Ok(());
        }

        let closed = std::mem::take(&mut self.closed);
        match sink.publish_closed(&closed).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.closed = closed;
                Err(error)
            }
        }
    }

    async fn flush_open<K>(&mut self, sink: &K) -> BpResult<()>
    where
        K: OpenCandleStore + ?Sized,
    {
        if self.open.is_empty() {
            return Ok(());
        }

        let mutations = std::mem::take(&mut self.open);
        let mut deletes = Vec::new();
        let mut opens = Vec::new();
        for (key, mutation) in mutations {
            match mutation {
                OpenMutation::Put(candle) => opens.push(candle),
                OpenMutation::Delete => deletes.push(key),
            }
        }
        deletes.sort_by_key(CandleKey::subject_suffix);
        opens.sort_by_key(|candle| candle.key.subject_suffix());

        let result = async {
            if !deletes.is_empty() {
                sink.delete_open(&deletes).await?;
            }
            if !opens.is_empty() {
                sink.update_open(&opens).await?;
            }
            Ok(())
        }
        .await;

        if let Err(error) = result {
            for key in deletes {
                self.open.insert(key, OpenMutation::Delete);
            }
            for candle in opens {
                self.open
                    .insert(candle.key.clone(), OpenMutation::Put(candle));
            }
            return Err(error);
        }
        Ok(())
    }
}

/// Open-candle keys whose windows closed and that have no newer open window.
impl CandleOutputWriter {
    pub(crate) fn closed_open_keys(
        update: &ProcessorUpdate,
        state: &AggregatorState,
    ) -> Vec<CandleKey> {
        let mut keys = update
            .closed
            .iter()
            .map(|closed| &closed.candle().key)
            .filter(|key| state.open_candle(key).is_none())
            .cloned()
            .collect::<Vec<_>>();
        keys.sort_by_key(CandleKey::subject_suffix);
        keys.dedup();
        keys
    }
}
