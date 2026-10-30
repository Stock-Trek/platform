//! Coalesced candle sink writes.
//!
//! Open candles and their tombstones are a derived serving projection, so
//! writes can be accumulated for a short window and only the final value per
//! [`CandleKey`] needs to be written. Closed candles are durable outputs and
//! are never coalesced away. The buffer is flushed before a checkpoint is
//! saved (and therefore before source records are acknowledged), and on
//! quiesce or shutdown.

use crate::{
    processor::ProcessorUpdate,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    state::AggregatorState,
};
use platform_errors::BpResult;
use platform_market_data::candle::{Candle, CandleKey, ClosedCandle};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// How long an open-candle write may wait for more updates to coalesce.
pub(super) const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
/// Maximum number of queued open mutations and closed candles before a flush.
pub(super) const DEFAULT_MAX_PENDING: usize = 4_096;

#[derive(Debug)]
enum OpenMutation {
    Put(Candle),
    Delete,
}

/// Buffers sink writes so the hot loop can batch and coalesce them.
#[derive(Debug)]
pub(super) struct CandleOutputBuffer {
    open: HashMap<CandleKey, OpenMutation>,
    closed: Vec<ClosedCandle>,
    last_flush: Instant,
    flush_interval: Duration,
    max_pending: usize,
}

impl CandleOutputBuffer {
    pub(super) fn new() -> Self {
        Self::with_limits(DEFAULT_FLUSH_INTERVAL, DEFAULT_MAX_PENDING)
    }

    pub(super) fn with_limits(flush_interval: Duration, max_pending: usize) -> Self {
        Self {
            open: HashMap::new(),
            closed: Vec::new(),
            last_flush: Instant::now(),
            flush_interval,
            max_pending: max_pending.max(1),
        }
    }

    pub(super) fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    pub(super) fn is_empty(&self) -> bool {
        self.open.is_empty() && self.closed.is_empty()
    }

    pub(super) fn pending_len(&self) -> usize {
        self.open.len() + self.closed.len()
    }

    pub(super) fn should_flush(&self, now: Instant) -> bool {
        !self.is_empty()
            && (self.pending_len() >= self.max_pending
                || now.saturating_duration_since(self.last_flush) >= self.flush_interval)
    }

    /// Queues one processor update, flushing first when the update would
    /// exceed the configured bound.
    pub(super) async fn enqueue<K>(
        &mut self,
        sink: &K,
        update: &ProcessorUpdate,
        state: &AggregatorState,
    ) -> BpResult<()>
    where
        K: OpenCandleStore + ClosedCandlePublisher + ?Sized,
    {
        let deletes = closed_open_keys(update, state);
        let incoming = update
            .closed
            .len()
            .saturating_add(deletes.len())
            .saturating_add(update.open.len());

        // Flush the previous batch before applying this update. This keeps
        // per-key ordering across flush boundaries: an older open value can
        // never be written after a newer one from this update.
        if !self.is_empty() && self.pending_len().saturating_add(incoming) > self.max_pending {
            self.flush(sink).await?;
        }

        self.closed.extend(update.closed.iter().cloned());
        // Apply puts before tombstones. `closed_open_keys` only contains keys
        // with no open window in the post-update state, so an open value for
        // one of those keys is a stale intermediate that must not resurrect the
        // deleted projection entry.
        for candle in &update.open {
            self.open
                .insert(candle.key.clone(), OpenMutation::Put(candle.clone()));
        }
        for key in deletes {
            self.open.insert(key, OpenMutation::Delete);
        }

        if self.should_flush(Instant::now()) {
            self.flush(sink).await?;
        }
        Ok(())
    }

    /// Publishes closed candles, then flushes the final open projection.
    ///
    /// Closed candles are published first and awaited so the durable output is
    /// stored before the caller can advance its checkpoint. If a flush fails,
    /// the batch is retained so a retry republishes the same deterministic
    /// closed-candle message ids and idempotent KV mutations.
    pub(super) async fn flush<K>(&mut self, sink: &K) -> BpResult<()>
    where
        K: OpenCandleStore + ClosedCandlePublisher + ?Sized,
    {
        if self.is_empty() {
            self.last_flush = Instant::now();
            return Ok(());
        }

        let closed = std::mem::take(&mut self.closed);
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
            if !closed.is_empty() {
                sink.publish_closed(&closed).await?;
            }
            if !deletes.is_empty() {
                sink.delete_open(&deletes).await?;
            }
            if !opens.is_empty() {
                sink.update_open(&opens).await?;
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                self.last_flush = Instant::now();
                Ok(())
            }
            Err(error) => {
                self.closed = closed;
                for key in deletes {
                    self.open.insert(key, OpenMutation::Delete);
                }
                for candle in opens {
                    self.open
                        .insert(candle.key.clone(), OpenMutation::Put(candle));
                }
                Err(error)
            }
        }
    }
}

/// Open-candle keys whose windows closed and that have no newer open window.
pub(super) fn closed_open_keys(
    update: &ProcessorUpdate,
    state: &AggregatorState,
) -> Vec<CandleKey> {
    let mut keys = update
        .closed
        .iter()
        .map(|closed| &closed.candle.key)
        .filter(|key| state.open_candle(key).is_none())
        .cloned()
        .collect::<Vec<_>>();
    keys.sort_by_key(CandleKey::subject_suffix);
    keys.dedup();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::{ClosedCandlePublisher, OpenCandleStore};
    use async_trait::async_trait;
    use platform_market_data::candle::CandleInterval;
    use platform_types::new_types::TimestampMilliseconds;
    use std::sync::{Arc, Mutex};
    use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

    #[derive(Default, Clone)]
    struct RecordingSink {
        calls: Arc<Mutex<Vec<&'static str>>>,
        closed: Arc<Mutex<Vec<ClosedCandle>>>,
        open: Arc<Mutex<Vec<Candle>>>,
        deleted: Arc<Mutex<Vec<CandleKey>>>,
    }

    impl RecordingSink {
        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl OpenCandleStore for RecordingSink {
        async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
            self.calls.lock().unwrap().push("open");
            self.open.lock().unwrap().extend_from_slice(candles);
            Ok(())
        }

        async fn delete_open(&self, keys: &[CandleKey]) -> BpResult<()> {
            self.calls.lock().unwrap().push("delete");
            self.deleted.lock().unwrap().extend_from_slice(keys);
            Ok(())
        }
    }

    #[async_trait]
    impl ClosedCandlePublisher for RecordingSink {
        async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
            self.calls.lock().unwrap().push("closed");
            self.closed.lock().unwrap().extend_from_slice(candles);
            Ok(())
        }
    }

    fn key(interval: &str) -> CandleKey {
        CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            CandleInterval::from_label(interval).unwrap(),
        )
    }

    fn candle(interval: &str, close: f64) -> Candle {
        use platform_market_data::decoders::trade::Trade;
        use rust_decimal::Decimal;

        let trade = Trade {
            exchange: CexId::Binance,
            trading_pair: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            price: Decimal::from_str_exact(&close.to_string()).unwrap(),
            quantity: Decimal::ONE,
            timestamp: TimestampMilliseconds(61_000),
        };
        Candle::open(&trade, CandleInterval::from_label(interval).unwrap())
    }

    fn closed(candle: Candle) -> ClosedCandle {
        ClosedCandle::new(candle, TimestampMilliseconds(121_000), 1, 0)
    }

    #[tokio::test]
    async fn open_mutations_are_coalesced_to_the_last_value_per_key() {
        let sink = RecordingSink::default();
        let mut buffer = CandleOutputBuffer::with_limits(Duration::from_secs(60), 100);
        let state = AggregatorState::new();

        buffer
            .enqueue(
                &sink,
                &ProcessorUpdate {
                    open: vec![candle("1m", 100.0)],
                    ..ProcessorUpdate::default()
                },
                &state,
            )
            .await
            .unwrap();
        buffer
            .enqueue(
                &sink,
                &ProcessorUpdate {
                    open: vec![candle("1m", 105.0)],
                    ..ProcessorUpdate::default()
                },
                &state,
            )
            .await
            .unwrap();

        assert!(sink.calls().is_empty());
        buffer.flush(&sink).await.unwrap();

        assert_eq!(sink.calls(), vec!["open"]);
        let open = sink.open.lock().unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].market_candle.ohlcv.close, 105.0);
    }

    #[tokio::test]
    async fn closed_candles_are_published_before_open_mutations() {
        let sink = RecordingSink::default();
        let mut buffer = CandleOutputBuffer::with_limits(Duration::from_secs(60), 100);

        buffer
            .enqueue(
                &sink,
                &ProcessorUpdate {
                    closed: vec![closed(candle("1m", 99.0))],
                    ..ProcessorUpdate::default()
                },
                &AggregatorState::new(),
            )
            .await
            .unwrap();
        buffer.flush(&sink).await.unwrap();

        assert_eq!(sink.calls(), vec!["closed", "delete"]);
        assert_eq!(sink.closed.lock().unwrap().len(), 1);
        assert_eq!(*sink.deleted.lock().unwrap(), vec![key("1m")]);

        let sink = RecordingSink::default();
        buffer
            .enqueue(
                &sink,
                &ProcessorUpdate {
                    open: vec![candle("1m", 100.0)],
                    ..ProcessorUpdate::default()
                },
                &AggregatorState::new(),
            )
            .await
            .unwrap();
        buffer.flush(&sink).await.unwrap();
        assert_eq!(sink.calls(), vec!["open"]);
    }

    #[test]
    fn time_bound_requests_a_flush() {
        let mut buffer = CandleOutputBuffer::with_limits(Duration::from_millis(10), 100);
        buffer
            .open
            .insert(key("1m"), OpenMutation::Put(candle("1m", 100.0)));

        assert!(!buffer.should_flush(Instant::now()));
        assert!(buffer.should_flush(Instant::now() + Duration::from_millis(11)));
    }

    #[tokio::test]
    async fn size_bound_flushes_without_waiting_for_the_timer() {
        let sink = RecordingSink::default();
        let mut buffer = CandleOutputBuffer::with_limits(Duration::from_secs(60), 2);

        buffer
            .enqueue(
                &sink,
                &ProcessorUpdate {
                    open: vec![candle("1m", 100.0)],
                    ..ProcessorUpdate::default()
                },
                &AggregatorState::new(),
            )
            .await
            .unwrap();
        buffer
            .enqueue(
                &sink,
                &ProcessorUpdate {
                    open: vec![candle("5m", 100.0)],
                    ..ProcessorUpdate::default()
                },
                &AggregatorState::new(),
            )
            .await
            .unwrap();

        assert_eq!(sink.calls(), vec!["open"]);
        assert!(buffer.is_empty());
    }
}
