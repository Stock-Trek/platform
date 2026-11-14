use crate::{
    candle_sink::{ClosedCandlePublisher, OpenCandleStore},
    processor::ProcessorUpdate,
    runner::output_writer::CandleOutputWriter,
    state::AggregatorState,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{PlatformError, PlatformResult};
use platform_market_data::{AlignedWindow, Candle, CandleKey, ClosedCandle, Trade};
use platform_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};
use tokio::sync::Notify;

#[derive(Default, Clone)]
struct RecordingSink {
    calls: Arc<Mutex<Vec<&'static str>>>,
    closed: Arc<Mutex<Vec<ClosedCandle>>>,
    open: Arc<Mutex<Vec<Candle>>>,
    deleted: Arc<Mutex<Vec<CandleKey>>>,
}

impl RecordingSink {
    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().clone()
    }
}

#[async_trait]
impl OpenCandleStore for RecordingSink {
    async fn update_open(&self, candles: &[Candle]) -> PlatformResult<()> {
        self.calls.lock().push("open");
        self.open.lock().extend_from_slice(candles);
        Ok(())
    }

    async fn delete_open(&self, keys: &[CandleKey]) -> PlatformResult<()> {
        self.calls.lock().push("delete");
        self.deleted.lock().extend_from_slice(keys);
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for RecordingSink {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> PlatformResult<()> {
        self.calls.lock().push("closed");
        self.closed.lock().extend_from_slice(candles);
        Ok(())
    }
}

#[derive(Default, Clone)]
struct BlockingSink {
    published: Arc<Mutex<usize>>,
    started: Arc<Notify>,
    gate: Arc<Notify>,
}

#[async_trait]
impl OpenCandleStore for BlockingSink {
    async fn update_open(&self, _candles: &[Candle]) -> PlatformResult<()> {
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for BlockingSink {
    async fn publish_closed(&self, _candles: &[ClosedCandle]) -> PlatformResult<()> {
        self.started.notify_one();
        self.gate.notified().await;
        *self.published.lock() += 1;
        Ok(())
    }
}

#[derive(Default, Clone)]
struct BlockingOpenSink {
    opens: Arc<Mutex<usize>>,
    gate: Arc<Notify>,
}

#[async_trait]
impl OpenCandleStore for BlockingOpenSink {
    async fn update_open(&self, _candles: &[Candle]) -> PlatformResult<()> {
        self.gate.notified().await;
        *self.opens.lock() += 1;
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for BlockingOpenSink {
    async fn publish_closed(&self, _candles: &[ClosedCandle]) -> PlatformResult<()> {
        Ok(())
    }
}

#[derive(Default, Clone)]
struct RetrySink {
    attempts: Arc<AtomicUsize>,
    published: Arc<Mutex<Vec<ClosedCandle>>>,
}

#[async_trait]
impl OpenCandleStore for RetrySink {
    async fn update_open(&self, _candles: &[Candle]) -> PlatformResult<()> {
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for RetrySink {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> PlatformResult<()> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(PlatformError::bad_value(
                "sink",
                "test",
                "transient failure",
            ));
        }
        self.published.lock().extend_from_slice(candles);
        Ok(())
    }
}

fn key(interval: &str) -> CandleKey {
    CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        AlignedWindow::try_from(interval).unwrap(),
    )
}

fn candle(interval: &str, close: f64) -> Candle {
    let trade = Trade {
        exchange: CexId::Binance,
        trading_pair: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        price: Decimal::from_str_exact(&close.to_string()).unwrap(),
        quantity: Decimal::ONE,
        timestamp: TimestampMilliseconds(61_000),
    };
    Candle::open(&trade, AlignedWindow::try_from(interval).unwrap()).unwrap()
}

fn closed(candle: Candle) -> ClosedCandle {
    ClosedCandle::new(candle, TimestampMilliseconds(121_000), 1, 0)
}

fn update(open: Vec<Candle>, closed: Vec<ClosedCandle>) -> ProcessorUpdate {
    ProcessorUpdate {
        open,
        closed,
        ..ProcessorUpdate::default()
    }
}

#[tokio::test]
async fn open_mutations_are_coalesced_to_the_last_value_per_key() {
    let sink = RecordingSink::default();
    let observed = sink.clone();
    let writer = CandleOutputWriter::start_with_limits(sink, 16, Duration::from_secs(60), 100);
    let state = AggregatorState::new();

    writer
        .enqueue(update(vec![candle("1m", 100.0)], Vec::new()), &state)
        .await
        .unwrap();
    writer
        .enqueue(update(vec![candle("1m", 105.0)], Vec::new()), &state)
        .await
        .unwrap();
    writer.flush().await.unwrap();
    writer.shutdown().await.unwrap();

    assert_eq!(observed.calls(), vec!["open"]);
    let open = observed.open.lock();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].ohlcv.close(), 105.0);
}

#[tokio::test]
async fn closed_candles_are_published_before_open_mutations() {
    let sink = RecordingSink::default();
    let observed = sink.clone();
    let writer = CandleOutputWriter::start_with_limits(sink, 16, Duration::from_secs(60), 100);

    writer
        .enqueue(
            update(vec![], vec![closed(candle("1m", 99.0))]),
            &AggregatorState::new(),
        )
        .await
        .unwrap();
    writer.flush().await.unwrap();
    writer.shutdown().await.unwrap();

    assert_eq!(observed.calls(), vec!["closed", "delete"]);
    assert_eq!(observed.closed.lock().len(), 1);
    assert_eq!(*observed.deleted.lock(), vec![key("1m")]);

    let sink = RecordingSink::default();
    let observed = sink.clone();
    let writer = CandleOutputWriter::start_with_limits(sink, 16, Duration::from_secs(60), 100);
    writer
        .enqueue(
            update(vec![candle("1m", 100.0)], Vec::new()),
            &AggregatorState::new(),
        )
        .await
        .unwrap();
    writer.flush().await.unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(observed.calls(), vec!["open"]);
}

#[tokio::test]
async fn flush_waits_for_closed_candle_publication() {
    let sink = BlockingSink::default();
    let observed = sink.clone();
    let writer = CandleOutputWriter::start_with_limits(sink, 16, Duration::from_secs(60), 100);

    writer
        .enqueue(
            update(vec![], vec![closed(candle("1m", 99.0))]),
            &AggregatorState::new(),
        )
        .await
        .unwrap();

    let barrier = tokio::spawn(async move { writer.flush().await });
    tokio::time::timeout(Duration::from_secs(1), observed.started.notified())
        .await
        .expect("publication should start");
    assert!(!barrier.is_finished());
    assert_eq!(*observed.published.lock(), 0);

    observed.gate.notify_one();
    tokio::time::timeout(Duration::from_secs(1), barrier)
        .await
        .expect("barrier should complete after publication")
        .expect("barrier task should not panic")
        .unwrap();
    assert_eq!(*observed.published.lock(), 1);
}

#[tokio::test]
async fn barrier_does_not_wait_for_pending_open_writes() {
    let sink = BlockingOpenSink::default();
    let observed = sink.clone();
    let writer = CandleOutputWriter::start_with_limits(sink, 16, Duration::from_secs(60), 100);

    writer
        .enqueue(
            update(vec![candle("1m", 100.0)], Vec::new()),
            &AggregatorState::new(),
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), writer.barrier())
        .await
        .expect("closed-candle barrier must not wait for open projections")
        .unwrap();
    assert_eq!(*observed.opens.lock(), 0);

    observed.gate.notify_one();
    writer.shutdown().await.unwrap();
    assert_eq!(*observed.opens.lock(), 1);
}

#[tokio::test]
async fn a_failed_closed_publication_is_retried_by_the_next_barrier() {
    let sink = RetrySink::default();
    let observed = sink.clone();
    let writer = CandleOutputWriter::start_with_limits(sink, 16, Duration::from_secs(60), 100);

    writer
        .enqueue(
            update(vec![], vec![closed(candle("1m", 99.0))]),
            &AggregatorState::new(),
        )
        .await
        .unwrap();

    assert!(writer.barrier().await.is_err());
    assert_eq!(observed.published.lock().len(), 0);

    writer.barrier().await.unwrap();
    writer.shutdown().await.unwrap();

    assert_eq!(observed.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(observed.published.lock().len(), 1);
}

#[tokio::test]
async fn shutdown_drains_pending_open_mutations() {
    let sink = RecordingSink::default();
    let observed = sink.clone();
    let writer = CandleOutputWriter::start_with_limits(sink, 16, Duration::from_secs(60), 100);

    writer
        .enqueue(
            update(vec![candle("1m", 100.0)], Vec::new()),
            &AggregatorState::new(),
        )
        .await
        .unwrap();
    writer.shutdown().await.unwrap();

    assert_eq!(observed.calls(), vec!["open"]);
    assert_eq!(observed.open.lock().len(), 1);
}

#[tokio::test]
async fn size_bound_flushes_without_waiting_for_the_timer() {
    let sink = RecordingSink::default();
    let observed = sink.clone();
    let writer = CandleOutputWriter::start_with_limits(sink, 16, Duration::from_secs(60), 2);
    let state = AggregatorState::new();

    writer
        .enqueue(update(vec![candle("1m", 100.0)], Vec::new()), &state)
        .await
        .unwrap();
    writer
        .enqueue(update(vec![candle("5m", 100.0)], Vec::new()), &state)
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        while observed.calls().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("size bound should trigger a flush");
    writer.shutdown().await.unwrap();
    assert_eq!(observed.calls(), vec!["open"]);
}
