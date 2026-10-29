use crate::{
    aggregator::CandleAggregator,
    config::AggregatorConfig,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::MarketEventSource,
};
use platform_errors::BpResult;
use platform_market_data::{
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::MarketEvent,
};
use platform_metrics::descriptions::{DECODE_KIND_TRADE, DROP_REASON_NO_DECODER, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_runtime::Runtime;
use platform_types::new_types::TimestampMilliseconds;
use std::collections::{HashMap, hash_map::Entry};
use stock_trek_types::cex::cex_id::CexId;
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

pub struct AggregatorRunner<S, K> {
    config: AggregatorConfig,
    source: S,
    sink: K,
}

impl<S, K> AggregatorRunner<S, K>
where
    S: MarketEventSource,
    K: OpenCandleStore + ClosedCandlePublisher,
{
    pub fn new(config: AggregatorConfig, source: S, sink: K) -> Self {
        Self {
            config,
            source,
            sink,
        }
    }

    pub async fn run(self) -> BpResult<()> {
        let Self {
            config,
            mut source,
            sink,
        } = self;
        let mut aggregator = CandleAggregator::new(config.intervals.clone());

        info!(
            intervals = ?config
                .intervals
                .iter()
                .map(|interval| interval.label())
                .collect::<Vec<_>>(),
            "aggregator started"
        );

        let result = Runtime::run_until_shutdown(Self::process_events(
            &config,
            &mut source,
            &sink,
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

    async fn process_events(
        config: &AggregatorConfig,
        source: &mut S,
        sink: &K,
        aggregator: &mut CandleAggregator,
    ) -> BpResult<()> {
        let mut decoders: HashMap<CexId, SharedTradeDecoder> = HashMap::new();
        let mut flush_ticker = time::interval(config.flush_interval);
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe_event = source.next_event() => {
                    match maybe_event {
                        Ok(Some(event)) => {
                            Self::process_event(sink, aggregator, &mut decoders, &event).await?;
                            source.ack(&event).await?;
                        }
                        Ok(None) => {
                            info!("market event source ended");
                            break;
                        }
                        Err(error) => {
                            error!(%error, "market event source stopped");
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
    use platform_market_data::candle::{Candle, ClosedCandle};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct FakeSource {
        events: Vec<MarketEvent>,
        fail: bool,
        acked: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl MarketEventSource for FakeSource {
        async fn next_event(&mut self) -> BpResult<Option<MarketEvent>> {
            if self.fail {
                return Err(platform_errors::BpError::external("source failed"));
            }
            if self.events.is_empty() {
                Ok(None)
            } else {
                Ok(Some(self.events.remove(0)))
            }
        }

        async fn ack(&mut self, _event: &MarketEvent) -> BpResult<()> {
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

    fn config() -> AggregatorConfig {
        AggregatorConfig {
            log_filter: "info".to_string(),
            nats_url: "nats://localhost:4222".to_string(),
            event_subject_prefix: "market.events".to_string(),
            open_candle_subject_prefix: "market.candles.open".to_string(),
            closed_candle_subject_prefix: "market.candles.closed".to_string(),
            intervals: vec![
                platform_market_data::candle::CandleInterval::from_label("1m").unwrap(),
            ],
            flush_interval: std::time::Duration::from_secs(1),
        }
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

    fn trade_event() -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(1),
            exchange_timestamp: Some(TimestampMilliseconds(61_000)),
            received_at: TimestampMilliseconds(61_001),
            payload: "eyJlIjoidHJhZGUiLCJzIjoiQlRDVVNEVCIsInAiOiIxMDAiLCJxIjoiMSIsIlQiOjYxMDAwfQ=="
                .to_string(),
        }
    }

    #[tokio::test]
    async fn acknowledges_after_applying_a_trade() {
        let sink = FakeSink::default();
        let observed = sink.clone();
        let acked = Arc::new(Mutex::new(0));
        let source = FakeSource {
            events: vec![trade_event()],
            acked: acked.clone(),
            ..FakeSource::default()
        };

        AggregatorRunner::new(config(), source, sink)
            .run()
            .await
            .unwrap();

        assert_eq!(observed.open_updates(), 1);
        assert_eq!(*acked.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn does_not_acknowledge_when_applying_a_trade_fails() {
        let acked = Arc::new(Mutex::new(0));
        let source = FakeSource {
            events: vec![trade_event()],
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
    async fn propagates_source_error() {
        let sink = FakeSink::default();
        let runner = AggregatorRunner::new(
            config(),
            FakeSource {
                events: Vec::new(),
                fail: true,
                ..FakeSource::default()
            },
            sink,
        );

        let error = runner.run().await.unwrap_err();

        assert_eq!(error.to_string(), "source failed");
    }
}
