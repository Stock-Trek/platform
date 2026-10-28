use crate::{
    aggregator::CandleAggregator,
    config::AggregatorConfig,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::MarketEventSource,
};
use chrono::Utc;
use platform_errors::BpResult;
use platform_market_data::{
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::MarketEvent,
};
use platform_metrics::descriptions::{DECODE_KIND_TRADE, DROP_REASON_NO_DECODER, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_types::new_types::TimestampMilliseconds;
use std::collections::{HashMap, hash_map::Entry};
use stock_trek_types::cex::cex_id::CexId;
use tokio::{
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tracing::{error, info, warn};

pub struct AggregatorRunner<S, K> {
    config: AggregatorConfig,
    source: S,
    sink: K,
}

impl<S, K> AggregatorRunner<S, K>
where
    S: MarketEventSource + 'static,
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

        let (sender, mut receiver) = mpsc::channel(1_024);
        let source_task = tokio::spawn(async move {
            loop {
                match source.next_event().await {
                    Ok(Some(event)) => {
                        if sender.send(event).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {
                        info!("market event source ended");
                        break;
                    }
                    Err(error) => {
                        error!(%error, "market event source stopped");
                        break;
                    }
                }
            }
        });

        info!(
            intervals = ?config
                .intervals
                .iter()
                .map(|interval| interval.label())
                .collect::<Vec<_>>(),
            "aggregator started"
        );

        let result = run_until_shutdown(&config, &mut receiver, &sink, &mut aggregator).await;
        source_task.abort();
        result
    }
}

async fn run_until_shutdown<K>(
    config: &AggregatorConfig,
    receiver: &mut mpsc::Receiver<MarketEvent>,
    sink: &K,
    aggregator: &mut CandleAggregator,
) -> BpResult<()>
where
    K: OpenCandleStore + ClosedCandlePublisher,
{
    let mut decoders: HashMap<CexId, SharedTradeDecoder> = HashMap::new();
    let mut flush_ticker = time::interval(config.flush_interval);
    flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe_event = receiver.recv() => {
                let Some(event) = maybe_event else {
                    info!("market event channel closed");
                    break;
                };
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
                            continue;
                        }
                    },
                };
                match decoder.decode(&event) {
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
                        MarketDataMetrics::record_decode_failure(
                            STAGE_AGGREGATOR,
                            DECODE_KIND_TRADE,
                        );
                        warn!(%error, stream = %event.stream, "skipping unreadable market event");
                    }
                }
            }
            _ = flush_ticker.tick() => {
                let closed = aggregator
                    .flush_closed_at_or_before(TimestampMilliseconds(Utc::now().timestamp_millis()));
                if !closed.is_empty() {
                    sink.publish_closed(&closed).await?;
                }
            }
            signal = platform_runtime::Runtime::shutdown_signal() => {
                signal?;
                break;
            }
        }
    }

    let closed =
        aggregator.flush_closed_at_or_before(TimestampMilliseconds(Utc::now().timestamp_millis()));
    if !closed.is_empty() {
        sink.publish_closed(&closed).await?;
    }
    Ok(())
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
    }

    #[async_trait]
    impl MarketEventSource for FakeSource {
        async fn next_event(&mut self) -> BpResult<Option<MarketEvent>> {
            if self.events.is_empty() {
                Ok(None)
            } else {
                Ok(Some(self.events.remove(0)))
            }
        }
    }

    #[derive(Default, Clone)]
    struct FakeSink {
        open_updates: Arc<Mutex<usize>>,
        closed_updates: Arc<Mutex<usize>>,
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
            *self.open_updates.lock().expect("open updates lock") += candles.len();
            Ok(())
        }
    }

    #[async_trait]
    impl ClosedCandlePublisher for FakeSink {
        async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
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
}
