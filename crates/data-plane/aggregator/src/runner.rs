use crate::{
    aggregator::CandleAggregator,
    config::AggregatorConfig,
    sink::{ClosedCandlePublisher, NatsCandleSink, OpenCandleStore},
    source::{MarketEventSource, NatsMarketEventSource},
};
use chrono::Utc;
use platform_errors::BpResult;
use platform_market_data::{
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use std::collections::{HashMap, hash_map::Entry};
use stock_trek_types::cex::cex_id::CexId;
use tokio::{
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tracing::{error, info, warn};

pub struct AggregatorRunner {
    config: AggregatorConfig,
}

impl AggregatorRunner {
    pub fn new(config: AggregatorConfig) -> Self {
        Self { config }
    }

    pub async fn run(self) -> BpResult<()> {
        let mut source = NatsMarketEventSource::connect(&self.config).await?;
        let sink = NatsCandleSink::connect(&self.config).await?;
        let mut aggregator = CandleAggregator::new(self.config.intervals.clone());

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
            intervals = ?self
                .config
                .intervals
                .iter()
                .map(|interval| interval.label())
                .collect::<Vec<_>>(),
            "aggregator started"
        );

        let result = self
            .run_until_shutdown(&mut receiver, &sink, &mut aggregator)
            .await;
        source_task.abort();
        result
    }

    async fn run_until_shutdown(
        &self,
        receiver: &mut mpsc::Receiver<MarketEvent>,
        sink: &NatsCandleSink,
        aggregator: &mut CandleAggregator,
    ) -> BpResult<()> {
        let mut decoders: HashMap<CexId, SharedTradeDecoder> = HashMap::new();
        let mut flush_ticker = time::interval(self.config.flush_interval);
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

        let closed = aggregator
            .flush_closed_at_or_before(TimestampMilliseconds(Utc::now().timestamp_millis()));
        if !closed.is_empty() {
            sink.publish_closed(&closed).await?;
        }
        Ok(())
    }
}
