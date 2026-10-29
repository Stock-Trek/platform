//! Repartition/router stage.
//!
//! The router turns the ingester's exchange/stream subjects into a shard stream
//! keyed by `(exchange, trading pair)`. Downstream partition writers therefore
//! see every event for a candle key in order, on exactly one shard.

use crate::{
    config::AggregatorConfig,
    partition::{PartitionId, all_partitions, partition_for},
    source::{MarketInput, MarketSource, SourceRecord},
};
use platform_errors::BpResult;
use platform_market_data::{
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::{GapMarker, MarketEvent},
};
use platform_metrics::descriptions::{DECODE_KIND_TRADE, DROP_REASON_NO_DECODER, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::EventPublisher;
use platform_runtime::Runtime;
use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;
use stock_trek_types::cex::cex_id::CexId;
use tracing::{info, warn};

/// Hashes decoded trading pairs onto partitions.
#[derive(Default)]
pub struct HashRouter {
    decoders: Mutex<HashMap<CexId, SharedTradeDecoder>>,
}

impl HashRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the partitions that must receive `input`.
    ///
    /// An event is routed to every partition owning one of its trading pairs.
    /// Gap markers carry no trading pair after exchange decoding, so they are
    /// broadcast to every partition.
    ///
    /// TODO: resolve the gap's exchange stream to a trading pair and address
    /// only the partition that owns it.
    pub fn partitions(
        &self,
        input: &MarketInput,
        partition_count: usize,
    ) -> BpResult<Vec<PartitionId>> {
        match input {
            MarketInput::Gap(_) => Ok(all_partitions(partition_count)),
            MarketInput::Event(event) => self.event_partitions(event, partition_count),
        }
    }

    /// Alias for [`Self::partitions`].
    pub fn route(&self, input: &MarketInput, partition_count: usize) -> BpResult<Vec<PartitionId>> {
        self.partitions(input, partition_count)
    }

    fn event_partitions(
        &self,
        event: &MarketEvent,
        partition_count: usize,
    ) -> BpResult<Vec<PartitionId>> {
        let decoder = self.decoder(event.exchange)?;
        let trades = decoder.decode(event)?;
        let mut partitions = BTreeSet::new();
        for trade in trades {
            partitions.insert(partition_for(
                trade.exchange,
                trade.trading_pair,
                partition_count,
            ));
        }
        Ok(partitions.into_iter().collect())
    }

    fn decoder(&self, exchange: CexId) -> BpResult<SharedTradeDecoder> {
        let mut decoders = self.decoders.lock().expect("router decoders lock");
        if let Some(decoder) = decoders.get(&exchange) {
            return Ok(decoder.clone());
        }
        let decoder = TradeDecoderFactory::for_exchange(exchange)?;
        decoders.insert(exchange, decoder.clone());
        Ok(decoder)
    }
}

/// Returns the partitions for one event without retaining a decoder cache.
pub fn partitions_for_event(
    event: &MarketEvent,
    partition_count: usize,
) -> BpResult<Vec<PartitionId>> {
    let decoder = TradeDecoderFactory::for_exchange(event.exchange)?;
    let trades = decoder.decode(event)?;
    let mut partitions = BTreeSet::new();
    for trade in trades {
        partitions.insert(partition_for(
            trade.exchange,
            trade.trading_pair,
            partition_count,
        ));
    }
    Ok(partitions.into_iter().collect())
}

/// Publishes routed inputs to their partition subjects.
pub struct NatsRouter<S, P> {
    config: AggregatorConfig,
    source: S,
    publisher: P,
    router: HashRouter,
}

impl<S, P> NatsRouter<S, P>
where
    S: MarketSource,
    P: EventPublisher,
{
    pub fn new(config: AggregatorConfig, source: S, publisher: P) -> Self {
        Self {
            config,
            source,
            publisher,
            router: HashRouter::new(),
        }
    }

    pub async fn run(self) -> BpResult<()> {
        let Self {
            config,
            mut source,
            publisher,
            router,
        } = self;

        info!(
            shard_subject_prefix = %config.shard_subject_prefix,
            shard_count = config.shard_count,
            "aggregator router started"
        );

        Runtime::run_until_shutdown(Self::route_inputs(
            &config,
            &mut source,
            &publisher,
            &router,
        ))
        .await
    }

    async fn route_inputs(
        config: &AggregatorConfig,
        source: &mut S,
        publisher: &P,
        router: &HashRouter,
    ) -> BpResult<()> {
        while let Some(record) = source.next_input().await? {
            Self::route_input(config, source, publisher, router, record).await?;
        }
        info!("aggregator router source ended");
        Ok(())
    }

    async fn route_input(
        config: &AggregatorConfig,
        source: &mut S,
        publisher: &P,
        router: &HashRouter,
        record: SourceRecord,
    ) -> BpResult<()> {
        let partitions = match router.partitions(&record.input, config.shard_count) {
            Ok(partitions) => partitions,
            Err(error) => {
                // Undecodable exchange streams must not poison the router. The
                // partition writer would drop the same event, so acknowledge it
                // after recording why it produced no output.
                match &record.input {
                    MarketInput::Event(_) => MarketDataMetrics::record_decode_failure(
                        STAGE_AGGREGATOR,
                        DECODE_KIND_TRADE,
                    ),
                    MarketInput::Gap(_) => MarketDataMetrics::record_dropped_event(
                        STAGE_AGGREGATOR,
                        DROP_REASON_NO_DECODER,
                    ),
                }
                warn!(%error, "dropping input that could not be routed");
                Vec::new()
            }
        };

        if !partitions.is_empty() {
            let payload = record.input.encode()?;
            for partition in partitions {
                let subject = partition.subject(&config.shard_subject_prefix);
                publisher.publish(&subject, &payload).await?;
            }
        }

        // Publishing is idempotent: the publisher derives a deterministic
        // message id from subject and payload, so a crash between publish and
        // acknowledgement only causes a de-duplicated replay.
        source.ack(&record).await
    }
}

/// Convenience for callers that only have a gap marker: gaps are broadcast.
pub fn gap_partitions(partition_count: usize, _gap: &GapMarker) -> Vec<PartitionId> {
    all_partitions(partition_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::partition_for_candle_key;
    use async_trait::async_trait;
    use platform_errors::BpResult;
    use platform_market_data::candle::CandleKey;
    use platform_types::new_types::TimestampMilliseconds;
    use std::sync::{Arc, Mutex as StdMutex};
    use stock_trek_types::cex::asset_id::AssetId;

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

    fn gap() -> GapMarker {
        GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 1,
            observed_sequence: 3,
            missing_from: 2,
            missing_to: 2,
            detected_at: TimestampMilliseconds(62_000),
        }
    }

    #[test]
    fn events_route_to_the_partition_owning_their_pair() {
        let router = HashRouter::new();
        let event = trade_event();
        let partitions = router.partitions(&MarketInput::Event(event), 8).unwrap();

        assert_eq!(partitions.len(), 1);
        assert!(partitions[0].get() < 8);
        assert_eq!(partitions, partitions_for_event(&trade_event(), 8).unwrap());
    }

    #[test]
    fn gaps_are_broadcast_to_every_partition() {
        let router = HashRouter::new();
        let partitions = router.partitions(&MarketInput::Gap(gap()), 4).unwrap();

        assert_eq!(partitions, all_partitions(4));
        assert_eq!(gap_partitions(4, &gap()), all_partitions(4));
    }

    #[test]
    fn every_interval_for_a_pair_uses_the_same_partition() {
        let first = CandleKey::new(
            CexId::Binance,
            stock_trek_types::cex::trading_pair::TradingPair::new(
                AssetId::Bitcoin,
                AssetId::TetherUSD,
            ),
            platform_market_data::candle::CandleInterval::from_label("1m").unwrap(),
        );
        let second = CandleKey {
            interval: platform_market_data::candle::CandleInterval::from_label("5m").unwrap(),
            ..first.clone()
        };

        assert_eq!(
            partition_for_candle_key(&first, 16),
            partition_for_candle_key(&second, 16)
        );
    }

    #[derive(Default)]
    struct FakeSource {
        records: Vec<SourceRecord>,
        acked: Arc<StdMutex<usize>>,
    }

    #[async_trait]
    impl MarketSource for FakeSource {
        async fn next_input(&mut self) -> BpResult<Option<SourceRecord>> {
            if self.records.is_empty() {
                Ok(None)
            } else {
                Ok(Some(self.records.remove(0)))
            }
        }

        async fn ack(&mut self, _record: &SourceRecord) -> BpResult<()> {
            *self.acked.lock().unwrap() += 1;
            Ok(())
        }
    }

    type PublishedMessages = Arc<StdMutex<Vec<(String, Vec<u8>)>>>;

    #[derive(Default, Clone)]
    struct FakePublisher {
        published: PublishedMessages,
    }

    #[async_trait]
    impl EventPublisher for FakePublisher {
        async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
            self.published
                .lock()
                .unwrap()
                .push((subject.to_string(), payload.to_vec()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn router_publishes_to_shard_subject_then_acknowledges() {
        let acked = Arc::new(StdMutex::new(0));
        let source = FakeSource {
            records: vec![SourceRecord::new(
                MarketInput::Event(trade_event()),
                Default::default(),
            )],
            acked: acked.clone(),
        };
        let publisher = FakePublisher::default();
        let observed = publisher.clone();
        let config = crate::config::AggregatorConfig {
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
        };

        NatsRouter::new(config, source, publisher)
            .run()
            .await
            .unwrap();

        let published = observed.published.lock().unwrap();
        assert_eq!(published.len(), 1);
        assert!(published[0].0.starts_with("aggregator.shards."));
        assert_eq!(
            MarketInput::decode(&published[0].1).unwrap(),
            MarketInput::Event(trade_event())
        );
        assert_eq!(*acked.lock().unwrap(), 1);
    }
}
