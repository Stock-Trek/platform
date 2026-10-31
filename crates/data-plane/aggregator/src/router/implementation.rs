use crate::{
    config::AggregatorConfig,
    shard::ShardId,
    source::{MarketInput, MarketSource, SourceRecord},
};
use parking_lot::Mutex;
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
use stock_trek_types::cex::cex_id::CexId;
use tracing::{info, warn};

/// Hashes decoded trading pairs onto shards.
#[derive(Default)]
pub struct HashRouter {
    decoders: Mutex<HashMap<CexId, SharedTradeDecoder>>,
}

impl HashRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the shards that must receive `input`.
    ///
    /// An event is routed to every shard owning one of its trading pairs.
    /// Gap markers are routed to the shard owning the trading pair encoded
    /// in their exchange stream.
    pub fn shards(&self, input: &MarketInput, shard_count: usize) -> BpResult<Vec<ShardId>> {
        match input {
            MarketInput::Gap(gap) => self.gap_shards(gap, shard_count),
            MarketInput::Event(event) => self.event_shards(event, shard_count),
        }
    }

    fn event_shards(&self, event: &MarketEvent, shard_count: usize) -> BpResult<Vec<ShardId>> {
        let decoder = self.decoder(event.exchange)?;
        let trades = decoder.decode(event)?;
        let mut shards = BTreeSet::new();
        for trade in trades {
            shards.insert(ShardId::for_trading_pair(
                trade.exchange,
                trade.trading_pair,
                shard_count,
            ));
        }
        Ok(shards.into_iter().collect())
    }

    fn decoder(&self, exchange: CexId) -> BpResult<SharedTradeDecoder> {
        let mut decoders = self.decoders.lock();
        if let Some(decoder) = decoders.get(&exchange) {
            return Ok(decoder.clone());
        }
        let decoder = TradeDecoderFactory::for_exchange(exchange)?;
        decoders.insert(exchange, decoder.clone());
        Ok(decoder)
    }

    fn gap_shards(&self, gap: &GapMarker, shard_count: usize) -> BpResult<Vec<ShardId>> {
        let decoder = self.decoder(gap.exchange)?;
        let trading_pair = decoder.trading_pair_for_stream(&gap.stream)?;
        Ok(vec![ShardId::for_trading_pair(
            gap.exchange,
            trading_pair,
            shard_count,
        )])
    }
}

/// Publishes routed inputs to their shard subjects.
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
        let shards = match router.shards(&record.input, config.shard_count) {
            Ok(shards) => shards,
            Err(error) => {
                // Undecodable exchange streams must not poison the router. The
                // shard writer would drop the same event, so acknowledge it
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

        if !shards.is_empty() {
            let payload = record.input.encode()?;
            for shard in shards {
                let subject = shard.subject(&config.shard_subject_prefix);
                publisher.publish(&subject, &payload).await?;
            }
        }

        // Publishing is idempotent: the publisher derives a deterministic
        // message id from subject and payload, so a crash between publish and
        // acknowledgement only causes a de-duplicated replay.
        source.ack(&record).await
    }
}
