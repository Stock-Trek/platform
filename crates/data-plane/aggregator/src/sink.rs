use crate::config::AggregatorConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::{
    candle::{Candle, ClosedCandle},
    envelope::EnvelopeCodec,
};
use platform_nats::{JetStream, KeyValue, NatsPublisher};
use std::sync::Arc;

/// Stores the latest version of candles that are still open.
///
/// This is a derived serving projection. Recovery uses the keyed state stored in
/// partition checkpoints, not this bucket.
#[async_trait]
pub trait OpenCandleStore: Send + Sync {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()>;
}

/// Publishes candles after they close.
#[async_trait]
pub trait ClosedCandlePublisher: Send + Sync {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()>;
}

/// Stores open candles in a NATS KV bucket and publishes closed candles to
/// JetStream.
///
/// Open candles are written per key only so clients can serve the current
/// partial candle. They are a derived projection of the checkpointed keyed
/// state; recovery never reads this bucket back into the aggregator.
///
/// Closed candles already carry a deterministic revision derived from the
/// candle identity, window, close watermark, source offset, and partition state
/// epoch. Publishing therefore needs no global revision counter or pending
/// marker: every replay publishes the same `Nats-Msg-Id`, which JetStream
/// de-duplicates while the partition checkpoint still fences redelivery.
pub struct NatsCandleSink {
    publisher: NatsPublisher,
    open_candles: KeyValue,
    closed_subject_prefix: String,
}

impl NatsCandleSink {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let publisher =
            NatsPublisher::connect(&config.nats_url, "platform-aggregator-candles").await?;
        publisher
            .ensure_stream(&config.closed_candle_subject_prefix)
            .await?;
        let open_candles = KeyValue::connect(
            &config.nats_url,
            "platform-aggregator-candles",
            &JetStream::kv_bucket_name(&config.open_candle_subject_prefix),
        )
        .await?;
        Ok(Self {
            publisher,
            open_candles,
            closed_subject_prefix: config.closed_candle_subject_prefix.clone(),
        })
    }

    fn publish_target(prefix: &str, closed: &ClosedCandle) -> (String, String) {
        let subject = format!("{}.{}", prefix, closed.candle.key.subject_suffix());
        (subject, closed.message_id())
    }
}

#[async_trait]
impl<T> OpenCandleStore for Arc<T>
where
    T: OpenCandleStore + ?Sized,
{
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
        (**self).update_open(candles).await
    }
}

#[async_trait]
impl<T> ClosedCandlePublisher for Arc<T>
where
    T: ClosedCandlePublisher + ?Sized,
{
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
        (**self).publish_closed(candles).await
    }
}

#[async_trait]
impl OpenCandleStore for NatsCandleSink {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
        for candle in candles {
            let key = candle.key.subject_suffix();
            let payload = candle.encode()?;
            self.open_candles.put(&key, &payload).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for NatsCandleSink {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
        for closed in candles {
            let (subject, message_id) = Self::publish_target(&self.closed_subject_prefix, closed);
            let payload = closed.encode()?;
            self.publisher
                .publish_with_message_id(&subject, &message_id, &payload)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_market_data::{candle::CandleInterval, decoders::trade::Trade};
    use platform_types::new_types::TimestampMilliseconds;
    use rust_decimal::Decimal;
    use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

    fn closed_candle(source_offset: u64, state_epoch: u64) -> ClosedCandle {
        let trade = Trade {
            exchange: CexId::Binance,
            trading_pair: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            price: Decimal::from_str_exact("100").unwrap(),
            quantity: Decimal::ONE,
            timestamp: TimestampMilliseconds(61_000),
        };
        let mut candle = Candle::open(&trade, CandleInterval::from_label("1m").unwrap());
        candle.close();
        ClosedCandle::new(
            candle,
            TimestampMilliseconds(121_000),
            source_offset,
            state_epoch,
        )
    }

    #[test]
    fn closed_candle_targets_are_deterministic_and_key_scoped() {
        let first = closed_candle(7, 0);
        let replay = closed_candle(7, 0);

        let first_target = NatsCandleSink::publish_target("market.candles", &first);
        let replay_target = NatsCandleSink::publish_target("market.candles", &replay);

        assert_eq!(first_target, replay_target);
        assert_eq!(
            first_target.0,
            "market.candles.binance.bitcoin.tether_usd.1m"
        );
        assert!(first_target.1.contains("binance.bitcoin.tether_usd.1m"));
        assert!(first_target.1.contains("epoch=0"));
    }

    #[test]
    fn closed_candle_targets_change_with_revision_and_epoch() {
        let base = NatsCandleSink::publish_target("market.candles", &closed_candle(7, 0));
        let other_offset = NatsCandleSink::publish_target("market.candles", &closed_candle(8, 0));
        let other_epoch = NatsCandleSink::publish_target("market.candles", &closed_candle(7, 1));

        assert_ne!(base.1, other_offset.1);
        assert_ne!(base.1, other_epoch.1);
    }
}
