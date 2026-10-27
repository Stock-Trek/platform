use crate::{aggregator::ClosedCandle, candle::Candle};
use async_trait::async_trait;
use platform_errors::BpResult;

/// Stores the latest version of candles that are still open.
#[async_trait]
pub trait OpenCandleStore: Send + Sync {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()>;
}

/// Publishes candles after they close.
#[async_trait]
pub trait ClosedCandlePublisher: Send + Sync {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()>;
}
