use crate::event::MarketEvent;
use platform_errors::{BuildError, BuildResult};
use platform_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

/// A single exchange trade used to build candles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    pub exchange: CexId,
    pub trading_pair: TradingPair,
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp: TimestampMilliseconds,
}

/// Decodes an exchange market event into trades.
pub trait TradeDecoder: Send + Sync {
    fn decode(&self, event: &MarketEvent) -> BuildResult<Vec<Trade>>;

    /// Resolves an exchange stream name to the trading pair it carries.
    fn trading_pair_for_stream(&self, stream: &str) -> BuildResult<TradingPair> {
        Err(BuildError::bad_value(
            "stream",
            stream,
            "trade decoder does not support stream resolution",
        ))
    }
}

/// Type-erased trade decoder for an exchange.
pub type SharedTradeDecoder = Arc<dyn TradeDecoder>;
