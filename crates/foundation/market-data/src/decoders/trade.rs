use crate::{decoders::binance::trade::BinanceTradeDecoder, event::MarketEvent};
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_types::new_types::TimestampMilliseconds;
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
    fn decode(&self, event: &MarketEvent) -> BpResult<Vec<Trade>>;

    /// Resolves an exchange stream name to the trading pair it carries.
    fn trading_pair_for_stream(&self, stream: &str) -> BpResult<TradingPair> {
        Err(BpError::bad_value(
            "stream",
            stream,
            "trade decoder does not support stream resolution",
        ))
    }
}

/// Type-erased trade decoder for an exchange.
pub type SharedTradeDecoder = Arc<dyn TradeDecoder>;

/// Builds the trade decoder for an exchange.
pub struct TradeDecoderFactory;

impl TradeDecoderFactory {
    pub fn for_exchange(exchange: CexId) -> BpResult<SharedTradeDecoder> {
        match exchange {
            CexId::Binance => Ok(Arc::new(BinanceTradeDecoder)),
            exchange => Err(BpError::Exchange(ExchangeError::UnsupportedExchange {
                exchange: exchange.to_string(),
            })),
        }
    }

    /// Resolves an exchange stream name without retaining a decoder cache.
    pub fn trading_pair_for_stream(exchange: CexId, stream: &str) -> BpResult<TradingPair> {
        Self::for_exchange(exchange)?.trading_pair_for_stream(stream)
    }
}
