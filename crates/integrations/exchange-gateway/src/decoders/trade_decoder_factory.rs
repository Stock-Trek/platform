use super::binance::BinanceTradeDecoder;
use platform_errors::{BuildError, BuildResult, ExchangeError};
use platform_market_data::trade::SharedTradeDecoder;
use std::sync::Arc;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

pub struct TradeDecoderFactory;

impl TradeDecoderFactory {
    pub fn for_exchange(exchange: CexId) -> BuildResult<SharedTradeDecoder> {
        match exchange {
            CexId::Binance => Ok(Arc::new(BinanceTradeDecoder)),
            exchange => Err(BuildError::Exchange(ExchangeError::UnsupportedExchange {
                exchange: exchange.to_string(),
            })),
        }
    }

    /// Resolves an exchange stream name without retaining a decoder cache.
    pub fn trading_pair_for_stream(exchange: CexId, stream: &str) -> BuildResult<TradingPair> {
        Self::for_exchange(exchange)?.trading_pair_for_stream(stream)
    }
}
