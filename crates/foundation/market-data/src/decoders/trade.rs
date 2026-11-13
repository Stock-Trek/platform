use crate::{decoders::trade_schema_decoder::SchemaTradeDecoder, event::MarketEvent};
use platform_errors::{BuildError, BuildResult, ExchangeError};
use platform_exchange_types::binance::market_data::BinanceTradeSchema;
use platform_type_bridge::BinanceConverter;
use platform_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    pub exchange: CexId,
    pub trading_pair: TradingPair,
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp: TimestampMilliseconds,
}

pub trait TradeDecoder: Send + Sync {
    fn decode(&self, event: &MarketEvent) -> BuildResult<Vec<Trade>>;

    fn trading_pair_for_stream(&self, stream: &str) -> BuildResult<TradingPair> {
        Err(BuildError::bad_value(
            "stream",
            stream,
            "trade decoder does not support stream resolution",
        ))
    }
}

pub type SharedTradeDecoder = Arc<dyn TradeDecoder>;

pub struct TradeDecoderFactory;

impl TradeDecoderFactory {
    pub fn for_exchange(exchange: CexId) -> BuildResult<SharedTradeDecoder> {
        match exchange {
            CexId::Binance => Ok(Arc::new(SchemaTradeDecoder::<BinanceTradeSchema>::new(
                Self::binance_trading_pair,
            ))),
            exchange => Err(BuildError::Exchange(ExchangeError::UnsupportedExchange {
                exchange: exchange.to_string(),
            })),
        }
    }

    pub fn trading_pair_for_stream(exchange: CexId, stream: &str) -> BuildResult<TradingPair> {
        Self::for_exchange(exchange)?.trading_pair_for_stream(stream)
    }

    fn binance_trading_pair(symbol: &str) -> Option<TradingPair> {
        BinanceConverter::ticker_mapping_static().trading_pair_from_symbol(symbol)
    }
}
