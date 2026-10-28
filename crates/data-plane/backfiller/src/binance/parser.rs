use crate::fetcher::BackfilledEvent;
use platform_exchange_types::binance::trades::BinanceTradeResponse;
use serde_json::json;

/// Converts Binance REST records into the event shape emitted by the ingester.
pub struct BinanceEventParser;

impl BinanceEventParser {
    pub fn trade(symbol: &str, trade: &BinanceTradeResponse) -> BackfilledEvent {
        BackfilledEvent {
            sequence: trade.id,
            exchange_timestamp: trade.time,
            data: json!({
                "e": "trade",
                "E": trade.time,
                "s": symbol,
                "t": trade.id,
                "p": trade.price,
                "q": trade.qty,
                "T": trade.time,
                "m": trade.isBuyerMaker,
                "M": trade.isBestMatch,
            }),
        }
    }
}
