use crate::fetcher::BackfilledEvent;
use platform_exchange_types::binance::{depth::BinanceDepthResponse, trades::BinanceTradeResponse};
use platform_types::new_types::TimestampMilliseconds;
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

    pub fn depth(
        symbol: &str,
        depth: &BinanceDepthResponse,
        timestamp: TimestampMilliseconds,
    ) -> BackfilledEvent {
        BackfilledEvent {
            sequence: depth.lastUpdateId,
            exchange_timestamp: timestamp,
            data: json!({
                "e": "depthUpdate",
                "E": timestamp,
                "s": symbol,
                "U": depth.lastUpdateId,
                "u": depth.lastUpdateId,
                "b": depth.bids,
                "a": depth.asks,
            }),
        }
    }

    pub fn book_ticker(
        symbol: &str,
        sequence: u64,
        bid_price: &str,
        bid_qty: &str,
        ask_price: &str,
        ask_qty: &str,
        timestamp: TimestampMilliseconds,
    ) -> BackfilledEvent {
        BackfilledEvent {
            sequence,
            exchange_timestamp: timestamp,
            data: json!({
                "e": "bookTicker",
                "E": timestamp,
                "u": sequence,
                "s": symbol,
                "b": bid_price,
                "B": bid_qty,
                "a": ask_price,
                "A": ask_qty,
            }),
        }
    }
}
