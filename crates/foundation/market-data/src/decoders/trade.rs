use crate::{decoders::json::JsonFrame, event::MarketEvent};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use platform_errors::{BpError, BpResult, ExchangeError, IngesterError};
use platform_type_bridge::{BinanceConverter, Converter};
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
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

/// Decodes exchange trade frames into the small trade model used for
/// aggregation.
///
/// TODO: dispatch through the shared SBE codec as well. Binary trade frames
/// currently contain prices and quantities that the ingester decoder skips over,
/// so only JSON spot trade and aggregate-trade streams are supported for now.
#[derive(Debug, Default, Clone, Copy)]
pub struct TradeDecoder;

impl TradeDecoder {
    pub fn decode(&self, event: &MarketEvent) -> BpResult<Vec<Trade>> {
        let stream = event.stream.to_ascii_lowercase();
        if !stream.ends_with("@trade") && !stream.ends_with("@aggtrade") {
            return Ok(Vec::new());
        }

        let frame = STANDARD
            .decode(&event.payload)
            .map_err(|error| IngesterError::malformed_binary(error.to_string()))?;
        let value: Value = serde_json::from_slice(&frame).map_err(IngesterError::json)?;
        let (_, data) = JsonFrame::split_envelope(value)?;

        if data
            .get("e")
            .and_then(Value::as_str)
            .is_some_and(|event_type| {
                !matches!(
                    event_type.to_ascii_lowercase().as_str(),
                    "trade" | "aggtrade"
                )
            })
        {
            return Ok(Vec::new());
        }

        let symbol = data
            .get("s")
            .and_then(Value::as_str)
            .or_else(|| stream.split_once('@').map(|(symbol, _)| symbol))
            .ok_or(IngesterError::MissingField("s"))?;
        let trading_pair = Self::trading_pair(event.exchange, symbol).ok_or_else(|| {
            BpError::Exchange(ExchangeError::BadValue {
                name: "symbol".to_string(),
                value: symbol.to_string(),
                reason: "could not resolve base and quote assets".to_string(),
            })
        })?;
        let price = Self::decimal_field(&data, "p")?;
        let quantity = Self::decimal_field(&data, "q")?;
        if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
            // TODO: surface invalid trades through metrics rather than silently
            // dropping them.
            return Ok(Vec::new());
        }

        let timestamp = data
            .get("T")
            .or_else(|| data.get("E"))
            .and_then(Value::as_i64)
            .map(TimestampMilliseconds)
            .or(event.exchange_timestamp)
            .unwrap_or(event.received_at);

        Ok(vec![Trade {
            exchange: event.exchange,
            trading_pair,
            price,
            quantity,
            timestamp,
        }])
    }

    fn trading_pair(exchange: CexId, symbol: &str) -> Option<TradingPair> {
        match exchange {
            CexId::Binance => BinanceConverter::default()
                .ticker_mapping()
                .trading_pair_from_symbol(symbol),
            // TODO: add symbol mappings for the remaining exchanges.
            _ => None,
        }
    }

    fn decimal_field(data: &Map<String, Value>, field: &'static str) -> BpResult<Decimal> {
        let value = data
            .get(field)
            .and_then(Value::as_str)
            .ok_or(IngesterError::MissingField(field))?;
        Decimal::from_str_exact(value).map_err(|error| {
            BpError::Exchange(ExchangeError::BadValue {
                name: field.to_string(),
                value: value.to_string(),
                reason: error.to_string(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use stock_trek_types::cex::asset_id::AssetId;
    use stock_trek_types::cex::cex_id::CexId;

    fn event(stream: &str, payload: Value) -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: stream.to_string(),
            sequence: None,
            exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
            received_at: TimestampMilliseconds(1_700_000_000_001),
            payload: STANDARD.encode(serde_json::to_vec(&payload).unwrap()),
        }
    }

    #[test]
    fn decodes_a_combined_trade_stream() {
        let market_event = event(
            "btcusdt@aggTrade",
            json!({
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "s": "BTCUSDT",
                    "p": "100.25",
                    "q": "2.5",
                    "T": 1_700_000_000_123_i64
                }
            }),
        );

        let trades = TradeDecoder.decode(&market_event).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(
            trades[0].trading_pair,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
        );
        assert_eq!(trades[0].price, Decimal::from_str_exact("100.25").unwrap());
        assert_eq!(trades[0].quantity, Decimal::from_str_exact("2.5").unwrap());
        assert_eq!(
            trades[0].timestamp,
            TimestampMilliseconds(1_700_000_000_123)
        );
    }

    #[test]
    fn ignores_non_trade_streams() {
        let market_event = event("btcusdt@depth", json!({ "e": "depthUpdate" }));
        assert!(TradeDecoder.decode(&market_event).unwrap().is_empty());
    }

    #[test]
    fn ignores_non_positive_trades() {
        let market_event = event(
            "btcusdt@trade",
            json!({ "e": "trade", "s": "BTCUSDT", "p": "0", "q": "1" }),
        );
        assert!(TradeDecoder.decode(&market_event).unwrap().is_empty());
    }

    #[test]
    fn rejects_malformed_base64() {
        let market_event = MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: None,
            exchange_timestamp: None,
            received_at: TimestampMilliseconds(0),
            payload: "not base64".to_string(),
        };
        assert!(TradeDecoder.decode(&market_event).is_err());
    }
}
