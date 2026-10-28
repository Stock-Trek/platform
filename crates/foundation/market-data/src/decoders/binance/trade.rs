use crate::{
    decoders::{
        binance::sbe::BinanceSbeCodec,
        json::JsonFrame,
        trade::{Trade, TradeDecoder},
    },
    event::MarketEvent,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use platform_errors::{BpError, BpResult, ExchangeError, IngesterError};
use platform_type_bridge::{BinanceConverter, Converter};
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use stock_trek_types::cex::trading_pair::TradingPair;

/// Decodes Binance JSON and SBE spot trade and aggregate-trade streams.
#[derive(Debug, Default, Clone, Copy)]
pub struct BinanceTradeDecoder;

impl TradeDecoder for BinanceTradeDecoder {
    fn decode(&self, event: &MarketEvent) -> BpResult<Vec<Trade>> {
        let stream = event.stream.to_ascii_lowercase();
        if !stream.ends_with("@trade") && !stream.ends_with("@aggtrade") {
            return Ok(Vec::new());
        }

        let frame = STANDARD
            .decode(&event.payload)
            .map_err(|error| IngesterError::malformed_binary(error.to_string()))?;
        if BinanceSbeCodec::is_sbe_frame(&frame) {
            return self.decode_sbe_trades(event, &frame);
        }

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
        let trading_pair = self.trading_pair(symbol).ok_or_else(|| {
            BpError::Exchange(ExchangeError::BadValue {
                name: "symbol".to_string(),
                value: symbol.to_string(),
                reason: "could not resolve base and quote assets".to_string(),
            })
        })?;
        let price = self.decimal_field(&data, "p")?;
        let quantity = self.decimal_field(&data, "q")?;
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
}

impl BinanceTradeDecoder {
    fn decode_sbe_trades(&self, event: &MarketEvent, frame: &[u8]) -> BpResult<Vec<Trade>> {
        let sbe_trades = BinanceSbeCodec.decode_trades(frame)?;
        let Some(first) = sbe_trades.first() else {
            return Ok(Vec::new());
        };
        let trading_pair = self.trading_pair(&first.symbol).ok_or_else(|| {
            BpError::Exchange(ExchangeError::BadValue {
                name: "symbol".to_string(),
                value: first.symbol.clone(),
                reason: "could not resolve base and quote assets".to_string(),
            })
        })?;

        Ok(sbe_trades
            .into_iter()
            .filter(|trade| trade.price > Decimal::ZERO && trade.quantity > Decimal::ZERO)
            .map(|trade| Trade {
                exchange: event.exchange,
                trading_pair,
                price: trade.price,
                quantity: trade.quantity,
                timestamp: if trade.event_time_us > 0 {
                    TimestampMilliseconds(trade.event_time_us / 1_000)
                } else {
                    event.exchange_timestamp.unwrap_or(event.received_at)
                },
            })
            .collect())
    }

    fn trading_pair(&self, symbol: &str) -> Option<TradingPair> {
        BinanceConverter::default()
            .ticker_mapping()
            .trading_pair_from_symbol(symbol)
    }

    fn decimal_field(&self, data: &Map<String, Value>, field: &'static str) -> BpResult<Decimal> {
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
    use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};

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

    fn sbe_trade_event() -> MarketEvent {
        let event_time_us = 1_700_000_000_123_000_i64;
        let mut frame = Vec::new();
        frame.extend_from_slice(&18u16.to_le_bytes());
        frame.extend_from_slice(&10_000u16.to_le_bytes());
        frame.extend_from_slice(&1u16.to_le_bytes());
        frame.extend_from_slice(&0u16.to_le_bytes());
        frame.extend_from_slice(&event_time_us.to_le_bytes());
        frame.extend_from_slice(&event_time_us.to_le_bytes());
        frame.push((-2_i8) as u8);
        frame.push((-3_i8) as u8);
        frame.extend_from_slice(&25u16.to_le_bytes());
        frame.extend_from_slice(&2u32.to_le_bytes());
        for (trade_id, price, quantity) in [(1_i64, 10_025_i64, 2_500_i64), (2, 10_050, 1_000)] {
            frame.extend_from_slice(&trade_id.to_le_bytes());
            frame.extend_from_slice(&price.to_le_bytes());
            frame.extend_from_slice(&quantity.to_le_bytes());
            frame.push(0);
        }
        frame.push(7);
        frame.extend_from_slice(b"btcusdt");

        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(2),
            exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_123)),
            received_at: TimestampMilliseconds(1_700_000_000_124),
            payload: STANDARD.encode(frame),
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

        let trades = BinanceTradeDecoder.decode(&market_event).unwrap();
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
        assert!(
            BinanceTradeDecoder
                .decode(&market_event)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn ignores_non_positive_trades() {
        let market_event = event(
            "btcusdt@trade",
            json!({ "e": "trade", "s": "BTCUSDT", "p": "0", "q": "1" }),
        );
        assert!(
            BinanceTradeDecoder
                .decode(&market_event)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn decodes_sbe_trades() {
        let trades = BinanceTradeDecoder.decode(&sbe_trade_event()).unwrap();

        assert_eq!(trades.len(), 2);
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
        assert_eq!(trades[1].price, Decimal::from_str_exact("100.5").unwrap());
        assert_eq!(trades[1].quantity, Decimal::from_str_exact("1").unwrap());
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
        assert!(BinanceTradeDecoder.decode(&market_event).is_err());
    }
}
