use crate::{
    decoders::{
        binance::sbe::BinanceSbeCodec,
        json::JsonFrame,
        trade::{Trade, TradeDecoder},
    },
    event::MarketEvent,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use platform_errors::{BpError, BpResult, IngesterError};
use platform_metrics::descriptions::{DROP_REASON_INVALID_TRADE, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_type_bridge::BinanceConverter;
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
            BpError::bad_value("symbol", symbol, "could not resolve base and quote assets")
        })?;
        let price = self.decimal_field(&data, "p")?;
        let quantity = self.decimal_field(&data, "q")?;
        if !Self::is_valid_trade(price, quantity) {
            Self::record_invalid_trade();
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

    fn trading_pair_for_stream(&self, stream: &str) -> BpResult<TradingPair> {
        let symbol = stream
            .split_once('@')
            .map_or(stream, |(symbol, _)| symbol)
            .trim();
        if symbol.is_empty() {
            return Err(BpError::bad_value("stream", stream, "symbol is empty"));
        }
        self.trading_pair(symbol).ok_or_else(|| {
            BpError::bad_value("stream", stream, "could not resolve base and quote assets")
        })
    }
}

impl BinanceTradeDecoder {
    fn decode_sbe_trades(&self, event: &MarketEvent, frame: &[u8]) -> BpResult<Vec<Trade>> {
        let sbe_trades = BinanceSbeCodec.decode_trades(frame)?;
        let Some(first) = sbe_trades.first() else {
            return Ok(Vec::new());
        };
        let trading_pair = self.trading_pair(&first.symbol).ok_or_else(|| {
            BpError::bad_value(
                "symbol",
                first.symbol.clone(),
                "could not resolve base and quote assets",
            )
        })?;

        let mut trades = Vec::with_capacity(sbe_trades.len());
        for trade in sbe_trades {
            if !Self::is_valid_trade(trade.price, trade.quantity) {
                Self::record_invalid_trade();
                continue;
            }
            trades.push(Trade {
                exchange: event.exchange,
                trading_pair,
                price: trade.price,
                quantity: trade.quantity,
                timestamp: if trade.event_time_us > 0 {
                    TimestampMilliseconds(trade.event_time_us / 1_000)
                } else {
                    event.exchange_timestamp.unwrap_or(event.received_at)
                },
            });
        }
        Ok(trades)
    }

    fn is_valid_trade(price: Decimal, quantity: Decimal) -> bool {
        price > Decimal::ZERO && quantity > Decimal::ZERO
    }

    fn record_invalid_trade() {
        MarketDataMetrics::record_dropped_event(STAGE_AGGREGATOR, DROP_REASON_INVALID_TRADE);
    }

    fn trading_pair(&self, symbol: &str) -> Option<TradingPair> {
        BinanceConverter::ticker_mapping_static().trading_pair_from_symbol(symbol)
    }

    fn decimal_field(&self, data: &Map<String, Value>, field: &'static str) -> BpResult<Decimal> {
        let value = data
            .get(field)
            .and_then(Value::as_str)
            .ok_or(IngesterError::MissingField(field))?;
        Decimal::from_str_exact(value)
            .map_err(|error| BpError::bad_value(field, value, error.to_string()))
    }
}
