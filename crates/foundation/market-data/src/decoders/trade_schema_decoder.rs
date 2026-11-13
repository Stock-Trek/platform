use crate::{
    decoders::{
        json_mapping::JsonFrame,
        sbe::SbeCodec,
        trade::{Trade, TradeDecoder},
    },
    event::MarketEvent,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use platform_errors::{BuildError, BuildResult, IngesterError};
use platform_exchange_types::market_data::{SbeTradeRecord, TradeSchemaDefinition};
use platform_metrics::{DROP_REASON_INVALID_TRADE, MarketDataMetrics, STAGE_AGGREGATOR};
use platform_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use std::marker::PhantomData;
use stock_trek_types::cex::trading_pair::TradingPair;

pub struct SchemaTradeDecoder<S> {
    symbol_mapping: fn(&str) -> Option<TradingPair>,
    _schema: PhantomData<S>,
}

impl<S> SchemaTradeDecoder<S> {
    pub const fn new(symbol_mapping: fn(&str) -> Option<TradingPair>) -> Self {
        Self {
            symbol_mapping,
            _schema: PhantomData,
        }
    }
}

impl<S: TradeSchemaDefinition + Send + Sync + 'static> TradeDecoder for SchemaTradeDecoder<S> {
    fn decode(&self, event: &MarketEvent) -> BuildResult<Vec<Trade>> {
        let schema = S::SCHEMA;
        let stream = event.stream.to_ascii_lowercase();
        if !schema
            .stream_suffixes
            .iter()
            .any(|suffix| stream.ends_with(suffix))
        {
            return Ok(Vec::new());
        }

        let frame = STANDARD
            .decode(&event.payload)
            .map_err(|error| IngesterError::malformed_binary(error.to_string()))?;
        if SbeCodec::<S::Sbe>::is_sbe_frame(&frame) {
            return self.decode_sbe_trades(event, &frame);
        }

        let value: Value = serde_json::from_slice(&frame).map_err(IngesterError::json)?;
        let (_, data) = JsonFrame::split_envelope(value)?;

        if data
            .get(schema.event_type_field)
            .and_then(Value::as_str)
            .is_some_and(|event_type| {
                !schema
                    .event_types
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(event_type))
            })
        {
            return Ok(Vec::new());
        }

        let symbol = data
            .get(schema.symbol_field)
            .and_then(Value::as_str)
            .or_else(|| stream.split_once('@').map(|(symbol, _)| symbol))
            .ok_or(IngesterError::MissingField(schema.symbol_field))?;
        let trading_pair = self.trading_pair(symbol).ok_or_else(|| {
            BuildError::bad_value("symbol", symbol, "could not resolve base and quote assets")
        })?;
        let price = self.decimal_field(&data, schema.price_field)?;
        let quantity = self.decimal_field(&data, schema.quantity_field)?;
        if !Self::is_valid_trade(price, quantity) {
            Self::record_invalid_trade();
            return Ok(Vec::new());
        }

        let timestamp = schema
            .timestamp_fields
            .iter()
            .find_map(|field| data.get(*field).and_then(Value::as_i64))
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

    fn trading_pair_for_stream(&self, stream: &str) -> BuildResult<TradingPair> {
        let symbol = stream
            .split_once('@')
            .map_or(stream, |(symbol, _)| symbol)
            .trim();
        if symbol.is_empty() {
            return Err(BuildError::bad_value("stream", stream, "symbol is empty"));
        }
        self.trading_pair(symbol).ok_or_else(|| {
            BuildError::bad_value("stream", stream, "could not resolve base and quote assets")
        })
    }
}

impl<S: TradeSchemaDefinition> SchemaTradeDecoder<S> {
    fn decode_sbe_trades(&self, event: &MarketEvent, frame: &[u8]) -> BuildResult<Vec<Trade>> {
        let sbe_trades = SbeCodec::<S::Sbe>::default().decode_trades(frame)?;
        let Some(first) = sbe_trades.first() else {
            return Ok(Vec::new());
        };
        let trading_pair = self.trading_pair(first.symbol()).ok_or_else(|| {
            BuildError::bad_value(
                "symbol",
                first.symbol(),
                "could not resolve base and quote assets",
            )
        })?;

        let mut trades = Vec::with_capacity(sbe_trades.len());
        for trade in sbe_trades {
            if !Self::is_valid_trade(trade.price(), trade.quantity()) {
                Self::record_invalid_trade();
                continue;
            }
            trades.push(Trade {
                exchange: event.exchange,
                trading_pair,
                price: trade.price(),
                quantity: trade.quantity(),
                timestamp: if trade.event_time_us() > 0 {
                    TimestampMilliseconds(trade.event_time_us() / 1_000)
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
        (self.symbol_mapping)(symbol)
    }

    fn decimal_field(
        &self,
        data: &Map<String, Value>,
        field: &'static str,
    ) -> BuildResult<Decimal> {
        let value = data
            .get(field)
            .and_then(Value::as_str)
            .ok_or(IngesterError::MissingField(field))?;
        Decimal::from_str_exact(value)
            .map_err(|error| BuildError::bad_value(field, value, error.to_string()))
    }
}
