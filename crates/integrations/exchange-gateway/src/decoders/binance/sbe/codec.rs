use super::{frame::BINANCE_TRADES_TEMPLATE_ID, groups::SbeGroupExt};
use iris::{Codec, ConnectionError};
use platform_errors::IngesterError;
use platform_market_data::{
    binary_cursor::BinaryCursor,
    frame::{DecodedFrame, FrameBytes},
};
use rust_decimal::Decimal;
use tokio_websockets::Message;

/// Codec for Binance's binary SBE market data streams.
///
/// Schema: <https://github.com/binance/binance-spot-api-docs/blob/master/sbe/schemas/stream_1_0.xml>
#[derive(Debug, Default, Clone, Copy)]
pub struct BinanceSbeCodec;

/// A trade decoded from a Binance SBE trades frame.
#[derive(Debug, Clone, PartialEq)]
pub struct BinanceSbeTrade {
    pub symbol: String,
    pub event_time_us: i64,
    pub price: Decimal,
    pub quantity: Decimal,
}

/// The top of book decoded from a Binance SBE best bid/ask frame.
#[derive(Debug, Clone, PartialEq)]
pub struct BinanceSbeBestBidAsk {
    pub symbol: String,
    pub event_time_us: i64,
    pub bid_price: Decimal,
    pub bid_quantity: Decimal,
    pub ask_price: Decimal,
    pub ask_quantity: Decimal,
}

impl BinanceSbeCodec {
    /// Decodes all trades carried by an SBE trades frame.
    ///
    /// Non-trade SBE frames return an empty vector so callers can share the
    /// stream-level filtering used by JSON trade frames.
    pub fn decode_trades(&self, frame: &[u8]) -> Result<Vec<BinanceSbeTrade>, IngesterError> {
        let (template_id, mut cursor) = self.frame_cursor(frame)?;
        match template_id {
            BINANCE_TRADES_TEMPLATE_ID => self.trade_entries(&mut cursor),
            _ => Ok(Vec::new()),
        }
    }

    /// Decodes the top-of-book carried by an SBE best bid/ask frame.
    pub fn decode_best_bid_ask(&self, frame: &[u8]) -> Result<BinanceSbeBestBidAsk, IngesterError> {
        let (_, best_bid_ask) = self.best_bid_ask_fields(&mut self.frame_cursor(frame)?.1)?;
        Ok(best_bid_ask)
    }

    fn trade_entries(
        &self,
        cursor: &mut BinaryCursor<'_>,
    ) -> Result<Vec<BinanceSbeTrade>, IngesterError> {
        let event_time_us = cursor.read_i64()?;
        let _transact_time_us = cursor.read_i64()?;
        let price_exponent = cursor.read_i8()?;
        let qty_exponent = cursor.read_i8()?;
        let trades = cursor.read_group32()?;
        if trades.count > 0 && trades.block_length == 0 {
            return Err(IngesterError::malformed_sbe(
                "trade entries cannot use a zero block length",
            ));
        }
        let entries_start = trades.entries_start;
        cursor.seek(trades.end()?)?;
        cursor.seek(entries_start)?;

        let mut values = Vec::with_capacity(trades.count.min(1_024));
        for index in 0..trades.count {
            cursor.seek(trades.entry(index)?)?;
            let _id = cursor.read_i64()?;
            let price = Self::decimal_from_mantissa(cursor.read_i64()?, price_exponent)?;
            let quantity = Self::decimal_from_mantissa(cursor.read_i64()?, qty_exponent)?;
            values.push((price, quantity));
        }
        cursor.seek(trades.end()?)?;

        let symbol = cursor.read_var_string()?;
        Ok(values
            .into_iter()
            .map(|(price, quantity)| BinanceSbeTrade {
                symbol: symbol.clone(),
                event_time_us,
                price,
                quantity,
            })
            .collect())
    }
}

impl Codec<(), DecodedFrame> for BinanceSbeCodec {
    fn encode(&self, _request: &()) -> Result<Vec<u8>, ConnectionError> {
        Err(ConnectionError::Codec(
            "the ingester does not send websocket messages".to_string(),
        ))
    }

    fn decode(&self, message: Message) -> Result<DecodedFrame, ConnectionError> {
        if !message.is_binary() {
            return Err(ConnectionError::BadMessagePayload);
        }

        let frame = FrameBytes::from_message(message)?;
        Ok(DecodedFrame::new(self.decode_frame(&frame)))
    }
}
