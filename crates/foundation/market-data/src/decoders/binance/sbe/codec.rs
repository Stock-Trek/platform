use super::{decimal::decimal_from_mantissa, groups::SbeGroupExt};
use crate::{
    decoders::{
        binary::BinaryCursor,
        frame::{DecodedFrame, FrameBytes},
    },
    event::ParsedMessage,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use iris::{Codec, ConnectionError};
use platform_errors::IngesterError;
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use tokio_websockets::Message;

pub(super) const BINANCE_SBE_SCHEMA_ID: u16 = 1;

pub(super) const BINANCE_TRADES_TEMPLATE_ID: u16 = 10_000;
pub(super) const BINANCE_BEST_BID_ASK_TEMPLATE_ID: u16 = 10_001;
pub(super) const BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID: u16 = 10_002;
pub(super) const BINANCE_DEPTH_DIFF_TEMPLATE_ID: u16 = 10_003;

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
    pub fn is_sbe_frame(frame: &[u8]) -> bool {
        if frame.len() < 8 {
            return false;
        }
        let template_id = u16::from_le_bytes([frame[2], frame[3]]);
        let schema_id = u16::from_le_bytes([frame[4], frame[5]]);
        schema_id == BINANCE_SBE_SCHEMA_ID
            || matches!(
                template_id,
                BINANCE_TRADES_TEMPLATE_ID
                    | BINANCE_BEST_BID_ASK_TEMPLATE_ID
                    | BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID
                    | BINANCE_DEPTH_DIFF_TEMPLATE_ID
            )
    }

    fn frame_cursor<'a>(&self, frame: &'a [u8]) -> Result<(u16, BinaryCursor<'a>), IngesterError> {
        let mut cursor = BinaryCursor::new(frame);
        let _block_length = cursor.read_u16()?;
        let template_id = cursor.read_u16()?;
        let schema_id = cursor.read_u16()?;
        let _version = cursor.read_u16()?;
        if schema_id != BINANCE_SBE_SCHEMA_ID {
            return Err(IngesterError::malformed_sbe(format!(
                "expected schema id {BINANCE_SBE_SCHEMA_ID}, got {schema_id}"
            )));
        }
        Ok((template_id, cursor))
    }

    fn decode_frame(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let (template_id, mut cursor) = self.frame_cursor(frame)?;

        match template_id {
            BINANCE_TRADES_TEMPLATE_ID => self.trades_event(&mut cursor, frame),
            BINANCE_BEST_BID_ASK_TEMPLATE_ID => self.best_bid_ask_event(&mut cursor, frame),
            BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID => self.depth_snapshot_event(&mut cursor, frame),
            BINANCE_DEPTH_DIFF_TEMPLATE_ID => self.depth_diff_event(&mut cursor, frame),
            template_id => Err(IngesterError::unknown_sbe_template(template_id)),
        }
    }

    /// Decodes all trades carried by an SBE trades frame.
    ///
    /// Non-trade SBE frames return an empty vector so callers can share the
    /// stream-level filtering used by JSON trade frames.
    pub fn decode_trades(&self, frame: &[u8]) -> Result<Vec<BinanceSbeTrade>, IngesterError> {
        let (template_id, mut cursor) = self.frame_cursor(frame)?;
        match template_id {
            BINANCE_TRADES_TEMPLATE_ID => self.trade_entries(&mut cursor),
            BINANCE_BEST_BID_ASK_TEMPLATE_ID
            | BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID
            | BINANCE_DEPTH_DIFF_TEMPLATE_ID => Ok(Vec::new()),
            template_id => Err(IngesterError::unknown_sbe_template(template_id)),
        }
    }

    /// Decodes the top-of-book carried by an SBE best bid/ask frame.
    pub fn decode_best_bid_ask(&self, frame: &[u8]) -> Result<BinanceSbeBestBidAsk, IngesterError> {
        let (template_id, mut cursor) = self.frame_cursor(frame)?;
        if template_id != BINANCE_BEST_BID_ASK_TEMPLATE_ID {
            return Err(IngesterError::malformed_sbe(format!(
                "expected best bid/ask template id {BINANCE_BEST_BID_ASK_TEMPLATE_ID}, got {template_id}"
            )));
        }
        let (_, best_bid_ask) = self.best_bid_ask_fields(&mut cursor)?;
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
            let price = decimal_from_mantissa(cursor.read_i64()?, price_exponent)?;
            let quantity = decimal_from_mantissa(cursor.read_i64()?, qty_exponent)?;
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

    fn trades_event(
        &self,
        cursor: &mut BinaryCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64()?;
        let _transact_time_us = cursor.read_i64()?;
        let _price_exponent = cursor.read_u8()?;
        let _qty_exponent = cursor.read_u8()?;
        let trades = cursor.read_group32()?;
        let sequence = if trades.count == 0 {
            None
        } else {
            cursor.seek(trades.entry(trades.count - 1)?)?;
            Some(cursor.read_sequence()?)
        };
        cursor.seek(trades.end()?)?;
        let symbol = cursor.read_var_string()?;
        Ok(self.parsed(frame, &symbol, "trade", sequence, Some(event_time_us)))
    }

    fn best_bid_ask_event(
        &self,
        cursor: &mut BinaryCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let (sequence, best_bid_ask) = self.best_bid_ask_fields(cursor)?;
        Ok(self.parsed(
            frame,
            &best_bid_ask.symbol,
            "bestBidAsk",
            Some(sequence),
            Some(best_bid_ask.event_time_us),
        ))
    }

    fn best_bid_ask_fields(
        &self,
        cursor: &mut BinaryCursor<'_>,
    ) -> Result<(u64, BinanceSbeBestBidAsk), IngesterError> {
        let event_time_us = cursor.read_i64()?;
        let sequence = cursor.read_sequence()?;
        let price_exponent = cursor.read_i8()?;
        let qty_exponent = cursor.read_i8()?;
        let bid_price = decimal_from_mantissa(cursor.read_i64()?, price_exponent)?;
        let bid_quantity = decimal_from_mantissa(cursor.read_i64()?, qty_exponent)?;
        let ask_price = decimal_from_mantissa(cursor.read_i64()?, price_exponent)?;
        let ask_quantity = decimal_from_mantissa(cursor.read_i64()?, qty_exponent)?;
        let symbol = cursor.read_var_string()?;
        Ok((
            sequence,
            BinanceSbeBestBidAsk {
                symbol,
                event_time_us,
                bid_price,
                bid_quantity,
                ask_price,
                ask_quantity,
            },
        ))
    }

    fn depth_snapshot_event(
        &self,
        cursor: &mut BinaryCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64()?;
        let sequence = Some(cursor.read_sequence()?);
        let _price_exponent = cursor.read_u8()?;
        let _qty_exponent = cursor.read_u8()?;
        let bids = cursor.read_group16()?;
        cursor.seek(bids.end()?)?;
        let asks = cursor.read_group16()?;
        cursor.seek(asks.end()?)?;
        let symbol = cursor.read_var_string()?;
        Ok(self.parsed(frame, &symbol, "depth20", sequence, Some(event_time_us)))
    }

    fn depth_diff_event(
        &self,
        cursor: &mut BinaryCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64()?;
        let _transact_time_us = cursor.read_i64()?;
        let sequence = Some(cursor.read_sequence()?);
        let _price_exponent = cursor.read_u8()?;
        let _qty_exponent = cursor.read_u8()?;
        let bids = cursor.read_group16()?;
        cursor.seek(bids.end()?)?;
        let asks = cursor.read_group16()?;
        cursor.seek(asks.end()?)?;
        let symbol = cursor.read_var_string()?;
        Ok(self.parsed(frame, &symbol, "depth", sequence, Some(event_time_us)))
    }

    fn parsed(
        &self,
        frame: &[u8],
        symbol: &str,
        suffix: &str,
        sequence: Option<u64>,
        event_time_us: Option<i64>,
    ) -> ParsedMessage {
        ParsedMessage {
            stream: format!("{}@{suffix}", symbol.to_ascii_lowercase()),
            sequence,
            first_sequence: None,
            previous_sequence: None,
            exchange_timestamp: event_time_us
                .map(|microseconds| TimestampMilliseconds(microseconds / 1_000)),
            payload: BASE64.encode(frame),
        }
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

        // TODO: validate the SBE header here once multiple binary message
        // versions need to be supported.
        let frame = FrameBytes::from_message(message)?;
        Ok(DecodedFrame::new(self.decode_frame(&frame)))
    }
}
