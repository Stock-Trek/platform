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

const BINANCE_SBE_SCHEMA_ID: u16 = 1;

const BINANCE_TRADES_TEMPLATE_ID: u16 = 10_000;
const BINANCE_BEST_BID_ASK_TEMPLATE_ID: u16 = 10_001;
const BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID: u16 = 10_002;
const BINANCE_DEPTH_DIFF_TEMPLATE_ID: u16 = 10_003;

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

/// SBE group header and the location of its entries within the frame.
#[derive(Debug, Clone, Copy)]
struct SbeGroup {
    entries_start: usize,
    block_length: usize,
    count: usize,
}

impl SbeGroup {
    fn entry(&self, index: usize) -> Result<usize, IngesterError> {
        if index >= self.count {
            return Err(IngesterError::malformed_sbe(format!(
                "group entry {index} is out of bounds for {} entries",
                self.count
            )));
        }
        index
            .checked_mul(self.block_length)
            .and_then(|offset| self.entries_start.checked_add(offset))
            .ok_or_else(|| IngesterError::malformed_sbe("group entry offset overflow"))
    }

    fn end(&self) -> Result<usize, IngesterError> {
        self.count
            .checked_mul(self.block_length)
            .and_then(|length| self.entries_start.checked_add(length))
            .ok_or_else(|| IngesterError::malformed_sbe("group size overflow"))
    }
}

/// SBE-specific group traversal layered on top of the generic binary cursor.
trait SbeGroupExt {
    fn read_group32(&mut self) -> Result<SbeGroup, IngesterError>;
    fn read_group16(&mut self) -> Result<SbeGroup, IngesterError>;
}

impl SbeGroupExt for BinaryCursor<'_> {
    fn read_group32(&mut self) -> Result<SbeGroup, IngesterError> {
        let block_length = self.read_u16()? as usize;
        let count = self.read_u32()? as usize;
        Ok(SbeGroup {
            entries_start: self.position(),
            block_length,
            count,
        })
    }

    fn read_group16(&mut self) -> Result<SbeGroup, IngesterError> {
        let block_length = self.read_u16()? as usize;
        let count = self.read_u16()? as usize;
        Ok(SbeGroup {
            entries_start: self.position(),
            block_length,
            count,
        })
    }
}

fn decimal_from_mantissa(mantissa: i64, exponent: i8) -> Result<Decimal, IngesterError> {
    let value = if exponent <= 0 {
        let scale = u32::from(exponent.unsigned_abs());
        Decimal::try_from_i128_with_scale(i128::from(mantissa), scale)
    } else {
        let factor = 10_i128
            .checked_pow(u32::from(exponent as u8))
            .ok_or_else(|| IngesterError::malformed_sbe("decimal exponent overflow"))?;
        let value = i128::from(mantissa)
            .checked_mul(factor)
            .ok_or_else(|| IngesterError::malformed_sbe("decimal mantissa overflow"))?;
        Decimal::try_from_i128_with_scale(value, 0)
    };
    value.map_err(|error| {
        IngesterError::malformed_sbe(format!(
            "invalid decimal mantissa {mantissa} with exponent {exponent}: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sbe_header(template_id: u16) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&18u16.to_le_bytes());
        frame.extend_from_slice(&template_id.to_le_bytes());
        frame.extend_from_slice(&BINANCE_SBE_SCHEMA_ID.to_le_bytes());
        frame.extend_from_slice(&0u16.to_le_bytes());
        frame
    }

    fn append_var_string(frame: &mut Vec<u8>, value: &str) {
        frame.push(value.len() as u8);
        frame.extend_from_slice(value.as_bytes());
    }

    fn sbe_trades_frame() -> Vec<u8> {
        let event_time_us = 1_700_000_000_123_000_i64;
        let mut frame = sbe_header(BINANCE_TRADES_TEMPLATE_ID);
        frame.extend_from_slice(&event_time_us.to_le_bytes());
        frame.extend_from_slice(&event_time_us.to_le_bytes());
        frame.push((-8_i8) as u8);
        frame.push((-8_i8) as u8);
        frame.extend_from_slice(&25u16.to_le_bytes());
        frame.extend_from_slice(&2u32.to_le_bytes());
        for trade_id in [42_i64, 43_i64] {
            frame.extend_from_slice(&trade_id.to_le_bytes());
            frame.extend_from_slice(&100_i64.to_le_bytes());
            frame.extend_from_slice(&1_i64.to_le_bytes());
            frame.push(0);
        }
        append_var_string(&mut frame, "btcusdt");
        frame
    }

    fn sbe_best_bid_ask_frame() -> Vec<u8> {
        let mut frame = sbe_header(BINANCE_BEST_BID_ASK_TEMPLATE_ID);
        frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
        frame.extend_from_slice(&99_i64.to_le_bytes());
        frame.push((-2_i8) as u8);
        frame.push((-3_i8) as u8);
        frame.extend_from_slice(&10_025_i64.to_le_bytes());
        frame.extend_from_slice(&2_500_i64.to_le_bytes());
        frame.extend_from_slice(&10_050_i64.to_le_bytes());
        frame.extend_from_slice(&1_000_i64.to_le_bytes());
        append_var_string(&mut frame, "btcusdt");
        frame
    }

    fn sbe_depth_snapshot_frame() -> Vec<u8> {
        let mut frame = sbe_header(BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID);
        frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
        frame.extend_from_slice(&456_i64.to_le_bytes());
        frame.push(0);
        frame.push(0);
        for _ in 0..2 {
            frame.extend_from_slice(&16u16.to_le_bytes());
            frame.extend_from_slice(&1u16.to_le_bytes());
            frame.extend_from_slice(&[0u8; 16]);
        }
        append_var_string(&mut frame, "btcusdt");
        frame
    }

    fn sbe_depth_diff_frame() -> Vec<u8> {
        let mut frame = sbe_header(BINANCE_DEPTH_DIFF_TEMPLATE_ID);
        frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
        frame.extend_from_slice(&100_i64.to_le_bytes());
        frame.extend_from_slice(&123_i64.to_le_bytes());
        frame.push(0);
        frame.push(0);
        for _ in 0..2 {
            frame.extend_from_slice(&16u16.to_le_bytes());
            frame.extend_from_slice(&1u16.to_le_bytes());
            frame.extend_from_slice(&[0u8; 16]);
        }
        append_var_string(&mut frame, "btcusdt");
        frame
    }

    fn decode(frame: Vec<u8>) -> Result<ParsedMessage, IngesterError> {
        BinanceSbeCodec
            .decode(Message::binary(frame))
            .unwrap()
            .result
    }

    #[test]
    fn decodes_binance_sbe_trades() {
        let frame = sbe_trades_frame();

        let parsed = decode(frame.clone()).unwrap();

        assert_eq!(parsed.stream, "btcusdt@trade");
        assert_eq!(parsed.sequence, Some(43));
        assert_eq!(
            parsed.exchange_timestamp,
            Some(TimestampMilliseconds(1_700_000_000_123))
        );
        assert_eq!(BASE64.decode(&parsed.payload).unwrap(), frame);
    }

    #[test]
    fn decodes_sbe_trade_entries() {
        let trades = BinanceSbeCodec.decode_trades(&sbe_trades_frame()).unwrap();

        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].symbol, "btcusdt");
        assert_eq!(trades[0].event_time_us, 1_700_000_000_123_000);
        assert_eq!(
            trades[0].price,
            Decimal::from_str_exact("0.000001").unwrap()
        );
        assert_eq!(
            trades[0].quantity,
            Decimal::from_str_exact("0.00000001").unwrap()
        );
        assert_eq!(trades[1].price, trades[0].price);
        assert_eq!(trades[1].quantity, trades[0].quantity);
    }

    #[test]
    fn non_trade_sbe_frames_have_no_trade_entries() {
        assert!(
            BinanceSbeCodec
                .decode_trades(&sbe_best_bid_ask_frame())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn decodes_binance_sbe_best_bid_ask() {
        let parsed = decode(sbe_best_bid_ask_frame()).unwrap();

        assert_eq!(parsed.stream, "btcusdt@bestBidAsk");
        assert_eq!(parsed.sequence, Some(99));
        assert_eq!(
            parsed.exchange_timestamp,
            Some(TimestampMilliseconds(1_700_000_000_123))
        );
    }

    #[test]
    fn decodes_sbe_best_bid_ask_fields() {
        let best_bid_ask = BinanceSbeCodec
            .decode_best_bid_ask(&sbe_best_bid_ask_frame())
            .unwrap();

        assert_eq!(best_bid_ask.symbol, "btcusdt");
        assert_eq!(best_bid_ask.event_time_us, 1_700_000_000_123_000);
        assert_eq!(
            best_bid_ask.bid_price,
            Decimal::from_str_exact("100.25").unwrap()
        );
        assert_eq!(
            best_bid_ask.bid_quantity,
            Decimal::from_str_exact("2.5").unwrap()
        );
        assert_eq!(
            best_bid_ask.ask_price,
            Decimal::from_str_exact("100.5").unwrap()
        );
        assert_eq!(
            best_bid_ask.ask_quantity,
            Decimal::from_str_exact("1").unwrap()
        );
    }

    #[test]
    fn decodes_binance_sbe_depth_snapshot_last_update_id() {
        let parsed = decode(sbe_depth_snapshot_frame()).unwrap();

        assert_eq!(parsed.stream, "btcusdt@depth20");
        assert_eq!(parsed.sequence, Some(456));
    }

    #[test]
    fn decodes_binance_sbe_depth_diff_and_skips_groups() {
        let parsed = decode(sbe_depth_diff_frame()).unwrap();

        assert_eq!(parsed.stream, "btcusdt@depth");
        assert_eq!(parsed.sequence, Some(123));
    }

    #[test]
    fn rejects_unknown_sbe_templates() {
        let error = decode(sbe_header(1)).unwrap_err();

        assert!(matches!(
            error,
            IngesterError::UnknownSbeTemplate { template_id: 1 }
        ));
    }

    #[test]
    fn rejects_non_binary_frames() {
        let result = BinanceSbeCodec.decode(Message::text("{}".to_string()));

        assert!(matches!(result, Err(ConnectionError::BadMessagePayload)));
    }

    #[test]
    fn encoding_requests_is_unsupported() {
        let result = BinanceSbeCodec.encode(&());

        assert!(matches!(result, Err(ConnectionError::Codec(_))));
    }
}
