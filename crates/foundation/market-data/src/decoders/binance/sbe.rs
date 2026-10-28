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

impl BinanceSbeCodec {
    fn decode_frame(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
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

        match template_id {
            BINANCE_TRADES_TEMPLATE_ID => self.trades_event(&mut cursor, frame),
            BINANCE_BEST_BID_ASK_TEMPLATE_ID => self.best_bid_ask_event(&mut cursor, frame),
            BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID => self.depth_snapshot_event(&mut cursor, frame),
            BINANCE_DEPTH_DIFF_TEMPLATE_ID => self.depth_diff_event(&mut cursor, frame),
            template_id => Err(IngesterError::unknown_sbe_template(template_id)),
        }
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
        let event_time_us = cursor.read_i64()?;
        let sequence = Some(cursor.read_sequence()?);
        let _price_exponent = cursor.read_u8()?;
        let _qty_exponent = cursor.read_u8()?;
        // TODO: decode the best bid and ask prices and quantities.
        cursor.skip(32)?;
        let symbol = cursor.read_var_string()?;
        Ok(self.parsed(frame, &symbol, "bestBidAsk", sequence, Some(event_time_us)))
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
        frame.push(0);
        frame.push(0);
        frame.extend_from_slice(&[0u8; 32]);
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
    fn decodes_binance_sbe_best_bid_ask() {
        let parsed = decode(sbe_best_bid_ask_frame()).unwrap();

        assert_eq!(parsed.stream, "btcusdt@bestBidAsk");
        assert_eq!(parsed.sequence, Some(99));
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
