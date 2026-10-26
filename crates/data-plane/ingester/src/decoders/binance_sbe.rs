use crate::{
    decoders::decoder::{BinaryDecoder, FrameDecoder},
    exchange::CexIdExt,
    source::ParsedMessage,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::IngesterError;
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::cex_id::CexId;

const BINANCE_SBE_SCHEMA_ID: u16 = 1;

const BINANCE_TRADES_TEMPLATE_ID: u16 = 10_000;
const BINANCE_BEST_BID_ASK_TEMPLATE_ID: u16 = 10_001;
const BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID: u16 = 10_002;
const BINANCE_DEPTH_DIFF_TEMPLATE_ID: u16 = 10_003;

/// Decoder for Binance's SBE market data streams.
///
/// Schema: <https://github.com/binance/binance-spot-api-docs/blob/master/sbe/schemas/stream_1_0.xml>
pub struct BinanceSbeDecoder {
    exchange: CexId,
}

impl BinanceSbeDecoder {
    pub fn new(exchange: CexId) -> Self {
        Self { exchange }
    }

    fn decode_frame(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let exchange = self.exchange.slug();
        let cursor = SbeCursor::new(frame, &exchange);
        let schema_id = cursor.read_u16(4)?;
        if schema_id != BINANCE_SBE_SCHEMA_ID {
            return Err(IngesterError::malformed_sbe(
                exchange.as_str(),
                format!("expected schema id {BINANCE_SBE_SCHEMA_ID}, got {schema_id}"),
            ));
        }

        match cursor.read_u16(2)? {
            BINANCE_TRADES_TEMPLATE_ID => self.trades_event(&cursor, frame),
            BINANCE_BEST_BID_ASK_TEMPLATE_ID => self.best_bid_ask_event(&cursor, frame),
            BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID => self.depth_snapshot_event(&cursor, frame),
            BINANCE_DEPTH_DIFF_TEMPLATE_ID => self.depth_diff_event(&cursor, frame),
            template_id => Err(IngesterError::unknown_sbe_template(
                exchange.as_str(),
                template_id,
            )),
        }
    }

    fn trades_event(
        &self,
        cursor: &SbeCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64(8)?;
        let (block_length, count, entries) = cursor.group_size32(26)?;
        let sequence = if count == 0 {
            None
        } else {
            let last_trade = cursor.advance(entries, count - 1, block_length)?;
            Some(cursor.read_sequence(last_trade)?)
        };
        let symbol_offset = cursor.advance(entries, count, block_length)?;
        let symbol = cursor.read_var_string(symbol_offset)?;
        Ok(self.parsed(frame, &symbol, "trade", sequence, Some(event_time_us)))
    }

    fn best_bid_ask_event(
        &self,
        cursor: &SbeCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64(8)?;
        let sequence = Some(cursor.read_sequence(16)?);
        let symbol = cursor.read_var_string(58)?;
        Ok(self.parsed(frame, &symbol, "bestBidAsk", sequence, Some(event_time_us)))
    }

    fn depth_snapshot_event(
        &self,
        cursor: &SbeCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64(8)?;
        let after_bids = cursor.end_of_group16(26)?;
        let after_asks = cursor.end_of_group16(after_bids)?;
        let symbol = cursor.read_var_string(after_asks)?;
        // TODO: snapshots are independent of the incremental update sequence.
        Ok(self.parsed(frame, &symbol, "depth20", None, Some(event_time_us)))
    }

    fn depth_diff_event(
        &self,
        cursor: &SbeCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64(8)?;
        let sequence = Some(cursor.read_sequence(24)?);
        let after_bids = cursor.end_of_group16(34)?;
        let after_asks = cursor.end_of_group16(after_bids)?;
        let symbol = cursor.read_var_string(after_asks)?;
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

impl FrameDecoder for BinanceSbeDecoder {
    fn exchange(&self) -> CexId {
        self.exchange
    }

    fn decode(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        self.decode_frame(frame)
    }
}

impl BinaryDecoder for BinanceSbeDecoder {}

struct SbeCursor<'a> {
    frame: &'a [u8],
    exchange: &'a str,
}

impl<'a> SbeCursor<'a> {
    fn new(frame: &'a [u8], exchange: &'a str) -> Self {
        Self { frame, exchange }
    }

    fn get(&self, offset: usize, length: usize) -> Result<&'a [u8], IngesterError> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "frame offset overflow"))?;
        self.frame.get(offset..end).ok_or_else(|| {
            IngesterError::malformed_sbe(
                self.exchange,
                format!("frame is too short for {length} bytes at offset {offset}"),
            )
        })
    }

    fn read_u8(&self, offset: usize) -> Result<u8, IngesterError> {
        Ok(self.get(offset, 1)?[0])
    }

    fn read_u16(&self, offset: usize) -> Result<u16, IngesterError> {
        let bytes: [u8; 2] = self
            .get(offset, 2)?
            .try_into()
            .map_err(|_| IngesterError::malformed_sbe(self.exchange, "invalid u16"))?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&self, offset: usize) -> Result<u32, IngesterError> {
        let bytes: [u8; 4] = self
            .get(offset, 4)?
            .try_into()
            .map_err(|_| IngesterError::malformed_sbe(self.exchange, "invalid u32"))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i64(&self, offset: usize) -> Result<i64, IngesterError> {
        let bytes: [u8; 8] = self
            .get(offset, 8)?
            .try_into()
            .map_err(|_| IngesterError::malformed_sbe(self.exchange, "invalid i64"))?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_sequence(&self, offset: usize) -> Result<u64, IngesterError> {
        let value = self.read_i64(offset)?;
        u64::try_from(value).map_err(|_| {
            IngesterError::malformed_sbe(
                self.exchange,
                format!("negative sequence {value} at offset {offset}"),
            )
        })
    }

    fn read_var_string(&self, offset: usize) -> Result<String, IngesterError> {
        let length = self.read_u8(offset)? as usize;
        let start = offset
            .checked_add(1)
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "string offset overflow"))?;
        let bytes = self.get(start, length)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|source| IngesterError::utf8(self.exchange, source))?;
        Ok(value.to_string())
    }

    fn group_size32(&self, offset: usize) -> Result<(usize, usize, usize), IngesterError> {
        let block_length = self.read_u16(offset)? as usize;
        let count = self.read_u32(offset + 2)? as usize;
        let entries = offset
            .checked_add(6)
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "group offset overflow"))?;
        Ok((block_length, count, entries))
    }

    fn end_of_group16(&self, offset: usize) -> Result<usize, IngesterError> {
        let block_length = self.read_u16(offset)? as usize;
        let count = self.read_u16(offset + 2)? as usize;
        let entries = offset
            .checked_add(4)
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "group offset overflow"))?;
        self.advance(entries, count, block_length)
    }

    fn advance(&self, base: usize, count: usize, stride: usize) -> Result<usize, IngesterError> {
        let length = count
            .checked_mul(stride)
            .and_then(|length| base.checked_add(length))
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "group size overflow"))?;
        if length > self.frame.len() {
            return Err(IngesterError::malformed_sbe(
                self.exchange,
                format!(
                    "group ends at {length} beyond frame length {}",
                    self.frame.len()
                ),
            ));
        }
        Ok(length)
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

    #[test]
    fn decodes_binance_sbe_trades() {
        let frame = sbe_trades_frame();
        let decoder = BinanceSbeDecoder::new(CexId::Binance);

        let parsed = decoder.decode(&frame).unwrap();

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
        let frame = sbe_best_bid_ask_frame();
        let decoder = BinanceSbeDecoder::new(CexId::Binance);

        let parsed = decoder.decode(&frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@bestBidAsk");
        assert_eq!(parsed.sequence, Some(99));
    }

    #[test]
    fn decodes_binance_sbe_depth_diff_and_skips_groups() {
        let frame = sbe_depth_diff_frame();
        let decoder = BinanceSbeDecoder::new(CexId::Binance);

        let parsed = decoder.decode(&frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@depth");
        assert_eq!(parsed.sequence, Some(123));
    }

    #[test]
    fn rejects_unknown_sbe_templates() {
        let frame = sbe_header(1);
        let decoder = BinanceSbeDecoder::new(CexId::Binance);

        let error = decoder.decode(&frame).unwrap_err();

        assert!(matches!(
            error,
            IngesterError::UnknownSbeTemplate { template_id: 1, .. }
        ));
    }
}
