use super::{
    codec::{BinanceSbeBestBidAsk, BinanceSbeCodec},
    frame::{
        BINANCE_BEST_BID_ASK_TEMPLATE_ID, BINANCE_DEPTH_DIFF_TEMPLATE_ID,
        BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID, BINANCE_TRADES_TEMPLATE_ID,
    },
    groups::SbeGroupExt,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::IngesterError;
use platform_market_data::ParsedMessage;
use platform_market_data::binary_cursor::BinaryCursor;
use platform_types::TimestampMilliseconds;

impl BinanceSbeCodec {
    pub(super) fn decode_frame(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let (template_id, mut cursor) = self.frame_cursor(frame)?;

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
        let (sequence, best_bid_ask) = self.best_bid_ask_fields(cursor)?;
        Ok(self.parsed(
            frame,
            &best_bid_ask.symbol,
            "bestBidAsk",
            Some(sequence),
            Some(best_bid_ask.event_time_us),
        ))
    }

    pub(super) fn best_bid_ask_fields(
        &self,
        cursor: &mut BinaryCursor<'_>,
    ) -> Result<(u64, BinanceSbeBestBidAsk), IngesterError> {
        let event_time_us = cursor.read_i64()?;
        let sequence = cursor.read_sequence()?;
        let price_exponent = cursor.read_i8()?;
        let qty_exponent = cursor.read_i8()?;
        let bid_price = Self::decimal_from_mantissa(cursor.read_i64()?, price_exponent)?;
        let bid_quantity = Self::decimal_from_mantissa(cursor.read_i64()?, qty_exponent)?;
        let ask_price = Self::decimal_from_mantissa(cursor.read_i64()?, price_exponent)?;
        let ask_quantity = Self::decimal_from_mantissa(cursor.read_i64()?, qty_exponent)?;
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
