use crate::{
    decoders::sbe::{SbeCodec, frame::SbeFrame, groups::SbeGroupExt},
    event::ParsedMessage,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::IngesterError;
use platform_exchange_types::market_data::{
    SbeBestBidAskRecord, SbeSchemaDefinition, SbeTemplateKind, SbeTradeRecord,
};
use platform_types::TimestampMilliseconds;

impl<S: SbeSchemaDefinition> SbeCodec<S> {
    pub(super) fn decode_frame(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let mut decoded = SbeFrame::decode::<S>(frame)?;

        match decoded.template().kind {
            SbeTemplateKind::Trades => self.trades_event(&mut decoded, frame),
            SbeTemplateKind::BestBidAsk => self.best_bid_ask_event(&mut decoded, frame),
            SbeTemplateKind::DepthSnapshot => self.depth_snapshot_event(&mut decoded, frame),
            SbeTemplateKind::DepthDiff => self.depth_diff_event(&mut decoded, frame),
        }
    }

    pub(super) fn trade_entries(
        &self,
        frame: &mut SbeFrame<'_>,
    ) -> Result<Vec<S::Trade>, IngesterError> {
        let cursor = frame.cursor();
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
            .map(|(price, quantity)| S::Trade::new(symbol.clone(), event_time_us, price, quantity))
            .collect())
    }

    fn trades_event(
        &self,
        frame: &mut SbeFrame<'_>,
        bytes: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let cursor = frame.cursor();
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
        Ok(self.parsed(bytes, &symbol, "trade", sequence, Some(event_time_us)))
    }

    fn best_bid_ask_event(
        &self,
        frame: &mut SbeFrame<'_>,
        bytes: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let (sequence, best_bid_ask) = self.best_bid_ask_fields(frame)?;
        Ok(self.parsed(
            bytes,
            best_bid_ask.symbol(),
            "bestBidAsk",
            Some(sequence),
            Some(best_bid_ask.event_time_us()),
        ))
    }

    pub(super) fn best_bid_ask_fields(
        &self,
        frame: &mut SbeFrame<'_>,
    ) -> Result<(u64, S::BestBidAsk), IngesterError> {
        let cursor = frame.cursor();
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
            S::BestBidAsk::new(
                symbol,
                event_time_us,
                bid_price,
                bid_quantity,
                ask_price,
                ask_quantity,
            ),
        ))
    }

    fn depth_snapshot_event(
        &self,
        frame: &mut SbeFrame<'_>,
        bytes: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let cursor = frame.cursor();
        let event_time_us = cursor.read_i64()?;
        let sequence = Some(cursor.read_sequence()?);
        let _price_exponent = cursor.read_u8()?;
        let _qty_exponent = cursor.read_u8()?;
        let bids = cursor.read_group16()?;
        cursor.seek(bids.end()?)?;
        let asks = cursor.read_group16()?;
        cursor.seek(asks.end()?)?;
        let symbol = cursor.read_var_string()?;
        Ok(self.parsed(bytes, &symbol, "depth20", sequence, Some(event_time_us)))
    }

    fn depth_diff_event(
        &self,
        frame: &mut SbeFrame<'_>,
        bytes: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let cursor = frame.cursor();
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
        Ok(self.parsed(bytes, &symbol, "depth", sequence, Some(event_time_us)))
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
