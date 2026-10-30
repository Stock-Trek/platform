use super::ArchiveBatch;
use crate::record::{ArchiveKind, ArchiveRecord, ArchivedMarketEvent};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use platform_errors::{BpError, BpResult};
use platform_market_data::{candle::ClosedCandle, envelope::EnvelopeCodec};
use std::sync::Arc;

impl ArchiveBatch {
    pub fn to_record_batch(&self) -> BpResult<RecordBatch> {
        Self::record_batch(self.kind, &self.records)
    }

    pub fn record_batch(kind: ArchiveKind, records: &[ArchiveRecord]) -> BpResult<RecordBatch> {
        match kind {
            ArchiveKind::Events => {
                let events = Self::events(records)?;
                let columns: Vec<ArrayRef> = vec![
                    Self::strings(events.iter().map(|event| event.event.exchange.to_string())),
                    Self::strings(events.iter().map(|event| event.event.stream.clone())),
                    Arc::new(UInt64Array::from_iter(
                        events.iter().map(|event| event.event.sequence),
                    )),
                    Arc::new(Int64Array::from_iter(events.iter().map(|event| {
                        event.event.exchange_timestamp.map(|timestamp| timestamp.0)
                    }))),
                    Arc::new(Int64Array::from_iter_values(
                        events.iter().map(|event| event.event.received_at.0),
                    )),
                    Self::strings(events.iter().map(|event| event.event.payload.clone())),
                    Arc::new(UInt64Array::from_iter_values(
                        events.iter().map(|event| event.state_epoch),
                    )),
                    Arc::new(UInt64Array::from_iter_values(
                        events.iter().map(|event| event.routing_key().value()),
                    )),
                ];
                Ok(RecordBatch::try_new(Self::schema(kind), columns)?)
            }
            ArchiveKind::Candles => {
                let candles = Self::candles(records)?;
                let revisions = candles
                    .iter()
                    .map(|closed| {
                        let payload = closed.revision.encode()?;
                        Ok(std::str::from_utf8(&payload)
                            .map_err(BpError::external)?
                            .to_string())
                    })
                    .collect::<BpResult<Vec<_>>>()?;
                let revision_ids = candles
                    .iter()
                    .map(|closed| closed.message_id())
                    .collect::<Vec<_>>();
                let columns: Vec<ArrayRef> = vec![
                    Self::strings(
                        candles
                            .iter()
                            .map(|closed| closed.candle.key.exchange.to_string()),
                    ),
                    Self::strings(
                        candles
                            .iter()
                            .map(|closed| closed.candle.key.trading_pair.base.to_string()),
                    ),
                    Self::strings(
                        candles
                            .iter()
                            .map(|closed| closed.candle.key.trading_pair.quote.to_string()),
                    ),
                    Self::strings(
                        candles
                            .iter()
                            .map(|closed| closed.candle.key.interval.label()),
                    ),
                    Arc::new(Int64Array::from_iter_values(candles.iter().map(|closed| {
                        i64::try_from(closed.candle.market_candle.start_time_millis_inc)
                            .unwrap_or(i64::MAX)
                    }))),
                    Arc::new(Int64Array::from_iter_values(candles.iter().map(|closed| {
                        i64::try_from(closed.candle.market_candle.end_time_millis_exc)
                            .unwrap_or(i64::MAX)
                    }))),
                    Arc::new(Int64Array::from_iter_values(candles.iter().map(|closed| {
                        i64::try_from(closed.candle.market_candle.duration_millis)
                            .unwrap_or(i64::MAX)
                    }))),
                    Arc::new(BooleanArray::from_iter(candles.iter().map(|closed| {
                        Some(closed.candle.market_candle.is_candle_closed)
                    }))),
                    Arc::new(Float64Array::from_iter_values(
                        candles
                            .iter()
                            .map(|closed| closed.candle.market_candle.ohlcv.open),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        candles
                            .iter()
                            .map(|closed| closed.candle.market_candle.ohlcv.high),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        candles
                            .iter()
                            .map(|closed| closed.candle.market_candle.ohlcv.low),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        candles
                            .iter()
                            .map(|closed| closed.candle.market_candle.ohlcv.close),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        candles
                            .iter()
                            .map(|closed| closed.candle.market_candle.ohlcv.volume),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        candles
                            .iter()
                            .map(|closed| closed.candle.market_candle.ohlcv.quote_volume),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        candles
                            .iter()
                            .map(|closed| closed.candle.market_candle.ohlcv.vwap),
                    )),
                    Arc::new(UInt64Array::from_iter_values(
                        candles
                            .iter()
                            .map(|closed| closed.candle.market_candle.trade_count),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        candles
                            .iter()
                            .map(|closed| closed.revision.close_watermark.0),
                    )),
                    Arc::new(UInt64Array::from_iter_values(
                        candles.iter().map(|closed| closed.revision.source_offset),
                    )),
                    Arc::new(UInt64Array::from_iter_values(
                        candles.iter().map(|closed| closed.revision.state_epoch),
                    )),
                    Self::strings(revision_ids),
                    Self::strings(revisions),
                    Arc::new(BooleanArray::from_iter(
                        candles.iter().map(|closed| Some(closed.candle.is_complete)),
                    )),
                    Arc::new(UInt64Array::from_iter_values(candles.iter().map(
                        |closed| {
                            crate::routing::RoutingKey::for_pair(
                                closed.candle.key.exchange,
                                &closed.candle.key.trading_pair,
                            )
                            .value()
                        },
                    ))),
                ];
                Ok(RecordBatch::try_new(Self::schema(kind), columns)?)
            }
        }
    }

    fn events(records: &[ArchiveRecord]) -> BpResult<Vec<&ArchivedMarketEvent>> {
        records
            .iter()
            .map(|record| match record {
                ArchiveRecord::MarketEvent(event) => Ok(event),
                ArchiveRecord::ClosedCandle(_) => {
                    Err(Self::kind_mismatch(ArchiveKind::Events, record))
                }
            })
            .collect()
    }

    fn candles(records: &[ArchiveRecord]) -> BpResult<Vec<&ClosedCandle>> {
        records
            .iter()
            .map(|record| match record {
                ArchiveRecord::ClosedCandle(closed) => Ok(closed),
                ArchiveRecord::MarketEvent(_) => {
                    Err(Self::kind_mismatch(ArchiveKind::Candles, record))
                }
            })
            .collect()
    }

    fn strings(values: impl IntoIterator<Item = String>) -> ArrayRef {
        Arc::new(StringArray::from_iter_values(values))
    }
}
