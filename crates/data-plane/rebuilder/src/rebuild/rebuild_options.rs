use crate::rebuild::{
    candle_spill::CandleSpill,
    replay_spill::{PairKey, ReplayRecord, ReplayRecords, ReplaySpill},
};
use platform_aggregator::processor::{CandleProcessor, ProcessorConfig};
use platform_errors::BpResult;
use platform_market_data::decoders::trade::{SharedTradeDecoder, Trade, TradeDecoderFactory};
use platform_market_data::event::MarketEvent;
use platform_types::new_types::TimestampMilliseconds;
use std::collections::{BTreeMap, HashMap, HashSet};
use stock_trek_types::cex::cex_id::CexId;
use tracing::warn;

/// Deterministic processing settings for a rebuild.
#[derive(Debug, Clone)]
pub struct RebuildOptions {
    pub processor: ProcessorConfig,
    /// Epoch assigned to every regenerated candle.
    ///
    /// A rebuild starts a new output lineage, so its deterministic revision ids
    /// cannot be suppressed by a previous live lineage.
    pub state_epoch: u64,
    /// Close candles that are still open when the archived input ends.
    ///
    /// Rebuilds normally run over a finite time range, so leaving the final
    /// partial candle open would silently omit it from the regenerated archive.
    pub finish_open_windows: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RebuildStats {
    pub events_read: usize,
    pub trade_stream_groups: usize,
    pub trades_applied: usize,
    pub decode_failures: usize,
    pub duplicate_events: usize,
    pub late_trades: usize,
    pub closed_candles: usize,
    pub unique_candles: usize,
    pub events_spilled: usize,
    pub candles_spilled: usize,
}

/// Replays archived events through the same pure fold used by the live
/// aggregator without materializing the whole run in memory.
///
/// Events are decoded once and staged per decoded `(exchange, trading pair)`
/// in a bounded external sorter. The merged replay is consumed one pair at a
/// time, mirroring the live router: every stream carrying a pair updates the
/// same shard, so `@trade` and `@aggTrade` events must update the same
/// candles. Grouping by pair also keeps one stream's high watermark from
/// making another pair's earlier events late, while still supporting raw
/// files that are not globally ordered.
impl RebuildOptions {
    /// Decodes one archived event and stages its per-pair replay records.
    pub fn push_event(
        &self,
        event: MarketEvent,
        decoders: &mut HashMap<CexId, SharedTradeDecoder>,
        replay: &mut ReplaySpill,
        stats: &mut RebuildStats,
    ) -> BpResult<()> {
        stats.events_read += 1;
        let decoder = match decoders.get(&event.exchange) {
            Some(decoder) => decoder.clone(),
            None => match TradeDecoderFactory::for_exchange(event.exchange) {
                Ok(decoder) => {
                    decoders.insert(event.exchange, decoder.clone());
                    decoder
                }
                Err(error) => {
                    stats.decode_failures += 1;
                    warn!(
                        exchange = %event.exchange,
                        %error,
                        "skipping archived event for unsupported exchange"
                    );
                    return Ok(());
                }
            },
        };

        let trades = match decoder.decode(&event) {
            Ok(trades) => trades,
            Err(error) => {
                stats.decode_failures += 1;
                warn!(
                    stream = %event.stream,
                    sequence = ?event.sequence,
                    %error,
                    "skipping undecodable archived event"
                );
                return Ok(());
            }
        };

        // A single event can theoretically decode to more than one pair. Give
        // each pair only its own trades so the replay mirrors routing.
        let mut trades_by_pair: BTreeMap<PairKey, Vec<Trade>> = BTreeMap::new();
        for trade in trades {
            let key = (
                trade.exchange.to_string(),
                trade.trading_pair.base.to_string(),
                trade.trading_pair.quote.to_string(),
            );
            trades_by_pair.entry(key).or_default().push(trade);
        }
        for (pair, trades) in trades_by_pair {
            replay.push(ReplayRecord {
                pair,
                event: event.clone(),
                trades,
            })?;
        }
        Ok(())
    }

    /// Replays the merged event stream and spills every closed candle in the
    /// sink's object order.
    pub fn replay(
        &self,
        events: ReplayRecords,
        candles: &mut CandleSpill,
        stats: &mut RebuildStats,
    ) -> BpResult<()> {
        let max_interval = self
            .processor
            .intervals
            .iter()
            .map(|interval| i64::try_from(interval.millis().upper_bound()).unwrap_or(i64::MAX))
            .max()
            .unwrap_or_default();
        let allowed_lateness = self.processor.allowed_lateness.0;

        let mut processor: Option<CandleProcessor> = None;
        let mut current_pair: Option<PairKey> = None;
        let mut seen_sequences = HashSet::new();
        let mut offset = 0_u64;
        let mut last_timestamp: Option<TimestampMilliseconds> = None;

        let mut events = events;
        while let Some(record) = events.next_record()? {
            if current_pair.as_ref() != Some(&record.pair) {
                Self::finish_group(
                    self.finish_open_windows,
                    max_interval,
                    allowed_lateness,
                    processor.as_mut(),
                    last_timestamp,
                    candles,
                    stats,
                )?;
                stats.trade_stream_groups += 1;
                let mut next = CandleProcessor::with_config(self.processor.clone());
                next.state_mut().state_epoch = self.state_epoch;
                processor = Some(next);
                current_pair = Some(record.pair.clone());
                seen_sequences.clear();
                offset = 0;
                last_timestamp = None;
            }

            let processor = processor
                .as_mut()
                .expect("pair group always creates a processor");
            let event = &record.event;

            // Raw archive files can contain the same event more than once. Drop
            // duplicates before assigning replay offsets so redundant rows do not
            // shift the provenance of later candle revisions. Sequence numbers are
            // scoped to a stream; `@trade` and `@aggTrade` may both use sequence 1.
            if let Some(sequence) = event.sequence
                && !seen_sequences.insert((event.stream.clone(), sequence))
            {
                stats.duplicate_events += 1;
                continue;
            }
            let event_offset = offset;
            offset = offset.saturating_add(1);
            last_timestamp = Some(ReplayRecord::event_timestamp(event));

            // Match the live runner by marking the offset before folding the
            // event, so candles closed by this event carry its provenance.
            processor.state_mut().mark_processed(event_offset);
            let update = processor.apply_event(event, &record.trades);
            stats.trades_applied += record.trades.len();
            stats.duplicate_events += update.duplicates;
            stats.late_trades += update.late_trades;
            for candle in update.closed {
                candles.push(candle)?;
                stats.closed_candles += 1;
            }
        }

        Self::finish_group(
            self.finish_open_windows,
            max_interval,
            allowed_lateness,
            processor.as_mut(),
            last_timestamp,
            candles,
            stats,
        )?;
        Ok(())
    }

    fn finish_group(
        finish_open_windows: bool,
        max_interval: i64,
        allowed_lateness: i64,
        processor: Option<&mut CandleProcessor>,
        last_timestamp: Option<TimestampMilliseconds>,
        candles: &mut CandleSpill,
        stats: &mut RebuildStats,
    ) -> BpResult<()> {
        let Some(processor) = processor else {
            return Ok(());
        };
        if finish_open_windows && let Some(last_timestamp) = last_timestamp {
            let flush_at = last_timestamp
                .0
                .saturating_add(max_interval)
                .saturating_add(allowed_lateness);
            for candle in processor.flush_closed_at_or_before(TimestampMilliseconds(flush_at)) {
                candles.push(candle)?;
                stats.closed_candles += 1;
            }
        }
        Ok(())
    }
}
