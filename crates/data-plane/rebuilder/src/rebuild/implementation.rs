use platform_aggregator::{
    processor::{CandleProcessor, ProcessorConfig},
    state::AggregatorState,
};
use platform_errors::BpResult;
use platform_market_data::{
    candle::{AlignedWindowExt, ClosedCandle},
    decoders::trade::{SharedTradeDecoder, Trade, TradeDecoderFactory},
    event::MarketEvent,
};
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
}

#[derive(Debug, Clone)]
pub struct RebuildResult {
    pub candles: Vec<ClosedCandle>,
    pub stats: RebuildStats,
}

type PairKey = (String, String, String);

struct ReplayEvent {
    event: MarketEvent,
    trades: Vec<Trade>,
}

/// Replays archived events through the same pure fold used by the live
/// aggregator.
///
/// Events are grouped by their decoded `(exchange, trading pair)` before
/// replay. The live router sends every stream carrying a pair to the same
/// shard, so `@trade` and `@aggTrade` events must update the same candles.
/// Grouping by pair also keeps one stream's high watermark from making another
/// pair's earlier events late, while still supporting raw files that are not
/// globally ordered.
impl RebuildOptions {
    pub fn rebuild(&self, events: Vec<MarketEvent>) -> BpResult<RebuildResult> {
        let mut stats = RebuildStats {
            events_read: events.len(),
            ..RebuildStats::default()
        };
        let mut decoders: HashMap<CexId, SharedTradeDecoder> = HashMap::new();
        let mut groups: BTreeMap<PairKey, Vec<ReplayEvent>> = BTreeMap::new();

        for event in events {
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
                        continue;
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
                    continue;
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
            for (key, trades) in trades_by_pair {
                groups.entry(key).or_default().push(ReplayEvent {
                    event: event.clone(),
                    trades,
                });
            }
        }
        stats.trade_stream_groups = groups.len();

        let max_interval = self
            .processor
            .intervals
            .iter()
            .map(|interval| interval.duration().0)
            .max()
            .unwrap_or_default();
        let allowed_lateness = self.processor.allowed_lateness.0;
        let mut regenerated = Vec::new();

        for (_, mut group) in groups {
            group.sort_by(|left, right| {
                Self::event_timestamp(&left.event)
                    .0
                    .cmp(&Self::event_timestamp(&right.event).0)
                    .then_with(|| {
                        left.event
                            .sequence
                            .unwrap_or_default()
                            .cmp(&right.event.sequence.unwrap_or_default())
                    })
                    .then_with(|| left.event.stream.cmp(&right.event.stream))
                    .then_with(|| left.event.payload.cmp(&right.event.payload))
            });

            let mut processor = CandleProcessor::with_config(self.processor.clone());
            let state: &mut AggregatorState = processor.state_mut();
            state.state_epoch = self.state_epoch;

            // Raw archive files can contain the same event more than once. Drop
            // duplicates before assigning replay offsets so redundant rows do not
            // shift the provenance of later candle revisions. Sequence numbers are
            // scoped to a stream; `@trade` and `@aggTrade` may both use sequence 1.
            let mut seen_sequences = HashSet::new();
            let mut offset = 0_u64;
            let mut last_timestamp = None;
            for replay in &group {
                let event = &replay.event;
                if let Some(sequence) = event.sequence
                    && !seen_sequences.insert((event.stream.as_str(), sequence))
                {
                    stats.duplicate_events += 1;
                    continue;
                }
                let event_offset = offset;
                offset = offset.saturating_add(1);
                last_timestamp = Some(Self::event_timestamp(event));

                // Match the live runner by marking the offset before folding the
                // event, so candles closed by this event carry its provenance.
                processor.state_mut().mark_processed(event_offset);
                let update = processor.apply_event(event, &replay.trades);
                stats.trades_applied += replay.trades.len();
                stats.duplicate_events += update.duplicates;
                stats.late_trades += update.late_trades;
                regenerated.extend(update.closed);
            }

            if self.finish_open_windows
                && let Some(last_timestamp) = last_timestamp
            {
                let flush_at = last_timestamp
                    .0
                    .saturating_add(max_interval)
                    .saturating_add(allowed_lateness);
                regenerated
                    .extend(processor.flush_closed_at_or_before(TimestampMilliseconds(flush_at)));
            }
        }

        stats.closed_candles = regenerated.len();
        let mut unique = BTreeMap::new();
        for candle in regenerated {
            unique.insert(candle.message_id(), candle);
        }
        let mut candles = unique.into_values().collect::<Vec<_>>();
        candles.sort_by(|left, right| left.revision().cmp(right.revision()));
        stats.unique_candles = candles.len();

        Ok(RebuildResult { candles, stats })
    }

    fn event_timestamp(event: &MarketEvent) -> TimestampMilliseconds {
        event.exchange_timestamp.unwrap_or(event.received_at)
    }
}
