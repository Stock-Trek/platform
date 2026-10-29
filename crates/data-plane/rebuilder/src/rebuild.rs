use platform_aggregator::{
    processor::{CandleProcessor, ProcessorConfig},
    state::AggregatorState,
};
use platform_errors::BpResult;
use platform_market_data::{
    candle::ClosedCandle,
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
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

/// Replays archived events through the same pure fold used by the live
/// aggregator.
///
/// Events are grouped by exchange stream before replay. Compacted event files
/// are already sorted by stream, timestamp, and sequence, but grouping also
/// makes raw files deterministic without allowing one stream's high watermark
/// to make another stream's earlier events late.
pub fn rebuild_candles(
    events: Vec<MarketEvent>,
    options: &RebuildOptions,
) -> BpResult<RebuildResult> {
    let mut stats = RebuildStats {
        events_read: events.len(),
        ..RebuildStats::default()
    };
    let mut groups: BTreeMap<(String, String), Vec<MarketEvent>> = BTreeMap::new();
    for event in events {
        groups
            .entry((event.exchange.to_string(), event.stream.clone()))
            .or_default()
            .push(event);
    }
    stats.trade_stream_groups = groups.len();

    let max_interval = options
        .processor
        .intervals
        .iter()
        .map(|interval| interval.duration().0)
        .max()
        .unwrap_or_default();
    let allowed_lateness = options.processor.allowed_lateness.0;
    let mut decoders: HashMap<CexId, SharedTradeDecoder> = HashMap::new();
    let mut regenerated = Vec::new();

    for (_, mut group) in groups {
        group.sort_by(|left, right| {
            event_timestamp(left)
                .0
                .cmp(&event_timestamp(right).0)
                .then_with(|| {
                    left.sequence
                        .unwrap_or_default()
                        .cmp(&right.sequence.unwrap_or_default())
                })
                .then_with(|| left.payload.cmp(&right.payload))
        });

        let mut processor = CandleProcessor::with_config(options.processor.clone());
        let state: &mut AggregatorState = processor.state_mut();
        state.state_epoch = options.state_epoch;

        // Raw archive files can contain the same event more than once. Drop
        // duplicates before assigning replay offsets so redundant rows do not
        // shift the provenance of later candle revisions.
        let mut seen_sequences = HashSet::new();
        let mut offset = 0_u64;
        let mut last_timestamp = None;
        for event in &group {
            if let Some(sequence) = event.sequence
                && !seen_sequences.insert(sequence)
            {
                stats.duplicate_events += 1;
                continue;
            }
            let event_offset = offset;
            offset = offset.saturating_add(1);
            last_timestamp = Some(event_timestamp(event));

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

            let trades = match decoder.decode(event) {
                Ok(trades) => trades,
                Err(error) => {
                    stats.decode_failures += 1;
                    warn!(
                        exchange = %event.exchange,
                        stream = %event.stream,
                        sequence = ?event.sequence,
                        %error,
                        "skipping undecodable archived event"
                    );
                    continue;
                }
            };
            // Match the live runner by marking the offset before folding the
            // event, so candles closed by this event carry its provenance.
            processor.state_mut().mark_processed(event_offset);
            let update = processor.apply_event(event, &trades);
            stats.trades_applied += trades.len();
            stats.duplicate_events += update.duplicates;
            stats.late_trades += update.late_trades;
            regenerated.extend(update.closed);
        }

        if options.finish_open_windows
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
    candles.sort_by(|left, right| left.revision.cmp(&right.revision));
    stats.unique_candles = candles.len();

    Ok(RebuildResult { candles, stats })
}

fn event_timestamp(event: &MarketEvent) -> TimestampMilliseconds {
    event.exchange_timestamp.unwrap_or(event.received_at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use platform_market_data::candle::CandleInterval;
    use platform_types::new_types::DurationMilliseconds;

    fn event(sequence: u64, timestamp: i64) -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(timestamp)),
            received_at: TimestampMilliseconds(timestamp),
            payload: STANDARD.encode(format!(
                r#"{{"e":"trade","E":{timestamp},"s":"BTCUSDT","t":{sequence},"p":"100","q":"1","T":{timestamp}}}"#
            )),
        }
    }

    fn options() -> RebuildOptions {
        RebuildOptions {
            processor: ProcessorConfig::new(
                vec![CandleInterval::from_label("1m").unwrap()],
                DurationMilliseconds::ZERO,
            ),
            state_epoch: 7,
            finish_open_windows: true,
        }
    }

    #[test]
    fn replay_is_deterministic_and_assigns_the_configured_epoch() {
        let events = vec![
            event(1, 1_000),
            event(2, 61_000),
            event(3, 122_000),
            event(4, 183_000),
        ];

        let first = rebuild_candles(events.clone(), &options()).unwrap();
        let second = rebuild_candles(events, &options()).unwrap();

        assert_eq!(first.stats.events_read, 4);
        assert_eq!(first.stats.trade_stream_groups, 1);
        assert_eq!(first.stats.trades_applied, 4);
        assert_eq!(first.stats.decode_failures, 0);
        assert_eq!(first.stats.unique_candles, 4);
        assert_eq!(
            first
                .candles
                .iter()
                .map(ClosedCandle::message_id)
                .collect::<Vec<_>>(),
            second
                .candles
                .iter()
                .map(ClosedCandle::message_id)
                .collect::<Vec<_>>()
        );
        assert!(
            first
                .candles
                .iter()
                .all(|candle| candle.revision.state_epoch == 7)
        );
    }

    #[test]
    fn duplicate_sequences_do_not_change_the_regenerated_candles() {
        let unique = rebuild_candles(
            vec![event(1, 1_000), event(2, 61_000), event(3, 122_000)],
            &options(),
        )
        .unwrap();
        let duplicated = rebuild_candles(
            vec![
                event(1, 1_000),
                event(2, 61_000),
                event(2, 61_000),
                event(3, 122_000),
            ],
            &options(),
        )
        .unwrap();

        assert_eq!(duplicated.stats.duplicate_events, 1);
        assert_eq!(
            unique
                .candles
                .iter()
                .map(ClosedCandle::message_id)
                .collect::<Vec<_>>(),
            duplicated
                .candles
                .iter()
                .map(ClosedCandle::message_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn streams_are_replayed_independently() {
        let mut other = event(1, 1_000);
        other.stream = "ethusdt@trade".to_string();
        other.payload = STANDARD
            .encode(r#"{"e":"trade","E":1000,"s":"ETHUSDT","t":1,"p":"10","q":"2","T":1000}"#);
        let events = vec![event(1, 1_000), event(2, 61_000), other];

        let result = rebuild_candles(events, &options()).unwrap();

        assert_eq!(result.stats.trade_stream_groups, 2);
        assert_eq!(result.stats.trades_applied, 3);
        assert_eq!(result.stats.late_trades, 0);
    }

    #[test]
    fn finishing_is_optional() {
        let events = vec![event(1, 1_000)];
        let mut unfinished = options();
        unfinished.finish_open_windows = false;

        let finished = rebuild_candles(events.clone(), &options()).unwrap();
        let unfinished = rebuild_candles(events, &unfinished).unwrap();

        assert_eq!(finished.candles.len(), 1);
        assert!(unfinished.candles.is_empty());
    }
}
