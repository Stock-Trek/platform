use platform_aggregator::{
    processor::{CandleProcessor, ProcessorConfig},
    state::AggregatorState,
};
use platform_errors::BpResult;
use platform_market_data::{
    candle::ClosedCandle,
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
pub fn rebuild_candles(
    events: Vec<MarketEvent>,
    options: &RebuildOptions,
) -> BpResult<RebuildResult> {
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

    let max_interval = options
        .processor
        .intervals
        .iter()
        .map(|interval| interval.duration().0)
        .max()
        .unwrap_or_default();
    let allowed_lateness = options.processor.allowed_lateness.0;
    let mut regenerated = Vec::new();

    for (_, mut group) in groups {
        group.sort_by(|left, right| {
            event_timestamp(&left.event)
                .0
                .cmp(&event_timestamp(&right.event).0)
                .then_with(|| {
                    left.event
                        .sequence
                        .unwrap_or_default()
                        .cmp(&right.event.sequence.unwrap_or_default())
                })
                .then_with(|| left.event.stream.cmp(&right.event.stream))
                .then_with(|| left.event.payload.cmp(&right.event.payload))
        });

        let mut processor = CandleProcessor::with_config(options.processor.clone());
        let state: &mut AggregatorState = processor.state_mut();
        state.state_epoch = options.state_epoch;

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
            last_timestamp = Some(event_timestamp(event));

            // Match the live runner by marking the offset before folding the
            // event, so candles closed by this event carry its provenance.
            processor.state_mut().mark_processed(event_offset);
            let update = processor.apply_event(event, &replay.trades);
            stats.trades_applied += replay.trades.len();
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
        stream_event("btcusdt@trade", sequence, timestamp, "100", "1")
    }

    fn stream_event(
        stream: &str,
        sequence: u64,
        timestamp: i64,
        price: &str,
        quantity: &str,
    ) -> MarketEvent {
        let symbol = stream
            .split_once('@')
            .map_or(stream, |(symbol, _)| symbol)
            .to_ascii_uppercase();
        let event_type = if stream.to_ascii_lowercase().ends_with("@aggtrade") {
            "aggTrade"
        } else {
            "trade"
        };
        MarketEvent {
            exchange: CexId::Binance,
            stream: stream.to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(timestamp)),
            received_at: TimestampMilliseconds(timestamp),
            payload: STANDARD.encode(format!(
                r#"{{"e":"{event_type}","E":{timestamp},"s":"{symbol}","p":"{price}","q":"{quantity}","T":{timestamp}}}"#
            )),
        }
    }

    fn event_with_trade_time(
        sequence: u64,
        event_timestamp: i64,
        trade_timestamp: i64,
    ) -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(event_timestamp)),
            received_at: TimestampMilliseconds(event_timestamp),
            payload: STANDARD.encode(format!(
                r#"{{"e":"trade","E":{event_timestamp},"s":"BTCUSDT","p":"100","q":"1","T":{trade_timestamp}}}"#
            )),
        }
    }

    fn options() -> RebuildOptions {
        RebuildOptions {
            processor: ProcessorConfig::without_retention(
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
    fn multi_stream_pairs_share_one_processor_and_candle() {
        // @trade and @aggTrade use independent sequence spaces, so equal
        // sequence numbers must not be mistaken for duplicate events.
        let events = vec![
            stream_event("btcusdt@trade", 1, 1_000, "100", "1"),
            stream_event("btcusdt@aggTrade", 1, 1_000, "200", "2"),
        ];

        let result = rebuild_candles(events, &options()).unwrap();

        assert_eq!(result.stats.trade_stream_groups, 1);
        assert_eq!(result.stats.trades_applied, 2);
        assert_eq!(result.stats.duplicate_events, 0);
        assert_eq!(result.stats.unique_candles, 1);
        let candle = &result.candles[0].candle;
        assert_eq!(candle.market_candle.trade_count, 2);
        assert_eq!(candle.market_candle.ohlcv.high, 200.0);
        assert_eq!(candle.market_candle.ohlcv.low, 100.0);
        assert_eq!(candle.market_candle.ohlcv.volume, 3.0);
    }

    #[test]
    fn multi_stream_pair_revisions_do_not_collide() {
        let events = vec![
            stream_event("btcusdt@trade", 1, 1_000, "100", "1"),
            stream_event("btcusdt@aggTrade", 1, 1_000, "200", "2"),
            stream_event("btcusdt@trade", 2, 61_000, "100", "1"),
            stream_event("btcusdt@aggTrade", 2, 61_000, "200", "2"),
        ];

        let result = rebuild_candles(events, &options()).unwrap();

        assert_eq!(result.stats.trade_stream_groups, 1);
        assert_eq!(result.stats.unique_candles, 2);
        assert_eq!(
            result
                .candles
                .iter()
                .map(ClosedCandle::message_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            2
        );
    }

    #[test]
    fn pairs_are_replayed_independently() {
        let other = stream_event("ethusdt@trade", 1, 1_000, "10", "2");
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

    #[test]
    fn rebuilt_candles_match_live_processing_for_cap_eviction_and_a_rejected_correction() {
        let interval = CandleInterval::from_label("1m").unwrap();
        let cutoff = DurationMilliseconds(1_000);
        let processor = ProcessorConfig::new(
            vec![interval],
            DurationMilliseconds::ZERO,
            BTreeMap::from([(interval, 1)]),
            cutoff,
        );

        let events = vec![
            event_with_trade_time(1, 61_000, 61_000),
            event_with_trade_time(2, 121_000, 121_000),
            event_with_trade_time(3, 181_000, 181_000),
            event_with_trade_time(4, 181_000, 61_500),
        ];

        let mut live = CandleProcessor::with_config(processor.clone());
        live.state_mut().state_epoch = 7;
        let decoder = TradeDecoderFactory::for_exchange(CexId::Binance).unwrap();
        let mut live_candles = Vec::new();
        for (offset, event) in events.iter().enumerate() {
            live.state_mut().mark_processed(offset as u64);
            let trades = decoder.decode(event).unwrap();
            let update = live.apply_event(event, &trades);
            live_candles.extend(update.closed);
        }

        let mut options = options();
        options.processor = processor;
        options.finish_open_windows = false;
        let rebuilt = rebuild_candles(events, &options).unwrap();

        assert_eq!(rebuilt.candles.len(), 2);
        assert_eq!(
            rebuilt
                .candles
                .iter()
                .map(ClosedCandle::message_id)
                .collect::<Vec<_>>(),
            live_candles
                .iter()
                .map(ClosedCandle::message_id)
                .collect::<Vec<_>>()
        );
        assert!(
            rebuilt
                .candles
                .iter()
                .all(
                    |candle| candle.candle.start_time() != TimestampMilliseconds(60_000)
                        || candle.candle.market_candle.trade_count == 1
                )
        );
    }
}
