use super::*;
use platform_market_data::candle::{AlignedWindow, Candle, parse_interval};
use platform_market_data::decoders::trade::Trade;
use platform_market_data::event::GapMarker;
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use stock_trek_types::cex::cex_id::CexId;
use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

fn pair() -> TradingPair {
    TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
}

fn trade(timestamp: i64) -> Trade {
    Trade {
        exchange: CexId::Binance,
        trading_pair: pair(),
        price: Decimal::from_str_exact("100").unwrap(),
        quantity: Decimal::ONE,
        timestamp: TimestampMilliseconds(timestamp),
    }
}

fn candle() -> Candle {
    Candle::open(&trade(61_000), parse_interval("1m").unwrap()).unwrap()
}

#[test]
fn new_state_has_the_current_schema_and_is_empty() {
    let state = AggregatorState::new();

    assert_eq!(state.schema_version, STATE_SCHEMA_VERSION);
    assert!(state.is_schema_compatible());
    assert_eq!(state.state_epoch, 0);
    assert!(state.open_candles.is_empty());
    assert!(state.closed_candles.is_empty());
    assert_eq!(state.last_processed_sequence, None);
    assert_eq!(state.watermark, None);
    assert!(state.streams.is_empty());
    assert!(state.known_gaps.is_empty());
    assert!(state.incomplete_windows.is_empty());
}

#[test]
fn watermarks_only_move_forward() {
    let mut state = AggregatorState::new();

    assert!(state.advance_watermark(TimestampMilliseconds(100)));
    assert!(!state.advance_watermark(TimestampMilliseconds(90)));
    assert_eq!(state.watermark, Some(TimestampMilliseconds(100)));
    assert!(state.advance_watermark(TimestampMilliseconds(101)));
    assert_eq!(state.watermark, Some(TimestampMilliseconds(101)));
}

#[test]
fn processed_sequences_are_monotonic() {
    let mut state = AggregatorState::new();

    state.mark_processed(4);
    state.mark_processed(2);
    state.mark_processed(7);

    assert_eq!(state.last_processed_sequence, Some(7));
    assert_eq!(state.last_processed_offset(), Some(7));
}

#[test]
fn duplicate_sequences_are_detected_per_stream() {
    let mut state = AggregatorState::new();
    let first = DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 7);
    let other_stream = DeduplicationKey::new(CexId::Binance, "ethusdt@trade", 7);

    assert!(state.observe_sequence(&first));
    assert!(!state.observe_sequence(&first));
    assert!(state.observe_sequence(&other_stream));
    assert!(state.is_duplicate(&first));
    assert_eq!(state.streams[&first.stream_id()].last_sequence, Some(7));
}

#[test]
fn non_adjacent_sequences_keep_separate_ranges() {
    let mut progress = StreamProgress::default();

    assert!(progress.observe_sequence(1));
    assert!(progress.observe_sequence(3));
    assert!(progress.observe_sequence(2));

    assert_eq!(progress.seen_sequences, vec![SequenceRange::new(1, 3)]);
    assert_eq!(progress.last_sequence, Some(3));
}

#[test]
fn sparse_sequences_have_bounded_deduplication_ranges() {
    let mut progress = StreamProgress::default();
    let observed = MAX_SEEN_SEQUENCE_RANGES as u64 * 3;

    for step in 0..observed {
        assert!(progress.observe_sequence(step * 2));
    }

    assert_eq!(progress.seen_sequences.len(), MAX_SEEN_SEQUENCE_RANGES);
    assert!(!progress.is_duplicate(0));
    assert!(progress.is_duplicate((observed - 1) * 2));
}

#[test]
fn contiguous_sequences_are_not_pruned() {
    let mut progress = StreamProgress::default();
    let observed = MAX_SEEN_SEQUENCE_RANGES as u64 * 3;

    for sequence in 0..observed {
        assert!(progress.observe_sequence(sequence));
    }

    assert_eq!(
        progress.seen_sequences,
        vec![SequenceRange::new(0, observed - 1)]
    );
    assert!(progress.is_duplicate(0));
}

#[test]
fn restored_state_is_pruned_to_the_deduplication_bound() {
    let mut state = AggregatorState::new();
    let stream = StreamProgress::stream_id(CexId::Binance, "btcusdt@trade");
    let ranges = (0..MAX_SEEN_SEQUENCE_RANGES as u64 * 2)
        .map(|sequence| SequenceRange::new(sequence * 2, sequence * 2))
        .collect();
    state.streams.insert(
        stream.clone(),
        StreamProgress {
            seen_sequences: ranges,
            ..StreamProgress::default()
        },
    );

    state.prune_stream_dedup();

    assert_eq!(
        state.streams[&stream].seen_sequences.len(),
        MAX_SEEN_SEQUENCE_RANGES
    );
}

#[test]
fn gaps_merge_overlapping_and_adjacent_ranges() {
    let mut state = AggregatorState::new();
    let mut first = GapMarker {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        previous_sequence: 1,
        observed_sequence: 4,
        missing_from: 2,
        missing_to: 3,
        detected_at: TimestampMilliseconds(10),
    };
    state.record_gap(&first);

    first.missing_from = 4;
    first.missing_to = 5;
    first.detected_at = TimestampMilliseconds(20);
    state.record_gap(&first);

    assert_eq!(state.known_gaps.len(), 1);
    assert_eq!(state.known_gaps[0].from_sequence, 2);
    assert_eq!(state.known_gaps[0].to_sequence, 5);
    assert_eq!(state.known_gaps[0].detected_at, TimestampMilliseconds(20));
}

#[test]
fn pruning_drops_bookkeeping_for_closed_windows_only() {
    let mut state = AggregatorState::new();
    let candle = candle();
    state.record_incomplete_window(IncompleteWindow {
        key: candle.key.clone(),
        window_start: candle.start_time(),
        window_end: candle.end_time(),
        stream: "btcusdt@trade".to_string(),
        from_sequence: 2,
        to_sequence: 2,
        detected_at: TimestampMilliseconds(61_000),
    });
    state.record_gap(&GapMarker {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        previous_sequence: 2,
        observed_sequence: 4,
        missing_from: 3,
        missing_to: 3,
        detected_at: TimestampMilliseconds(61_000),
    });

    state.prune_closed_windows(TimestampMilliseconds(120_000));

    assert!(state.incomplete_windows.is_empty());
    assert!(state.known_gaps.is_empty());
}

#[test]
fn pruning_keeps_bookkeeping_for_windows_at_or_after_the_cutoff() {
    let mut state = AggregatorState::new();
    let candle = candle();
    state.record_incomplete_window(IncompleteWindow {
        key: candle.key.clone(),
        window_start: candle.start_time(),
        window_end: candle.end_time(),
        stream: "btcusdt@trade".to_string(),
        from_sequence: 2,
        to_sequence: 2,
        detected_at: TimestampMilliseconds(61_000),
    });
    state.record_gap(&GapMarker {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        previous_sequence: 2,
        observed_sequence: 4,
        missing_from: 3,
        missing_to: 3,
        detected_at: TimestampMilliseconds(61_000),
    });

    state.prune_closed_windows(TimestampMilliseconds(60_000));

    assert_eq!(state.incomplete_windows.len(), 1);
    assert_eq!(state.known_gaps.len(), 1);
}

#[test]
fn state_round_trips_through_json_with_candle_keys() {
    let mut state = AggregatorState::new();
    let candle = candle();
    state
        .open_candles
        .entry(candle.key.clone())
        .or_default()
        .push(candle.clone());
    let mut closed = candle.clone();
    closed.close();
    state.retain_closed_candle(closed, usize::MAX);
    state.retain_closed_candle(closed_candle_for(candle.key.interval, 120_000), usize::MAX);
    state.mark_processed(42);
    state.advance_watermark(TimestampMilliseconds(61_000));
    state.observe_sequence(&DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 9));

    let payload = serde_json::to_vec(&state).unwrap();
    let decoded: AggregatorState = serde_json::from_slice(&payload).unwrap();

    assert!(decoded.is_schema_compatible());
    assert_eq!(decoded.open_candles_len(), 1);
    assert_eq!(decoded.closed_candles_len(), 2);
    assert_eq!(
        decoded.closed_candles_len_by_interval()[&candle.key.interval],
        2
    );
    let decoded_candle = decoded
        .open_candles
        .values()
        .next()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(
        decoded_candle.key.subject_suffix(),
        candle.key.subject_suffix()
    );
    assert!(
        decoded
            .closed_candle(&candle.key, candle.start_time())
            .unwrap()
            .market_candle
            .is_candle_closed
    );
    assert_eq!(decoded.last_processed_sequence, Some(42));
    assert_eq!(decoded.watermark, Some(TimestampMilliseconds(61_000)));
    assert!(decoded.is_duplicate(&DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 9,)));
    assert_eq!(decoded, state);
}

#[test]
fn incompatible_schema_is_rejected() {
    let mut state = AggregatorState::new();
    state.schema_version = STATE_SCHEMA_VERSION + 1;

    assert!(!state.is_schema_compatible());
    assert!(state.ensure_schema_compatible().is_err());
}

#[test]
fn schema_compatibility_can_be_checked_against_a_configured_version() {
    let state = AggregatorState::new();

    assert!(state.is_schema_compatible_with(STATE_SCHEMA_VERSION));
    assert!(
        state
            .ensure_schema_compatible_with(STATE_SCHEMA_VERSION)
            .is_ok()
    );
    assert!(!state.is_schema_compatible_with(STATE_SCHEMA_VERSION + 1));
    assert!(
        state
            .ensure_schema_compatible_with(STATE_SCHEMA_VERSION + 1)
            .is_err()
    );
}

fn candle_for(interval: AlignedWindow, timestamp: i64) -> Candle {
    let mut trade = trade(timestamp);
    trade.timestamp = TimestampMilliseconds(timestamp);
    Candle::open(&trade, interval).unwrap()
}

fn closed_candle_for(interval: AlignedWindow, timestamp: i64) -> Candle {
    let mut candle = candle_for(interval, timestamp);
    candle.close();
    candle
}

#[test]
fn per_interval_cap_keeps_the_newest_candles() {
    let mut state = AggregatorState::new();
    let interval = parse_interval("1m").unwrap();
    let key = candle_for(interval, 60_000).key.clone();

    for timestamp in [60_000, 120_000, 180_000] {
        let evicted = state.retain_closed_candle(closed_candle_for(interval, timestamp), 2);
        assert!(evicted.len() <= 1);
    }

    assert_eq!(state.closed_candles_len(), 2);
    assert!(
        state
            .closed_candle(&key, TimestampMilliseconds(60_000))
            .is_none()
    );
    assert!(
        state
            .closed_candle(&key, TimestampMilliseconds(120_000))
            .is_some()
    );
    assert!(
        state
            .closed_candle(&key, TimestampMilliseconds(180_000))
            .is_some()
    );
}

#[test]
fn caps_are_independent_between_intervals() {
    let mut state = AggregatorState::new();
    let one_minute = parse_interval("1m").unwrap();
    let five_minutes = parse_interval("5m").unwrap();
    let one_minute_key = candle_for(one_minute, 60_000).key.clone();
    let five_minute_key = candle_for(five_minutes, 60_000).key.clone();

    state.retain_closed_candle(closed_candle_for(five_minutes, 60_000), 1);
    state.retain_closed_candle(closed_candle_for(one_minute, 60_000), 1);
    state.retain_closed_candle(closed_candle_for(one_minute, 120_000), 1);

    assert!(
        state
            .closed_candle(&one_minute_key, TimestampMilliseconds(60_000))
            .is_none()
    );
    assert!(
        state
            .closed_candle(&one_minute_key, TimestampMilliseconds(120_000))
            .is_some()
    );
    assert!(
        state
            .closed_candle(&five_minute_key, TimestampMilliseconds(0))
            .is_some()
    );
}

#[test]
fn replacing_a_retained_window_does_not_evict() {
    let mut state = AggregatorState::new();
    let interval = parse_interval("1m").unwrap();
    let key = candle_for(interval, 60_000).key.clone();

    state.retain_closed_candle(closed_candle_for(interval, 60_000), 1);
    let evicted = state.retain_closed_candle(closed_candle_for(interval, 60_000), 1);

    assert!(evicted.is_empty());
    assert_eq!(state.closed_candles_len(), 1);
    assert!(
        state
            .closed_candle(&key, TimestampMilliseconds(60_000))
            .is_some()
    );
}

#[test]
fn retaining_an_older_window_at_capacity_would_evict_the_candidate() {
    let mut state = AggregatorState::new();
    let interval = parse_interval("1m").unwrap();
    let key = candle_for(interval, 120_000).key.clone();

    state.retain_closed_candle(closed_candle_for(interval, 120_000), 1);

    assert!(state.would_evict_closed_candidate(&key, TimestampMilliseconds(60_000), 1,));
    assert!(!state.would_evict_closed_candidate(&key, TimestampMilliseconds(120_000), 1,));
    assert!(!state.would_evict_closed_candidate(&key, TimestampMilliseconds(180_000), 1,));
}

#[test]
fn multiple_keys_each_keep_their_own_cap() {
    let mut state = AggregatorState::new();
    let interval = parse_interval("1m").unwrap();
    let mut other_trade = trade(61_000);
    other_trade.trading_pair = TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD);
    let other_key = Candle::open(&other_trade, interval).unwrap().key.clone();
    let key = candle_for(interval, 60_000).key.clone();

    for timestamp in [60_000, 120_000] {
        state.retain_closed_candle(closed_candle_for(interval, timestamp), 1);
        let mut other = Candle::open(
            &{
                let mut trade = other_trade.clone();
                trade.timestamp = TimestampMilliseconds(timestamp);
                trade
            },
            interval,
        )
        .unwrap();
        other.close();
        state.retain_closed_candle(other, 1);
    }

    assert_eq!(state.closed_candles_len(), 2);
    assert!(
        state
            .closed_candle(&key, TimestampMilliseconds(120_000))
            .is_some()
    );
    assert!(
        state
            .closed_candle(&other_key, TimestampMilliseconds(120_000))
            .is_some()
    );
}
