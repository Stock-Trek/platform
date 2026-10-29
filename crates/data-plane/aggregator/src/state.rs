//! Durable keyed aggregation state.
//!
//! The open candles, event-time watermarks, deduplication ranges, and gap
//! markers are the recovery state of a partition. Applying an input is a pure
//! update of this state, so replaying the same event log produces the same
//! candles and the same incompleteness records.

use platform_errors::{BpError, BpResult};
use platform_market_data::{
    candle::{Candle, CandleKey},
    event::GapMarker,
};
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap};
use stock_trek_types::cex::cex_id::CexId;

/// Schema version of the serialized keyed state.
///
/// Bump this whenever the persisted shape changes in a way that cannot be read
/// as the current state. Checkpoints with another version are ignored and the
/// partition is rebuilt from archived raw events.
pub const STATE_SCHEMA_VERSION: u32 = 2;

/// Alias used by callers that describe the constant by its shorter name.
pub const SCHEMA_VERSION: u32 = STATE_SCHEMA_VERSION;

/// An inclusive range of source sequences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SequenceRange {
    pub from_sequence: u64,
    pub to_sequence: u64,
}

impl SequenceRange {
    pub fn new(from_sequence: u64, to_sequence: u64) -> Self {
        Self {
            from_sequence,
            to_sequence,
        }
    }

    pub fn contains(&self, sequence: u64) -> bool {
        self.from_sequence <= sequence && sequence <= self.to_sequence
    }

    pub fn overlaps_or_adjacent(&self, other: &Self) -> bool {
        self.from_sequence <= other.to_sequence.saturating_add(1)
            && other.from_sequence <= self.to_sequence.saturating_add(1)
    }
}

/// Alias for a known gap in the source stream.
pub type IncompleteRange = SequenceRange;

/// Identity of an element in an exchange stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeduplicationKey {
    pub exchange: CexId,
    pub stream: String,
    pub sequence: u64,
}

impl DeduplicationKey {
    pub fn new(exchange: CexId, stream: impl Into<String>, sequence: u64) -> Self {
        Self {
            exchange,
            stream: stream.into(),
            sequence,
        }
    }

    pub fn stream_id(&self) -> String {
        stream_id(self.exchange, &self.stream)
    }
}

/// Stable key for the progress of one exchange stream.
pub fn stream_id(exchange: CexId, stream: &str) -> String {
    format!("{exchange}.{stream}")
}

/// Per-stream event-time progress and deduplication ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamProgress {
    pub watermark: Option<TimestampMilliseconds>,
    pub last_sequence: Option<u64>,
    /// Inclusive sequence ranges already applied for this stream.
    pub seen_sequences: Vec<SequenceRange>,
}

impl StreamProgress {
    pub fn is_duplicate(&self, sequence: u64) -> bool {
        self.seen_sequences
            .iter()
            .any(|range| range.contains(sequence))
    }

    /// Records a sequence and returns `false` when it had already been seen.
    pub fn observe_sequence(&mut self, sequence: u64) -> bool {
        if self.is_duplicate(sequence) {
            return false;
        }
        self.insert_sequence(sequence);
        self.last_sequence = Some(match self.last_sequence {
            Some(current) => current.max(sequence),
            None => sequence,
        });
        true
    }

    pub fn advance_watermark(&mut self, timestamp: TimestampMilliseconds) -> bool {
        match self.watermark {
            Some(current) if current >= timestamp => false,
            _ => {
                self.watermark = Some(timestamp);
                true
            }
        }
    }

    fn insert_sequence(&mut self, sequence: u64) {
        let mut merged = SequenceRange::new(sequence, sequence);
        self.seen_sequences.retain(|existing| {
            if existing.overlaps_or_adjacent(&merged) {
                merged.from_sequence = merged.from_sequence.min(existing.from_sequence);
                merged.to_sequence = merged.to_sequence.max(existing.to_sequence);
                false
            } else {
                true
            }
        });
        self.seen_sequences.push(merged);
        self.seen_sequences.sort();
    }
}

/// A source range that is known to be incomplete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnownGap {
    pub exchange: CexId,
    pub stream: String,
    pub from_sequence: u64,
    pub to_sequence: u64,
    pub detected_at: TimestampMilliseconds,
}

impl KnownGap {
    pub fn new(gap: &GapMarker) -> Self {
        Self {
            exchange: gap.exchange,
            stream: gap.stream.clone(),
            from_sequence: gap.missing_from,
            to_sequence: gap.missing_to,
            detected_at: gap.detected_at,
        }
    }

    pub fn range(&self) -> SequenceRange {
        SequenceRange::new(self.from_sequence, self.to_sequence)
    }

    fn merge(&mut self, other: &Self) {
        self.from_sequence = self.from_sequence.min(other.from_sequence);
        self.to_sequence = self.to_sequence.max(other.to_sequence);
        self.detected_at = self.detected_at.max(other.detected_at);
    }
}

/// A candle window that may be missing trades because of a source gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncompleteWindow {
    pub key: CandleKey,
    pub window_start: TimestampMilliseconds,
    pub window_end: TimestampMilliseconds,
    pub stream: String,
    pub from_sequence: u64,
    pub to_sequence: u64,
    pub detected_at: TimestampMilliseconds,
}

impl IncompleteWindow {
    fn same_window(&self, other: &Self) -> bool {
        self.key == other.key && self.window_start == other.window_start
    }

    fn merge(&mut self, other: &Self) {
        self.from_sequence = self.from_sequence.min(other.from_sequence);
        self.to_sequence = self.to_sequence.max(other.to_sequence);
        self.window_end = self.window_end.max(other.window_end);
        self.detected_at = self.detected_at.max(other.detected_at);
    }
}

/// The keyed state of one aggregator partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatorState {
    /// Version of this serialized state.
    pub schema_version: u32,
    /// Open candles keyed by exchange, trading pair, and interval.
    ///
    /// A key can hold more than one window while an allowed-lateness window is
    /// still open, so late trades can update the correct candle instead of
    /// reopening a closed one.
    #[serde(with = "open_candles")]
    pub open_candles: HashMap<CandleKey, Vec<Candle>>,
    /// Last routed source sequence whose effects are included in this state.
    pub last_processed_sequence: Option<u64>,
    /// Global event-time watermark observed by this partition.
    pub watermark: Option<TimestampMilliseconds>,
    /// Per-stream watermarks and deduplication ranges.
    pub streams: BTreeMap<String, StreamProgress>,
    /// Source ranges that are known to be incomplete.
    pub known_gaps: Vec<KnownGap>,
    /// Candle windows affected by a known gap.
    pub incomplete_windows: Vec<IncompleteWindow>,
}

impl Default for AggregatorState {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregatorState {
    pub fn new() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            open_candles: HashMap::new(),
            last_processed_sequence: None,
            watermark: None,
            streams: BTreeMap::new(),
            known_gaps: Vec::new(),
            incomplete_windows: Vec::new(),
        }
    }

    pub fn is_schema_compatible(&self) -> bool {
        self.schema_version == STATE_SCHEMA_VERSION
    }

    pub fn ensure_schema_compatible(&self) -> BpResult<()> {
        if self.is_schema_compatible() {
            return Ok(());
        }
        Err(BpError::bad_value(
            "aggregator state",
            self.schema_version.to_string(),
            format!("expected schema version {STATE_SCHEMA_VERSION}"),
        ))
    }

    /// Last processed source sequence/offset, if any.
    pub fn last_processed_offset(&self) -> Option<u64> {
        self.last_processed_sequence
    }

    /// Returns the newest open candle for a key.
    pub fn open_candle(&self, key: &CandleKey) -> Option<&Candle> {
        self.open_candles
            .get(key)
            .and_then(|candles| candles.last())
    }

    pub fn open_candles_len(&self) -> usize {
        self.open_candles.values().map(Vec::len).sum()
    }

    /// Advances the global event-time watermark. Returns whether it moved.
    pub fn advance_watermark(&mut self, timestamp: TimestampMilliseconds) -> bool {
        match self.watermark {
            Some(current) if current >= timestamp => false,
            _ => {
                self.watermark = Some(timestamp);
                true
            }
        }
    }

    pub fn stream_watermark(&self, exchange: CexId, stream: &str) -> Option<TimestampMilliseconds> {
        self.streams
            .get(&stream_id(exchange, stream))
            .and_then(|progress| progress.watermark)
    }

    pub fn advance_stream_watermark(
        &mut self,
        exchange: CexId,
        stream: &str,
        timestamp: TimestampMilliseconds,
    ) -> bool {
        self.streams
            .entry(stream_id(exchange, stream))
            .or_default()
            .advance_watermark(timestamp)
    }

    pub fn is_duplicate(&self, key: &DeduplicationKey) -> bool {
        self.streams
            .get(&key.stream_id())
            .is_some_and(|progress| progress.is_duplicate(key.sequence))
    }

    /// Marks a key as seen. Returns `false` when it was already seen.
    pub fn observe_sequence(&mut self, key: &DeduplicationKey) -> bool {
        self.streams
            .entry(key.stream_id())
            .or_default()
            .observe_sequence(key.sequence)
    }

    /// Records the last processed source sequence/offset.
    pub fn mark_processed(&mut self, sequence: u64) {
        self.last_processed_sequence = Some(match self.last_processed_sequence {
            Some(current) => current.max(sequence),
            None => sequence,
        });
    }

    /// Records a gap, merging overlapping or adjacent ranges for the same
    /// exchange stream.
    pub fn record_gap(&mut self, gap: &GapMarker) {
        let mut merged = KnownGap::new(gap);
        self.known_gaps.retain(|existing| {
            if existing.exchange == merged.exchange
                && existing.stream == merged.stream
                && existing.range().overlaps_or_adjacent(&merged.range())
            {
                merged.merge(existing);
                false
            } else {
                true
            }
        });
        self.known_gaps.push(merged);
        self.known_gaps.sort_by(|left, right| {
            left.exchange
                .to_string()
                .cmp(&right.exchange.to_string())
                .then_with(|| left.stream.cmp(&right.stream))
                .then_with(|| left.from_sequence.cmp(&right.from_sequence))
        });
    }

    /// Records an incomplete range directly.
    pub fn record_incomplete_range(
        &mut self,
        exchange: CexId,
        stream: impl Into<String>,
        range: SequenceRange,
        detected_at: TimestampMilliseconds,
    ) {
        let gap = GapMarker {
            exchange,
            stream: stream.into(),
            previous_sequence: range.from_sequence.saturating_sub(1),
            observed_sequence: range.to_sequence.saturating_add(1),
            missing_from: range.from_sequence,
            missing_to: range.to_sequence,
            detected_at,
        };
        self.record_gap(&gap);
    }

    pub fn record_incomplete_window(&mut self, window: IncompleteWindow) {
        if let Some(existing) = self
            .incomplete_windows
            .iter_mut()
            .find(|existing| existing.same_window(&window))
        {
            existing.merge(&window);
        } else {
            self.incomplete_windows.push(window);
        }
        self.incomplete_windows.sort_by(|left, right| {
            left.key
                .subject_suffix()
                .cmp(&right.key.subject_suffix())
                .then_with(|| left.window_start.cmp(&right.window_start))
                .then_with(|| left.from_sequence.cmp(&right.from_sequence))
        });
    }

    pub fn has_gap(&self, exchange: CexId, stream: &str, sequence: u64) -> bool {
        self.known_gaps.iter().any(|gap| {
            gap.exchange == exchange && gap.stream == stream && gap.range().contains(sequence)
        })
    }

    pub fn is_window_incomplete(
        &self,
        key: &CandleKey,
        window_start: TimestampMilliseconds,
    ) -> bool {
        self.incomplete_windows
            .iter()
            .any(|window| window.key == *key && window.window_start == window_start)
    }
}

impl PartialEq for AggregatorState {
    fn eq(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.last_processed_sequence == other.last_processed_sequence
            && self.watermark == other.watermark
            && self.streams == other.streams
            && self.known_gaps == other.known_gaps
            && self.incomplete_windows == other.incomplete_windows
            && self.open_candles.len() == other.open_candles.len()
            && self.open_candles.iter().all(|(key, candles)| {
                other.open_candles.get(key).is_some_and(|other_candles| {
                    candles.len() == other_candles.len()
                        && candles
                            .iter()
                            .zip(other_candles)
                            .all(|(candle, other_candle)| candle_eq(candle, other_candle))
                })
            })
    }
}

fn candle_eq(left: &Candle, right: &Candle) -> bool {
    left.key == right.key
        && left.market_candle.start_time_millis_inc == right.market_candle.start_time_millis_inc
        && left.market_candle.end_time_millis_exc == right.market_candle.end_time_millis_exc
        && left.market_candle.duration_millis == right.market_candle.duration_millis
        && left.market_candle.is_candle_closed == right.market_candle.is_candle_closed
        && left.market_candle.trade_count == right.market_candle.trade_count
        && left.market_candle.ohlcv.open == right.market_candle.ohlcv.open
        && left.market_candle.ohlcv.high == right.market_candle.ohlcv.high
        && left.market_candle.ohlcv.low == right.market_candle.ohlcv.low
        && left.market_candle.ohlcv.close == right.market_candle.ohlcv.close
        && left.market_candle.ohlcv.volume == right.market_candle.ohlcv.volume
        && left.market_candle.ohlcv.quote_volume == right.market_candle.ohlcv.quote_volume
        && left.market_candle.ohlcv.vwap == right.market_candle.ohlcv.vwap
}

/// Serializes grouped open candles as one deterministically sorted list.
mod open_candles {
    use super::*;

    pub fn serialize<S>(
        candles: &HashMap<CandleKey, Vec<Candle>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut values = candles
            .values()
            .flat_map(|candles| candles.iter())
            .collect::<Vec<_>>();
        values.sort_by_key(|candle| (candle.key.subject_suffix(), candle.start_time().0));
        values.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<CandleKey, Vec<Candle>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let candles = Vec::<Candle>::deserialize(deserializer)?;
        let mut grouped: HashMap<CandleKey, Vec<Candle>> = HashMap::new();
        for candle in candles {
            grouped.entry(candle.key.clone()).or_default().push(candle);
        }
        for candles in grouped.values_mut() {
            candles.sort_by_key(Candle::start_time);
        }
        Ok(grouped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_market_data::candle::CandleInterval;
    use platform_market_data::decoders::trade::Trade;
    use rust_decimal::Decimal;
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
        Candle::open(&trade(61_000), CandleInterval::from_label("1m").unwrap())
    }

    #[test]
    fn new_state_has_the_current_schema_and_is_empty() {
        let state = AggregatorState::new();

        assert_eq!(state.schema_version, STATE_SCHEMA_VERSION);
        assert!(state.is_schema_compatible());
        assert!(state.open_candles.is_empty());
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
        assert!(state.has_gap(CexId::Binance, "btcusdt@trade", 3));
        assert!(!state.has_gap(CexId::Binance, "btcusdt@trade", 6));
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
        state.mark_processed(42);
        state.advance_watermark(TimestampMilliseconds(61_000));
        state.observe_sequence(&DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 9));

        let payload = serde_json::to_vec(&state).unwrap();
        let decoded: AggregatorState = serde_json::from_slice(&payload).unwrap();

        assert!(decoded.is_schema_compatible());
        assert_eq!(decoded.open_candles_len(), 1);
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
}
