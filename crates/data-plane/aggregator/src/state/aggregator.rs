//! Durable keyed aggregation state.
//!
//! The open candles, event-time watermarks, deduplication ranges, and gap
//! markers are the recovery state of a partition. Applying an input is a pure
//! update of this state, so replaying the same event log produces the same
//! candles and the same incompleteness records.

use super::candles::candle_maps_eq;
use super::gaps::{IncompleteWindow, KnownGap};
use super::streams::StreamProgress;
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::{Candle, CandleKey};
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Schema version of the serialized keyed state.
///
/// Bump this whenever the persisted shape changes in a way that cannot be read
/// as the current state. Checkpoints with another version are ignored and the
/// partition is rebuilt from archived raw events.
pub const STATE_SCHEMA_VERSION: u32 = 4;

/// The keyed state of one aggregator partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatorState {
    /// Version of this serialized state.
    pub schema_version: u32,
    /// Epoch of this state lineage.
    ///
    /// Every checkpoint restores the same epoch. A state rebuilt from archived
    /// inputs must advance it so deterministic output ids from the previous
    /// lineage cannot suppress the rebuilt outputs.
    pub state_epoch: u64,
    /// Open candles keyed by exchange, trading pair, and interval.
    ///
    /// A key can hold more than one window while an allowed-lateness window is
    /// still open, so late trades can update the correct candle instead of
    /// reopening a closed one.
    #[serde(with = "super::candles::open_candles")]
    pub open_candles: HashMap<CandleKey, Vec<Candle>>,
    /// Closed candles retained so late or backfilled trades can revise them.
    #[serde(default, with = "super::candles::open_candles")]
    pub closed_candles: HashMap<CandleKey, Vec<Candle>>,
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
        Self::new_with_schema_version(STATE_SCHEMA_VERSION)
    }

    /// Creates empty state stamped with the schema version the caller expects.
    pub fn new_with_schema_version(schema_version: u32) -> Self {
        Self {
            schema_version,
            state_epoch: 0,
            open_candles: HashMap::new(),
            closed_candles: HashMap::new(),
            last_processed_sequence: None,
            watermark: None,
            streams: BTreeMap::new(),
            known_gaps: Vec::new(),
            incomplete_windows: Vec::new(),
        }
    }

    pub fn is_schema_compatible(&self) -> bool {
        self.is_schema_compatible_with(STATE_SCHEMA_VERSION)
    }

    /// Whether this state carries the schema version the process expects.
    pub fn is_schema_compatible_with(&self, expected_version: u32) -> bool {
        self.schema_version == expected_version
    }

    pub fn ensure_schema_compatible(&self) -> BpResult<()> {
        self.ensure_schema_compatible_with(STATE_SCHEMA_VERSION)
    }

    pub fn ensure_schema_compatible_with(&self, expected_version: u32) -> BpResult<()> {
        if self.is_schema_compatible_with(expected_version) {
            return Ok(());
        }
        Err(BpError::bad_value(
            "aggregator state",
            self.schema_version.to_string(),
            format!("expected schema version {expected_version}"),
        ))
    }

    /// Last processed source sequence/offset, if any.
    pub fn last_processed_offset(&self) -> Option<u64> {
        self.last_processed_sequence
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

    /// Records the last processed source sequence/offset.
    pub fn mark_processed(&mut self, sequence: u64) {
        self.last_processed_sequence = Some(match self.last_processed_sequence {
            Some(current) => current.max(sequence),
            None => sequence,
        });
    }
}

impl PartialEq for AggregatorState {
    fn eq(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.state_epoch == other.state_epoch
            && self.last_processed_sequence == other.last_processed_sequence
            && self.watermark == other.watermark
            && self.streams == other.streams
            && self.known_gaps == other.known_gaps
            && self.incomplete_windows == other.incomplete_windows
            && candle_maps_eq(&self.open_candles, &other.open_candles)
            && candle_maps_eq(&self.closed_candles, &other.closed_candles)
    }
}
