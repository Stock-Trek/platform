use super::body::Candle;
use super::key::CandleKey;
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};

/// Deterministic revision of a closed candle.
///
/// A revision is a pure function of the candle identity and window, the source
/// watermark that closed the candle, the source offset that advanced that
/// watermark, and the shard state epoch. Replaying the same input log
/// therefore recreates the same revision; there is no process-local or global
/// revision counter to race across replicas.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CandleRevision {
    /// Candle identity this revision belongs to.
    pub key: CandleKey,
    /// Inclusive window start.
    pub window_start: TimestampMilliseconds,
    /// Exclusive window end.
    pub window_end: TimestampMilliseconds,
    /// Event-time watermark at which the candle was closed.
    pub close_watermark: TimestampMilliseconds,
    /// Source offset that advanced the watermark to `close_watermark`.
    pub source_offset: u64,
    /// State epoch that produced this revision.
    pub state_epoch: u64,
}

impl PartialOrd for CandleRevision {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CandleRevision {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .subject_suffix()
            .cmp(&other.key.subject_suffix())
            .then_with(|| self.window_start.cmp(&other.window_start))
            .then_with(|| self.window_end.cmp(&other.window_end))
            .then_with(|| self.close_watermark.cmp(&other.close_watermark))
            .then_with(|| self.source_offset.cmp(&other.source_offset))
            .then_with(|| self.state_epoch.cmp(&other.state_epoch))
    }
}

impl CandleRevision {
    /// Derives the revision for `candle` from the closing watermark and source
    /// offset that are part of the shard state.
    pub fn for_candle(
        candle: &Candle,
        close_watermark: TimestampMilliseconds,
        source_offset: u64,
        state_epoch: u64,
    ) -> Self {
        Self {
            key: candle.key.clone(),
            window_start: candle.start_time(),
            window_end: candle.end_time(),
            close_watermark,
            source_offset,
            state_epoch,
        }
    }

    /// Stable id for the published closed-candle message.
    ///
    /// The id covers the candle key, its window, and the state epoch. Revision
    /// provenance is included as well, so a replay of the same processing step
    /// publishes the same id while a genuinely new revision is not hidden by
    /// JetStream de-duplication.
    pub fn message_id(&self) -> String {
        format!(
            "{}@{}:{}.epoch={}.watermark={}.offset={}",
            self.key.subject_suffix(),
            self.window_start.0,
            self.window_end.0,
            self.state_epoch,
            self.close_watermark.0,
            self.source_offset,
        )
    }
}
