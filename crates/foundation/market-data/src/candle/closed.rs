use super::body::Candle;
use super::revision::CandleRevision;
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};

/// A candle that has left its aggregation window and is ready to be archived.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedCandle {
    /// Deterministic revision assigned when the candle closed.
    revision: CandleRevision,
    candle: Candle,
}

impl ClosedCandle {
    /// Builds a closed candle and derives its deterministic revision.
    pub fn new(
        candle: Candle,
        close_watermark: TimestampMilliseconds,
        source_offset: u64,
        state_epoch: u64,
    ) -> Self {
        let revision =
            CandleRevision::for_candle(&candle, close_watermark, source_offset, state_epoch);
        Self { revision, candle }
    }

    /// The deterministic revision assigned when the candle closed.
    ///
    /// The revision is derived from the candle at construction time and cannot
    /// be changed independently of the candle.
    pub fn revision(&self) -> &CandleRevision {
        &self.revision
    }

    /// The closed candle body.
    pub fn candle(&self) -> &Candle {
        &self.candle
    }

    /// Stable id used when publishing this candle.
    pub fn message_id(&self) -> String {
        self.revision.message_id()
    }

    /// Whether every source event that belongs to this candle was observed.
    pub fn is_complete(&self) -> bool {
        self.candle.is_complete
    }

    /// Whether a source gap affected this candle.
    pub fn is_incomplete(&self) -> bool {
        !self.is_complete()
    }
}

impl std::ops::Deref for ClosedCandle {
    type Target = Candle;

    fn deref(&self) -> &Self::Target {
        &self.candle
    }
}
