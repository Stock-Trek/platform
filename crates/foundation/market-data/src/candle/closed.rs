use super::body::Candle;
use super::revision::CandleRevision;
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};

/// A candle that has left its aggregation window and is ready to be archived.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedCandle {
    /// Deterministic revision assigned when the candle closed.
    pub revision: CandleRevision,
    pub candle: Candle,
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

    /// Marks this candle as affected by a source gap.
    pub fn mark_incomplete(&mut self) {
        self.candle.mark_incomplete();
    }
}

impl std::ops::Deref for ClosedCandle {
    type Target = Candle;

    fn deref(&self) -> &Self::Target {
        &self.candle
    }
}

impl std::ops::DerefMut for ClosedCandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.candle
    }
}
