use crate::gap_retry_store::PendingGap;
use platform_metrics::MarketDataMetrics;
use platform_metrics::{
    BACKFILL_OUTCOME_COVERED, BACKFILL_OUTCOME_EMPTY, BACKFILL_OUTCOME_ERROR,
    BACKFILL_OUTCOME_PARTIAL,
};
use tracing::{info, warn};

#[derive(Clone, Copy)]
pub enum GapOutcome<'a> {
    Error {
        error: &'a str,
        message: &'static str,
    },
    Empty,
    Partial {
        expected: u64,
        published: u64,
    },
    Covered {
        published: u64,
    },
}

impl GapOutcome<'_> {
    pub fn record(self, pending: &PendingGap) {
        MarketDataMetrics::record_backfill_gap_outcome(self.metric());
        match self {
            Self::Error { error, message } => warn!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                %error,
                "{message}"
            ),
            Self::Empty => warn!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                "exchange returned no events for gap"
            ),
            Self::Partial {
                expected,
                published,
            } => warn!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                expected,
                published,
                "backfill did not cover the whole gap"
            ),
            Self::Covered { published } => info!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                published,
                "backfilled gap"
            ),
        }
    }

    fn metric(self) -> &'static str {
        match self {
            Self::Error { .. } => BACKFILL_OUTCOME_ERROR,
            Self::Empty => BACKFILL_OUTCOME_EMPTY,
            Self::Partial { .. } => BACKFILL_OUTCOME_PARTIAL,
            Self::Covered { .. } => BACKFILL_OUTCOME_COVERED,
        }
    }
}
