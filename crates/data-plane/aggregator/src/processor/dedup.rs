//! Event deduplication.
//!
//! Market events are deduplicated by `(exchange, stream, sequence)`. Replayed
//! or redelivered events must not be folded into candles twice, and duplicates
//! are recorded as a dropped-event metric.

use super::{CandleProcessor, ProcessorUpdate};
use crate::state::DeduplicationKey;
use platform_market_data::event::MarketEvent;
use platform_metrics::descriptions::{DROP_REASON_DUPLICATE, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;

impl CandleProcessor {
    /// Records the event's sequence and returns whether it should be applied.
    pub(super) fn accept_event(
        &mut self,
        event: &MarketEvent,
        update: &mut ProcessorUpdate,
    ) -> bool {
        let Some(sequence) = event.sequence else {
            return true;
        };
        let key = DeduplicationKey::new(event.exchange, &event.stream, sequence);
        if self.state.observe_sequence(&key) {
            return true;
        }

        update.duplicates += 1;
        MarketDataMetrics::record_dropped_event(STAGE_AGGREGATOR, DROP_REASON_DUPLICATE);
        false
    }
}
