//! Outputs produced by applying one input to the keyed state.

use crate::state::IncompleteWindow;
use platform_market_data::candle::{Candle, ClosedCandle};
use std::collections::HashSet;

/// Outputs produced by applying one input to the keyed state.
#[derive(Debug, Default, Clone)]
pub struct ProcessorUpdate {
    pub open: Vec<Candle>,
    pub closed: Vec<ClosedCandle>,
    pub incomplete_windows: Vec<IncompleteWindow>,
    /// Events dropped because their `(exchange, stream, sequence)` was seen.
    pub duplicates: usize,
    /// Trades that arrived after their candle's allowed-lateness window closed
    /// and were applied as closed-candle corrections.
    pub late_trades: usize,
}

impl ProcessorUpdate {
    pub fn is_empty(&self) -> bool {
        self.open.is_empty()
            && self.closed.is_empty()
            && self.incomplete_windows.is_empty()
            && self.duplicates == 0
            && self.late_trades == 0
    }

    /// Keeps only the last open-candle value for each [`platform_market_data::candle::CandleKey`].
    ///
    /// Open candles are a derived projection keyed only by candle identity, so
    /// intermediate values produced while folding multiple trades are not
    /// observable. Coalescing them here avoids cloning and writing each
    /// intermediate value to the shared KV bucket.
    pub fn coalesce_open(&mut self) {
        if self.open.len() < 2 {
            return;
        }
        let mut seen = HashSet::with_capacity(self.open.len());
        self.open.reverse();
        self.open.retain(|candle| seen.insert(candle.key.clone()));
        self.open.reverse();
    }
}
