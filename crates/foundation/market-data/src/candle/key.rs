use super::interval::AlignedWindow;
use serde::{Deserialize, Serialize};
use std::fmt;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

/// Identity of an open candle. One candle is maintained per exchange, trading
/// pair, and interval.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CandleKey {
    pub exchange: CexId,
    pub trading_pair: TradingPair,
    #[serde(with = "super::interval::serde_label")]
    pub interval: AlignedWindow,
}

impl CandleKey {
    pub fn new(exchange: CexId, trading_pair: TradingPair, interval: AlignedWindow) -> Self {
        Self {
            exchange,
            trading_pair,
            interval,
        }
    }

    /// The subject/key segment used when publishing this candle.
    pub fn subject_suffix(&self) -> String {
        format!(
            "{}.{}.{}.{}",
            self.exchange,
            self.trading_pair.base,
            self.trading_pair.quote,
            self.interval.label()
        )
    }
}

impl fmt::Display for CandleKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.subject_suffix())
    }
}
