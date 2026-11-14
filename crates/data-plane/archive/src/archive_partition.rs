use platform_market_data::CandleKey;
use stock_trek_types::cex::cex_id::CexId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArchivePartition {
    Events { exchange: CexId, stream: String },
    Candles { candle: CandleKey },
}

impl ArchivePartition {
    pub fn path(&self) -> String {
        match self {
            Self::Events { exchange, stream } => format!(
                "kind=events/exchange={}/stream={}",
                Self::segment(&exchange.to_string()),
                Self::segment(stream)
            ),
            Self::Candles { candle } => format!(
                "kind=candles/exchange={}/base={}/quote={}/interval={}",
                Self::segment(&candle.exchange.to_string()),
                Self::segment(&candle.trading_pair.base.to_string()),
                Self::segment(&candle.trading_pair.quote.to_string()),
                Self::segment(candle.interval.label())
            ),
        }
    }

    fn segment(value: &str) -> String {
        let value = value.trim().replace(['/', '\\'], "_");
        if value.is_empty() {
            "unknown".to_string()
        } else {
            value
        }
    }
}
