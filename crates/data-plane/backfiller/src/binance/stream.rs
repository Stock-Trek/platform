use platform_errors::{BpError, BpResult, ExchangeError};

/// The subset of exchange streams the first backfiller cut can reconstruct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Trade,
    AggTrade,
}

/// A parsed exchange stream name such as `btcusdt@aggTrade`.
#[derive(Debug, Clone)]
pub struct MarketStream {
    pub symbol: String,
    pub kind: StreamKind,
}

impl MarketStream {
    pub fn parse(stream: &str) -> BpResult<Self> {
        let Some((symbol, suffix)) = stream.split_once('@') else {
            return Err(BpError::Exchange(ExchangeError::BadValue {
                name: "stream".to_string(),
                value: stream.to_string(),
                reason: "expected <symbol>@<stream>".to_string(),
            }));
        };
        let kind = match suffix.to_ascii_lowercase().as_str() {
            "trade" => StreamKind::Trade,
            "aggtrade" => StreamKind::AggTrade,
            _ => {
                // TODO: add REST reconstructors for depth, book-ticker, kline and
                // other sequenced streams. The exchange APIs return different
                // snapshot/update shapes for each stream.
                return Err(BpError::Exchange(ExchangeError::BadValue {
                    name: "stream".to_string(),
                    value: stream.to_string(),
                    reason: "only trade and aggTrade gaps are supported".to_string(),
                }));
            }
        };
        if symbol.is_empty() {
            return Err(BpError::Exchange(ExchangeError::BadValue {
                name: "stream".to_string(),
                value: stream.to_string(),
                reason: "symbol is empty".to_string(),
            }));
        }
        Ok(Self {
            symbol: symbol.to_ascii_uppercase(),
            kind,
        })
    }
}
