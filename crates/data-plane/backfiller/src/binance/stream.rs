use platform_errors::{BpError, BpResult, ExchangeError};

/// A parsed Binance stream name such as `btcusdt@trade`.
#[derive(Debug, Clone)]
pub struct MarketStream {
    pub symbol: String,
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
        if !suffix.eq_ignore_ascii_case("trade") {
            // TODO: add REST reconstructors for depth, book-ticker, kline and
            // other sequenced streams. The exchange APIs return different
            // snapshot/update shapes for each stream.
            return Err(BpError::Exchange(ExchangeError::BadValue {
                name: "stream".to_string(),
                value: stream.to_string(),
                reason: "only trade gaps are supported".to_string(),
            }));
        }
        if symbol.is_empty() {
            return Err(BpError::Exchange(ExchangeError::BadValue {
                name: "stream".to_string(),
                value: stream.to_string(),
                reason: "symbol is empty".to_string(),
            }));
        }
        Ok(Self {
            symbol: symbol.to_ascii_uppercase(),
        })
    }
}
