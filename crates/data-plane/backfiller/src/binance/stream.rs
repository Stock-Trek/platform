use platform_errors::{BpError, BpResult};
use platform_exchange_types::binance::klines::BinanceKlinesInterval;

/// The market data stream behind a Binance stream name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketStreamKind {
    Trade,
    Depth,
    BookTicker,
    Kline(BinanceKlinesInterval),
}

/// A parsed Binance stream name such as `btcusdt@trade`.
#[derive(Debug, Clone)]
pub struct MarketStream {
    pub symbol: String,
    pub kind: MarketStreamKind,
}

impl MarketStream {
    pub fn parse(stream: &str) -> BpResult<Self> {
        let Some((symbol, suffix)) = stream.split_once('@') else {
            return Err(BpError::bad_value(
                "stream",
                stream,
                "expected <symbol>@<stream>",
            ));
        };
        if symbol.is_empty() {
            return Err(BpError::bad_value("stream", stream, "symbol is empty"));
        }
        let kind = Self::kind(stream, suffix)?;
        Ok(Self {
            symbol: symbol.to_ascii_uppercase(),
            kind,
        })
    }

    fn kind(stream: &str, suffix: &str) -> BpResult<MarketStreamKind> {
        let lower = suffix.to_ascii_lowercase();
        if lower == "trade" {
            return Ok(MarketStreamKind::Trade);
        }
        if lower == "bookticker" {
            return Ok(MarketStreamKind::BookTicker);
        }
        if Self::is_depth_suffix(&lower) {
            return Ok(MarketStreamKind::Depth);
        }
        if let Some(interval) = lower.strip_prefix("kline_") {
            return BinanceKlinesInterval::try_from(interval)
                .map(MarketStreamKind::Kline)
                .map_err(|_| {
                    BpError::bad_value(
                        "stream",
                        stream,
                        format!("unsupported kline interval {interval}"),
                    )
                });
        }
        Err(BpError::bad_value(
            "stream",
            stream,
            "unsupported stream; expected trade, depth, bookTicker, or kline_<interval>",
        ))
    }

    fn is_depth_suffix(suffix: &str) -> bool {
        if suffix == "depth" || suffix.starts_with("depth@") {
            return true;
        }
        let Some(levels) = suffix.strip_prefix("depth") else {
            return false;
        };
        let digits_len = levels
            .chars()
            .take_while(|character| character.is_ascii_digit())
            .count();
        if digits_len == 0 {
            return false;
        }
        let rest = &levels[digits_len..];
        rest.is_empty() || rest.starts_with('@')
    }
}

#[cfg(test)]
mod tests;
