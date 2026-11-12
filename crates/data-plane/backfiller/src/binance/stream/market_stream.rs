use platform_errors::{BuildError, BuildResult};
use platform_exchange_types::binance::klines::BinanceKlinesInterval;

/// The market data stream behind a Binance stream name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketStreamKind {
    Trade,
    Depth,
    BookTicker,
    Kline(BinanceKlinesInterval),
}

impl MarketStreamKind {
    fn parse(stream: &str, suffix: &str) -> BuildResult<Self> {
        let lower = suffix.to_ascii_lowercase();
        match lower.as_str() {
            "trade" => Ok(Self::Trade),
            "bookticker" => Ok(Self::BookTicker),
            _ if Self::is_depth_suffix(&lower) => Ok(Self::Depth),
            _ => Self::parse_kline(stream, &lower),
        }
    }

    fn parse_kline(stream: &str, suffix: &str) -> BuildResult<Self> {
        let Some(interval) = suffix.strip_prefix("kline_") else {
            return Err(BuildError::bad_value(
                "stream",
                stream,
                "unsupported stream; expected trade, depth, bookTicker, or kline_<interval>",
            ));
        };
        BinanceKlinesInterval::try_from(interval)
            .map(Self::Kline)
            .map_err(|_| {
                BuildError::bad_value(
                    "stream",
                    stream,
                    format!("unsupported kline interval {interval}"),
                )
            })
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

/// A parsed Binance stream name such as `btcusdt@trade`.
#[derive(Debug, Clone)]
pub struct MarketStream {
    pub symbol: String,
    pub kind: MarketStreamKind,
}

impl MarketStream {
    pub fn parse(stream: &str) -> BuildResult<Self> {
        let Some((symbol, suffix)) = stream.split_once('@') else {
            return Err(BuildError::bad_value(
                "stream",
                stream,
                "expected <symbol>@<stream>",
            ));
        };
        if symbol.is_empty() {
            return Err(BuildError::bad_value("stream", stream, "symbol is empty"));
        }
        let kind = MarketStreamKind::parse(stream, suffix)?;
        Ok(Self {
            symbol: symbol.to_ascii_uppercase(),
            kind,
        })
    }
}
