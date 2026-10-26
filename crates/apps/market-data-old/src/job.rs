use crate::{constants::common_trading_pairs, feed::Feed, path::MarketDataPath, s3::ObjectKey};
use chrono::{DateTime, NaiveDate, Utc};
use platform_errors::{BpError, BpResult, DataError};
use platform_exchange_types::urls::TradingMode;
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::{Map, Value};
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

/// The operation requested from the data Lambda.
///
/// The canonical wire format is an internally tagged object, for example:
///
/// ```json
/// { "mode": "backfill", "pair": "paper/binance/bitcoin/tetherusd",
///   "feed": "trades", "start": "2024-01-01T00:00:00Z",
///   "end": "2024-01-02T00:00:00Z", "force": true }
/// ```
///
/// The payload must explicitly include a `mode`, `job`, or `action` field
/// whose value is one of `update`, `backfill`, or `compact`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Job {
    Update,
    Backfill(BackfillJob),
    Compact(CompactJob),
}

impl<'de> Deserialize<'de> for Job {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse_value(Value::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl Job {
    /// Parse a Lambda payload, accepting `mode`, `job`, or `action` as the tag.
    ///
    /// A payload without an explicit mode is rejected rather than defaulting to
    /// a scheduled update.
    pub fn from_value(value: Value) -> BpResult<Self> {
        let job = Self::parse_value(value)?;
        job.validate()?;
        Ok(job)
    }

    fn parse_value(value: Value) -> BpResult<Self> {
        match value {
            Value::Object(mut object) => {
                let mut tag_keys = ["mode", "job", "action"]
                    .into_iter()
                    .filter(|key| object.contains_key(*key));
                let Some(tag_key) = tag_keys.next() else {
                    return Err(BpError::Data(DataError::InvalidJob {
                        message: "job payload must include one of mode, job, or action".to_string(),
                    }));
                };
                if tag_keys.next().is_some() {
                    return Err(BpError::Data(DataError::InvalidJob {
                        message: "job payload must include only one of mode, job, or action"
                            .to_string(),
                    }));
                }
                let tag = object.remove(tag_key).unwrap_or(Value::Null);
                match tag {
                    Value::String(mode) => Self::from_mode(&mode, object),
                    Value::Object(nested) => {
                        if nested.len() != 1 {
                            return Err(BpError::Data(DataError::InvalidJob {
                                message: "a nested job mode must contain exactly one entry"
                                    .to_string(),
                            }));
                        }
                        let (mode, payload) = nested.into_iter().next().unwrap();
                        match payload {
                            Value::Object(payload) => {
                                object.extend(payload);
                                Self::from_mode(&mode, object)
                            }
                            Value::Null => Self::from_mode(&mode, object),
                            _ => Err(BpError::Data(DataError::InvalidJob {
                                message: format!(
                                    "nested mode {mode} must contain an object payload"
                                ),
                            })),
                        }
                    }
                    other => Err(BpError::Data(DataError::InvalidJob {
                        message: format!("job mode must be a string, got {other}"),
                    })),
                }
            }
            other => Err(BpError::Data(DataError::InvalidJob {
                message: format!("job payload must be an object, got {other}"),
            })),
        }
    }

    pub fn mode(&self) -> &'static str {
        match self {
            Self::Update => "update",
            Self::Backfill(_) => "backfill",
            Self::Compact(_) => "compact",
        }
    }

    /// Validate the job before any S3 or exchange work starts.
    pub fn validate(&self) -> BpResult<()> {
        match self {
            Self::Update => Ok(()),
            Self::Backfill(backfill) => backfill.validate(),
            Self::Compact(compact) => compact.prefix().map(|_| ()),
        }
    }

    fn from_mode(mode: &str, object: Map<String, Value>) -> BpResult<Self> {
        match mode {
            "update" => {
                if object.is_empty() {
                    Ok(Self::Update)
                } else {
                    Err(BpError::Data(DataError::InvalidJob {
                        message: format!(
                            "update job does not accept fields: {}",
                            object.keys().cloned().collect::<Vec<_>>().join(", ")
                        ),
                    }))
                }
            }
            "backfill" => serde_json::from_value(Value::Object(object))
                .map(Self::Backfill)
                .map_err(|error| {
                    BpError::Data(DataError::InvalidJob {
                        message: format!("invalid backfill job: {error}"),
                    })
                }),
            "compact" => serde_json::from_value(Value::Object(object))
                .map(Self::Compact)
                .map_err(|error| {
                    BpError::Data(DataError::InvalidJob {
                        message: format!("invalid compact job: {error}"),
                    })
                }),
            other => Err(BpError::Data(DataError::InvalidJob {
                message: format!("unknown job mode {other}; expected update, backfill, or compact"),
            })),
        }
    }
}

/// A targeted backfill for one trading pair and time range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillJob {
    pub pair: TradingPairPath,
    /// The type of market data to backfill.
    pub feed: Feed,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub start: DateTime<Utc>,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub end: DateTime<Utc>,
    /// Re-fetch and overwrite windows that already have data.
    ///
    /// Compacted periods are still skipped because rebuilding a compacted
    /// roll-up is a separate compaction concern. Skipped compacted windows are
    /// reported so a forced backfill cannot silently do nothing.
    pub force: bool,
}

impl BackfillJob {
    pub fn new(
        pair: impl Into<TradingPairPath>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        force: bool,
    ) -> Self {
        Self {
            pair: pair.into(),
            feed: Feed::Trades,
            start,
            end,
            force,
        }
    }

    pub fn with_feed(mut self, feed: Feed) -> Self {
        self.feed = feed;
        self
    }

    pub fn validate(&self) -> BpResult<()> {
        if self.start >= self.end {
            return Err(BpError::Data(DataError::InvalidBackfillRange {
                start: self.start,
                end: self.end,
            }));
        }
        self.pair.resolve().map(|_| ())
    }

    pub fn resolve(&self) -> BpResult<ResolvedTradingPair> {
        self.pair.resolve()
    }
}

/// A targeted compaction for one prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactJob {
    pub target: String,
}

impl CompactJob {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
        }
    }

    /// Normalize the target into the prefix expected by the compactor.
    ///
    /// Compaction deletes source files below the returned prefix, so the target
    /// must address the data namespace for a configured trading pair and stop at
    /// a valid compaction depth. Arbitrary prefixes are rejected before any S3
    /// listing or deletion takes place.
    pub fn prefix(&self) -> BpResult<ObjectKey> {
        MarketDataPath::compact_prefix(&self.target)
    }
}

/// A trading-pair path such as `paper/binance/bitcoin/tetherusd`.
///
/// It can be supplied as a string or as an object with `trading_mode`, `cex_id`,
/// `base`, and `quote` fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradingPairPath(String);

impl TradingPairPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn resolve(&self) -> BpResult<ResolvedTradingPair> {
        let normalized = self.0.trim().trim_matches('/').replace([':', '\\'], "/");
        if normalized.is_empty() {
            return Err(BpError::Data(DataError::InvalidTradingPairPath {
                pair: self.0.clone(),
            }));
        }
        for (cex_id, trading_mode, trading_pairs) in common_trading_pairs() {
            for trading_pair in trading_pairs {
                let candidate = MarketDataPath::trading_pair(
                    trading_mode,
                    cex_id,
                    trading_pair.base,
                    trading_pair.quote,
                );
                if candidate.eq_ignore_ascii_case(&normalized) {
                    return Ok(ResolvedTradingPair {
                        cex_id,
                        trading_mode,
                        trading_pair,
                    });
                }
            }
        }
        Err(BpError::Data(DataError::UnsupportedTradingPair {
            pair: self.0.clone(),
        }))
    }
}

impl From<&str> for TradingPairPath {
    fn from(path: &str) -> Self {
        Self::new(path)
    }
}

impl From<String> for TradingPairPath {
    fn from(path: String) -> Self {
        Self::new(path)
    }
}

impl Serialize for TradingPairPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for TradingPairPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum TradingPairPathRepr {
            Path(String),
            Parts(TradingPairPathParts),
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TradingPairPathParts {
            trading_mode: String,
            cex_id: String,
            base: String,
            quote: String,
        }

        match TradingPairPathRepr::deserialize(deserializer)? {
            TradingPairPathRepr::Path(path) => Ok(Self::new(path)),
            TradingPairPathRepr::Parts(parts) => Ok(Self::new(MarketDataPath::trading_pair(
                parts.trading_mode,
                parts.cex_id,
                parts.base,
                parts.quote,
            ))),
        }
    }
}

/// A configured trading pair resolved from a [`TradingPairPath`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedTradingPair {
    pub cex_id: CexId,
    pub trading_mode: TradingMode,
    pub trading_pair: TradingPair,
}

/// Summary of the work performed by a job.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct JobOutcome {
    pub windows_ingested: usize,
    pub windows_compacted: usize,
    pub windows_incomplete: usize,
    pub windows_deadline: usize,
    pub compaction_targets: usize,
    pub incomplete: bool,
}

impl JobOutcome {
    pub fn is_incomplete(&self) -> bool {
        self.incomplete || self.windows_incomplete > 0 || self.windows_deadline > 0
    }

    pub fn status(&self) -> &'static str {
        if self.is_incomplete() {
            "incomplete"
        } else {
            "ok"
        }
    }
}

fn deserialize_timestamp<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if let Ok(date_time) = DateTime::parse_from_rfc3339(&value) {
        return Ok(date_time.with_timezone(&Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(&value, "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .map(|date_time| date_time.and_utc())
            .ok_or_else(|| de::Error::custom(format!("invalid date {value}")));
    }
    Err(de::Error::custom(format!(
        "expected an RFC 3339 timestamp or YYYY-MM-DD date, got {value}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn utc(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
            .single()
            .unwrap()
    }

    #[test]
    fn payloads_without_a_mode_are_rejected() {
        assert!(Job::from_value(json!({})).is_err());
        assert!(Job::from_value(Value::Null).is_err());
        assert!(Job::from_value(json!({ "mod": "backfill", "strat": "2024-01-01" })).is_err());
    }

    #[test]
    fn multiple_mode_keys_are_rejected() {
        assert!(Job::from_value(json!({ "mode": "update", "job": "update" })).is_err());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(Job::from_value(json!({ "mode": "update", "extra": true })).is_err());
        assert!(
            Job::from_value(json!({
                "mode": "backfill",
                "pair": "paper/binance/bitcoin/tetherusd",
                "feed": "trades",
                "start": "2024-01-01",
                "end": "2024-01-02",
                "force": false,
                "extra": true
            }))
            .is_err()
        );
        assert!(
            Job::from_value(json!({
                "mode": "compact",
                "target": "paper/binance/bitcoin/tetherusd",
                "extra": true
            }))
            .is_err()
        );
        assert!(
            Job::from_value(json!({
                "mode": "backfill",
                "pair": {
                    "trading_mode": "Paper",
                    "exchange": "Binance",
                    "base": "Bitcoin",
                    "quote": "TetherUSD",
                    "extra": true
                },
                "feed": "trades",
                "start": "2024-01-01",
                "end": "2024-01-02",
                "force": false
            }))
            .is_err()
        );
    }

    #[test]
    fn backfill_requires_explicit_force() {
        assert!(
            Job::from_value(json!({
                "mode": "backfill",
                "pair": "paper/binance/bitcoin/tetherusd",
                "feed": "trades",
                "start": "2024-01-01",
                "end": "2024-01-02"
            }))
            .is_err()
        );
    }

    #[test]
    fn backfill_requires_an_explicit_feed() {
        assert!(
            Job::from_value(json!({
                "mode": "backfill",
                "pair": "paper/binance/bitcoin/tetherusd",
                "start": "2024-01-01",
                "end": "2024-01-02",
                "force": false
            }))
            .is_err()
        );
    }

    #[test]
    fn direct_deserialization_uses_the_same_strict_contract() {
        assert_eq!(
            serde_json::from_value::<Job>(json!({ "job": "update" })).unwrap(),
            Job::Update
        );
        assert!(serde_json::from_value::<Job>(json!({ "mode": "update", "extra": true })).is_err());
    }

    #[test]
    fn backfill_is_parsed_with_force() {
        let job = Job::from_value(json!({
            "mode": "backfill",
            "pair": "paper/binance/bitcoin/tetherusd",
            "feed": "trades",
            "start": "2024-01-01",
            "end": "2024-01-02T00:00:00Z",
            "force": true
        }))
        .unwrap();
        let Job::Backfill(backfill) = job else {
            panic!("expected a backfill job");
        };
        assert_eq!(backfill.pair.as_str(), "paper/binance/bitcoin/tetherusd");
        assert_eq!(backfill.feed, Feed::Trades);
        assert_eq!(backfill.start, utc(2024, 1, 1));
        assert_eq!(backfill.end, utc(2024, 1, 2));
        assert!(backfill.force);
        backfill.validate().unwrap();
    }

    #[test]
    fn backfill_accepts_a_feed() {
        let job = Job::from_value(json!({
            "mode": "backfill",
            "pair": "paper/binance/bitcoin/tetherusd",
            "feed": "open_interest",
            "start": "2024-01-01",
            "end": "2024-01-02",
            "force": false
        }))
        .unwrap();
        let Job::Backfill(backfill) = job else {
            panic!("expected a backfill job");
        };
        assert_eq!(backfill.feed, Feed::OpenInterest);
    }

    #[test]
    fn trading_pair_path_accepts_structured_parts() {
        let job = Job::from_value(json!({
            "mode": "backfill",
            "pair": {
                "trading_mode": "Paper",
                "cex_id": "Binance",
                "base": "Bitcoin",
                "quote": "TetherUSD"
            },
            "feed": "trades",
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-01T01:00:00Z",
            "force": false
        }))
        .unwrap();
        let Job::Backfill(backfill) = job else {
            panic!("expected a backfill job");
        };
        let resolved = backfill.resolve().unwrap();
        assert_eq!(resolved.trading_mode, TradingMode::Paper);
        assert_eq!(resolved.cex_id, CexId::Binance);
    }

    #[test]
    fn nested_mode_form_is_accepted() {
        let job = Job::from_value(json!({
            "mode": {
                "compact": {
                    "target": "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01/"
                }
            }
        }))
        .unwrap();
        let Job::Compact(compact) = job else {
            panic!("expected a compact job");
        };
        assert_eq!(
            compact.prefix().unwrap().as_str(),
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01/"
        );
    }

    #[test]
    fn compact_prefix_is_normalized() {
        let compact = CompactJob::new("paper/binance/bitcoin/tetherusd/trades/raw");
        assert_eq!(
            compact.prefix().unwrap().as_str(),
            "paper/binance/bitcoin/tetherusd/trades/raw/"
        );
        let compact =
            CompactJob::new("paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01/00/06.parquet");
        assert_eq!(
            compact.prefix().unwrap().as_str(),
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01/00/"
        );
    }

    #[test]
    fn compact_rejects_prefixes_outside_the_data_namespace() {
        for target in [
            "random",
            "random/prefix/with/segments",
            "paper/binance/bitcoin",
            "paper/binance/bitcoin/dogecoin",
            "other/binance/bitcoin/tetherusd",
            "_locks/market-data-updater",
            "/paper/binance/bitcoin/tetherusd",
            "paper//binance/bitcoin/tetherusd",
            "paper/binance/bitcoin/tetherusd/../other",
            "paper/binance/bitcoin/tetherusd",
            "paper/binance/bitcoin/tetherusd/not-a-feed",
            "paper/binance/bitcoin/tetherusd/not-a-feed/2024",
            "paper/binance/bitcoin/tetherusd/not-a-feed/raw",
            "paper/binance/bitcoin/tetherusd/not-a-feed/raw/2024",
        ] {
            assert!(
                CompactJob::new(target).prefix().is_err(),
                "expected compact target {target} to be rejected"
            );
        }
    }

    #[test]
    fn compact_rejects_invalid_depths_and_time_segments() {
        for target in [
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01/00/00",
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01/00/00/00",
            "paper/binance/bitcoin/tetherusd/trades/raw/not-a-year",
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/13",
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/32",
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01/24",
        ] {
            assert!(
                CompactJob::new(target).prefix().is_err(),
                "expected compact target {target} to be rejected"
            );
        }
    }

    #[test]
    fn compact_accepts_each_valid_data_depth() {
        for target in [
            "paper/binance/bitcoin/tetherusd/trades/raw",
            "paper/binance/bitcoin/tetherusd/trades/raw/2024",
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01",
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01",
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01/00",
            "paper/binance/bitcoin/tetherusd/trades/raw/2024/01/01/00/06.parquet",
        ] {
            assert!(
                CompactJob::new(target).prefix().is_ok(),
                "expected compact target {target} to be accepted"
            );
        }
    }

    #[test]
    fn invalid_modes_and_ranges_are_rejected() {
        assert!(Job::from_value(json!({ "mode": "unknown" })).is_err());
        assert!(
            Job::from_value(json!({
                "mode": "backfill",
                "pair": "paper/binance/bitcoin/tetherusd",
                "feed": "trades",
                "start": "2024-01-02",
                "end": "2024-01-01",
                "force": false
            }))
            .is_err()
        );
        assert!(
            Job::from_value(json!({
                "mode": "backfill",
                "pair": "paper/binance/bitcoin/tetherusd",
                "feed": "trades",
                "start": "2024-01-01",
                "end": "2024-01-02",
                "force": false
            }))
            .unwrap()
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn unsupported_pair_is_rejected() {
        let pair = TradingPairPath::new("paper/binance/dogecoin/tetherusd");
        assert!(pair.resolve().is_err());
    }

    #[test]
    fn outcome_status_distinguishes_compacted_incomplete_and_deadline_windows() {
        assert_eq!(JobOutcome::default().status(), "ok");
        assert_eq!(
            JobOutcome {
                windows_compacted: 2,
                ..JobOutcome::default()
            }
            .status(),
            "ok"
        );
        assert_eq!(
            JobOutcome {
                windows_incomplete: 2,
                ..JobOutcome::default()
            }
            .status(),
            "incomplete"
        );
        assert_eq!(
            JobOutcome {
                windows_deadline: 2,
                ..JobOutcome::default()
            }
            .status(),
            "incomplete"
        );
        assert_eq!(
            JobOutcome {
                incomplete: true,
                ..JobOutcome::default()
            }
            .status(),
            "incomplete"
        );
    }
}
