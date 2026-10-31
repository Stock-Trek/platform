//! Candle storage, lookup, equality, and serialization for aggregator state.

use super::AggregatorState;
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::{Candle, CandleInterval, CandleKey};
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

impl AggregatorState {
    /// Returns the newest open candle for a key.
    pub fn open_candle(&self, key: &CandleKey) -> Option<&Candle> {
        self.open_candles
            .get(key)
            .and_then(|candles| candles.last())
    }

    pub fn open_candles_len(&self) -> usize {
        self.open_candles.values().map(Vec::len).sum()
    }

    pub fn closed_candle(
        &self,
        key: &CandleKey,
        window_start: TimestampMilliseconds,
    ) -> Option<&Candle> {
        self.closed_candles.get(key).and_then(|candles| {
            candles
                .binary_search_by_key(&window_start.0, |candle| candle.start_time().0)
                .ok()
                .and_then(|position| candles.get(position))
        })
    }

    pub fn closed_candles_len(&self) -> usize {
        self.closed_candles.values().map(Vec::len).sum()
    }

    pub fn closed_candles_len_by_interval(
        &self,
    ) -> std::collections::BTreeMap<CandleInterval, usize> {
        let mut counts = std::collections::BTreeMap::new();
        for (key, candles) in &self.closed_candles {
            *counts.entry(key.interval).or_default() += candles.len();
        }
        counts
    }

    pub fn would_evict_closed_candidate(
        &self,
        key: &CandleKey,
        window_start: TimestampMilliseconds,
        max_closed_candles: usize,
    ) -> bool {
        if max_closed_candles == 0 {
            return true;
        }
        let Some(candles) = self.closed_candles.get(key) else {
            return false;
        };
        if candles
            .binary_search_by_key(&window_start.0, |candle| candle.start_time().0)
            .is_ok()
        {
            return false;
        }
        candles.len() >= max_closed_candles
            && candles
                .first()
                .is_some_and(|oldest| window_start < oldest.start_time())
    }

    pub fn retain_closed_candle(
        &mut self,
        candle: Candle,
        max_closed_candles: usize,
    ) -> Vec<Candle> {
        let candles = self.closed_candles.entry(candle.key.clone()).or_default();
        match candles
            .binary_search_by_key(&candle.start_time().0, |candidate| candidate.start_time().0)
        {
            Ok(position) => {
                candles[position] = candle;
                Vec::new()
            }
            Err(position) => {
                candles.insert(position, candle);
                Self::evict_excess(candles, max_closed_candles)
            }
        }
    }

    pub fn prune_closed_candles_to_caps(
        &mut self,
        caps: &std::collections::BTreeMap<CandleInterval, usize>,
    ) -> BpResult<Vec<Candle>> {
        let mut evicted = Vec::new();
        for (key, candles) in &mut self.closed_candles {
            let Some(cap) = caps.get(&key.interval).copied() else {
                return Err(BpError::config_invalid(
                    "closed candle retention",
                    format!(
                        "restored state contains unconfigured interval {}",
                        key.interval
                    ),
                ));
            };
            evicted.extend(Self::evict_excess(candles, cap));
        }
        evicted.sort_by(|left, right| {
            left.key
                .subject_suffix()
                .cmp(&right.key.subject_suffix())
                .then_with(|| left.start_time().cmp(&right.start_time()))
        });
        Ok(evicted)
    }

    pub fn candles_use_intervals(&self, intervals: &[CandleInterval]) -> bool {
        self.open_candles
            .keys()
            .chain(self.closed_candles.keys())
            .all(|key| intervals.contains(&key.interval))
    }

    fn evict_excess(candles: &mut Vec<Candle>, max_closed_candles: usize) -> Vec<Candle> {
        let mut evicted = Vec::new();
        while candles.len() > max_closed_candles {
            evicted.push(candles.remove(0));
        }
        evicted
    }
}

pub(super) fn candle_maps_eq(
    left: &HashMap<CandleKey, Vec<Candle>>,
    right: &HashMap<CandleKey, Vec<Candle>>,
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(key, candles)| {
            right.get(key).is_some_and(|other| {
                candles.len() == other.len()
                    && candles
                        .iter()
                        .zip(other)
                        .all(|(candle, other)| candle_eq(candle, other))
            })
        })
}

fn candle_eq(left: &Candle, right: &Candle) -> bool {
    left.key == right.key
        && left.is_complete == right.is_complete
        && left.market_candle.start_time_millis_inc == right.market_candle.start_time_millis_inc
        && left.market_candle.end_time_millis_exc == right.market_candle.end_time_millis_exc
        && left.market_candle.duration_millis == right.market_candle.duration_millis
        && left.market_candle.is_candle_closed == right.market_candle.is_candle_closed
        && left.market_candle.trade_count == right.market_candle.trade_count
        && left.market_candle.ohlcv.open == right.market_candle.ohlcv.open
        && left.market_candle.ohlcv.high == right.market_candle.ohlcv.high
        && left.market_candle.ohlcv.low == right.market_candle.ohlcv.low
        && left.market_candle.ohlcv.close == right.market_candle.ohlcv.close
        && left.market_candle.ohlcv.volume == right.market_candle.ohlcv.volume
        && left.market_candle.ohlcv.quote_volume == right.market_candle.ohlcv.quote_volume
        && left.market_candle.ohlcv.vwap == right.market_candle.ohlcv.vwap
}

/// Serializes grouped open candles as one deterministically sorted list.
pub(super) mod open_candles {
    use super::*;

    pub fn serialize<S>(
        candles: &HashMap<CandleKey, Vec<Candle>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut values = candles
            .values()
            .flat_map(|candles| candles.iter())
            .collect::<Vec<_>>();
        values.sort_by_key(|candle| (candle.key.subject_suffix(), candle.start_time().0));
        values.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<CandleKey, Vec<Candle>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let candles = Vec::<Candle>::deserialize(deserializer)?;
        let mut grouped: HashMap<CandleKey, Vec<Candle>> = HashMap::new();
        for candle in candles {
            grouped.entry(candle.key.clone()).or_default().push(candle);
        }
        for candles in grouped.values_mut() {
            candles.sort_by_key(Candle::start_time);
        }
        Ok(grouped)
    }
}
