//! Stable routing keys shared by live aggregation and archived objects.
//!
//! A routing key is a pure function of `(exchange, trading pair)`. Archive
//! files are grouped into a fixed number of virtual buckets derived from that
//! key, so changing the active shard count only changes which buckets a
//! shard owns. It never changes the contents of an archive bucket.

use platform_market_data::{decoders::trade::TradeDecoderFactory, event::MarketEvent};
use std::fmt;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

/// Number of virtual hash buckets used by the archive layout.
///
/// The value is intentionally independent of the aggregator's active shard
/// count. A power of two keeps the bucket path compact while still spreading
/// keys evenly for the shard counts used in practice.
pub const ROUTING_BUCKET_COUNT: u64 = 1 << 16;

const ROUTING_BUCKET_FIELD: &str = "bucket=";

/// The stable hash of a trading pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoutingKey(u64);

impl RoutingKey {
    /// Builds the routing key for an exchange and trading pair.
    pub fn for_pair(exchange: CexId, trading_pair: &TradingPair) -> Self {
        let key = format!("{exchange}.{}.{}", trading_pair.base, trading_pair.quote);
        Self(Self::fnv1a_64(key.as_bytes()))
    }

    /// Builds the routing key for a market event.
    ///
    /// Trade streams always identify one trading pair. Resolving the pair from
    /// the stream avoids decoding the event payload just to archive it. Events
    /// that cannot be resolved still receive a deterministic stream-scoped key
    /// so they are never dropped by the archiver.
    pub fn for_event(event: &MarketEvent) -> Self {
        match TradeDecoderFactory::trading_pair_for_stream(event.exchange, &event.stream) {
            Ok(trading_pair) => Self::for_pair(event.exchange, &trading_pair),
            Err(_) => {
                let key = format!("{}.stream.{}", event.exchange, event.stream);
                Self(Self::fnv1a_64(key.as_bytes()))
            }
        }
    }

    /// The raw 64-bit hash.
    pub fn value(self) -> u64 {
        self.0
    }

    /// The virtual archive bucket for this key.
    pub fn bucket(self) -> RoutingBucket {
        RoutingBucket::from_key(self)
    }
}

/// One virtual bucket in the archive's fixed hash space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoutingBucket(u64);

impl RoutingBucket {
    /// The number of virtual buckets.
    pub const COUNT: u64 = ROUTING_BUCKET_COUNT;

    /// Builds a bucket from a routing key.
    pub fn from_key(key: RoutingKey) -> Self {
        Self(key.value() % Self::COUNT)
    }

    /// Parses a bucket index, rejecting values outside the fixed hash space.
    pub fn from_index(index: u64) -> Option<Self> {
        (index < Self::COUNT).then_some(Self(index))
    }

    /// The zero-based bucket index.
    pub fn index(self) -> u64 {
        self.0
    }

    /// The shard that owns this bucket for the given active shard count.
    pub fn shard_index(self, shard_count: usize) -> u32 {
        debug_assert!(shard_count > 0, "shard count must be positive");
        let shard_count = shard_count.max(1) as u64;
        (self.0 % shard_count) as u32
    }

    /// Whether the given active shard owns this bucket.
    ///
    /// Bucket ownership is derived from the fixed virtual bucket index, so this
    /// is the single check archive readers use to select their own slice of
    /// the archive.
    pub fn is_owned_by(self, shard: u32, shard_count: usize) -> bool {
        self.shard_index(shard_count) == shard
    }

    /// The object-key path segment for this bucket.
    pub fn segment(self) -> String {
        format!("{ROUTING_BUCKET_FIELD}{:05}", self.0)
    }

    /// Parses the bucket segment from an archive object key or partition path.
    pub fn parse(path: &str) -> Option<Self> {
        path.split('/').find_map(|segment| {
            let index = segment.strip_prefix(ROUTING_BUCKET_FIELD)?.parse().ok()?;
            Self::from_index(index)
        })
    }
}

impl fmt::Display for RoutingBucket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:05}", self.0)
    }
}

impl RoutingKey {
    fn fnv1a_64(bytes: &[u8]) -> u64 {
        const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stock_trek_types::cex::asset_id::AssetId;

    fn bitcoin_pair() -> TradingPair {
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
    }

    #[test]
    fn routing_keys_are_stable_and_pair_specific() {
        let first = RoutingKey::for_pair(CexId::Binance, &bitcoin_pair());
        let again = RoutingKey::for_pair(CexId::Binance, &bitcoin_pair());
        let ethereum = RoutingKey::for_pair(
            CexId::Binance,
            &TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD),
        );

        assert_eq!(first, again);
        assert_ne!(first, ethereum);
        assert!(first.bucket().index() < RoutingBucket::COUNT);
    }

    #[test]
    fn events_and_candles_for_the_same_pair_share_a_routing_key() {
        let event = MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(1),
            exchange_timestamp: None,
            received_at: platform_types::new_types::TimestampMilliseconds(0),
            payload: String::new(),
        };

        assert_eq!(
            RoutingKey::for_event(&event),
            RoutingKey::for_pair(CexId::Binance, &bitcoin_pair())
        );
    }

    #[test]
    fn bucket_segments_round_trip() {
        let key = RoutingKey::for_pair(CexId::Binance, &bitcoin_pair());
        let bucket = key.bucket();
        let path = format!(
            "events/{}/year=2024/month=01/day=02/hour=03",
            bucket.segment()
        );

        assert_eq!(RoutingBucket::parse(&path), Some(bucket));
        assert_eq!(RoutingBucket::parse("events/year=2024"), None);
    }

    #[test]
    fn shard_assignment_is_derived_from_the_archive_bucket() {
        let bucket = RoutingKey::for_pair(CexId::Binance, &bitcoin_pair()).bucket();

        for shard_count in [1, 2, 8, 16, 100] {
            assert!(bucket.shard_index(shard_count) < shard_count as u32);
        }
    }

    #[test]
    fn every_archive_bucket_has_exactly_one_owning_shard() {
        for shard_count in [1_u32, 2, 8, 16, 100] {
            let mut owned = vec![0_usize; shard_count as usize];
            for index in 0..RoutingBucket::COUNT {
                let bucket = RoutingBucket::from_index(index).unwrap();
                let shard = bucket.shard_index(shard_count as usize);
                assert!(bucket.is_owned_by(shard, shard_count as usize));
                owned[shard as usize] += 1;
            }
            assert_eq!(owned.iter().sum::<usize>(), RoutingBucket::COUNT as usize);
        }
    }
}
