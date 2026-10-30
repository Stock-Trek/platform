//! Stable key-to-shard assignment for the aggregator shard stream.

use platform_archive::{RoutingBucket, RoutingKey};
use platform_market_data::{candle::CandleKey, decoders::trade::Trade};
use serde::{Deserialize, Serialize};
use std::fmt;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

/// Identifier of one aggregator shard.
///
/// A shard owns every candle key that hashes to it, so exactly one writer
/// updates those candles at a time.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ShardId(u32);

impl ShardId {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    /// The subject this shard's routed input is published to.
    pub fn subject(self, prefix: &str) -> String {
        format!("{prefix}.{}", self.0)
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Returns the shard that owns `(exchange, trading_pair)`.
///
/// The key is first assigned to a bucket in the archive's fixed virtual hash
/// space, then that bucket is assigned to the active shard. Increasing the
/// shard count only re-distributes whole archive buckets; it never splits
/// one bucket across shards.
pub fn shard_for(exchange: CexId, trading_pair: TradingPair, shard_count: usize) -> ShardId {
    debug_assert!(shard_count > 0, "shard count must be positive");
    debug_assert!(
        shard_count as u64 <= RoutingBucket::COUNT,
        "shard count must not exceed the fixed archive bucket count"
    );
    let bucket = RoutingKey::for_pair(exchange, &trading_pair).bucket();
    ShardId::new(bucket.shard_index(shard_count))
}

/// Returns the shard that owns `key`.
///
/// Candle interval is deliberately excluded: every interval for a trading pair
/// stays on the same shard, which keeps all state for the pair together.
pub fn shard_for_candle_key(key: &CandleKey, shard_count: usize) -> ShardId {
    shard_for(key.exchange, key.trading_pair, shard_count)
}

/// Filters decoded trades down to the ones owned by `shard`.
///
/// A decoder may emit trades for more than one trading pair from a single
/// event, so live processing and archive rebuilds must apply the same ownership
/// check before folding trades into keyed state.
pub fn trades_for_shard(
    trades: impl IntoIterator<Item = Trade>,
    shard: ShardId,
    shard_count: usize,
) -> Vec<Trade> {
    trades
        .into_iter()
        .filter(|trade| shard_for(trade.exchange, trade.trading_pair, shard_count) == shard)
        .collect()
}

/// Every shard in `0..shard_count`, in ascending order.
pub fn all_shards(shard_count: usize) -> Vec<ShardId> {
    (0..shard_count).map(|id| ShardId::new(id as u32)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use stock_trek_types::cex::asset_id::AssetId;

    fn pair() -> TradingPair {
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
    }

    #[test]
    fn shards_are_stable_and_within_range() {
        let first = shard_for(CexId::Binance, pair(), 16);
        let again = shard_for(CexId::Binance, pair(), 16);

        assert_eq!(first, again);
        assert!(first.get() < 16);
    }

    #[test]
    fn different_pairs_can_land_on_different_shards() {
        let bitcoin = shard_for(CexId::Binance, pair(), 16);
        let ethereum = shard_for(
            CexId::Binance,
            TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD),
            16,
        );

        assert!(bitcoin.get() < 16);
        assert!(ethereum.get() < 16);
    }

    #[test]
    fn shards_follow_the_fixed_archive_buckets() {
        let bucket = RoutingKey::for_pair(CexId::Binance, &pair()).bucket();

        for shard_count in [1, 2, 8, 16, 100] {
            assert_eq!(
                shard_for(CexId::Binance, pair(), shard_count),
                ShardId::new(bucket.shard_index(shard_count))
            );
        }
    }

    #[test]
    fn build_shard_subjects() {
        assert_eq!(
            ShardId::new(3).subject("aggregator.shards"),
            "aggregator.shards.3"
        );
        assert_eq!(all_shards(3).len(), 3);
    }
}
