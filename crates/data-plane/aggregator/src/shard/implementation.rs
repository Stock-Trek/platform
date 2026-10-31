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

impl ShardId {
    /// Returns the shard that owns `(exchange, trading_pair)`.
    ///
    /// The key is first assigned to a bucket in the archive's fixed virtual hash
    /// space, then that bucket is assigned to the active shard. Increasing the
    /// shard count only re-distributes whole archive buckets; it never splits
    /// one bucket across shards.
    pub fn for_trading_pair(
        exchange: CexId,
        trading_pair: TradingPair,
        shard_count: usize,
    ) -> ShardId {
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
    /// Candle interval is deliberately excluded: every interval for a trading
    /// pair stays on the same shard, which keeps all state for the pair
    /// together.
    pub fn for_candle_key(key: &CandleKey, shard_count: usize) -> ShardId {
        Self::for_trading_pair(key.exchange, key.trading_pair, shard_count)
    }

    /// Filters decoded trades down to the ones owned by `shard`.
    ///
    /// A decoder may emit trades for more than one trading pair from a single
    /// event, so live processing and archive rebuilds must apply the same
    /// ownership check before folding trades into keyed state.
    pub fn trades_for_shard(
        trades: impl IntoIterator<Item = Trade>,
        shard: ShardId,
        shard_count: usize,
    ) -> Vec<Trade> {
        trades
            .into_iter()
            .filter(|trade| {
                Self::for_trading_pair(trade.exchange, trade.trading_pair, shard_count) == shard
            })
            .collect()
    }

    /// Every shard in `0..shard_count`, in ascending order.
    pub fn all(shard_count: usize) -> Vec<ShardId> {
        (0..shard_count).map(|id| ShardId::new(id as u32)).collect()
    }
}
