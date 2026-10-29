//! Stable key-to-partition assignment for the aggregator shard stream.

use platform_market_data::candle::CandleKey;
use serde::{Deserialize, Serialize};
use std::fmt;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

/// Identifier of one aggregator partition.
///
/// A partition owns every candle key that hashes to it, so exactly one writer
/// updates those candles at a time.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct PartitionId(u32);

impl PartitionId {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    /// The subject this partition's routed input is published to.
    pub fn subject(self, prefix: &str) -> String {
        format!("{prefix}.{}", self.0)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Returns the partition that owns `(exchange, trading_pair)`.
///
/// The hash is a small, dependency-free FNV-1a implementation over the stable
/// display strings. It is deterministic across processes and releases, which is
/// required for every replica to build the same routing table.
pub fn partition_for(
    exchange: CexId,
    trading_pair: TradingPair,
    partition_count: usize,
) -> PartitionId {
    debug_assert!(partition_count > 0, "partition count must be positive");
    let partition_count = partition_count.max(1);
    let key = format!("{exchange}.{}.{}", trading_pair.base, trading_pair.quote);
    PartitionId::new((fnv1a_64(key.as_bytes()) % partition_count as u64) as u32)
}

/// Returns the partition that owns `key`.
///
/// Candle interval is deliberately excluded: every interval for a trading pair
/// stays on the same partition, which keeps all state for the pair together.
pub fn partition_for_candle_key(key: &CandleKey, partition_count: usize) -> PartitionId {
    partition_for(key.exchange, key.trading_pair, partition_count)
}

/// Every partition in `0..partition_count`, in ascending order.
pub fn all_partitions(partition_count: usize) -> Vec<PartitionId> {
    (0..partition_count)
        .map(|id| PartitionId::new(id as u32))
        .collect()
}

fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use stock_trek_types::cex::asset_id::AssetId;

    fn pair() -> TradingPair {
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
    }

    #[test]
    fn partitions_are_stable_and_within_range() {
        let first = partition_for(CexId::Binance, pair(), 16);
        let again = partition_for(CexId::Binance, pair(), 16);

        assert_eq!(first, again);
        assert!(first.get() < 16);
    }

    #[test]
    fn different_pairs_can_land_on_different_partitions() {
        let bitcoin = partition_for(CexId::Binance, pair(), 16);
        let ethereum = partition_for(
            CexId::Binance,
            TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD),
            16,
        );

        assert!(bitcoin.get() < 16);
        assert!(ethereum.get() < 16);
    }

    #[test]
    fn build_partition_subjects() {
        assert_eq!(
            PartitionId::new(3).subject("aggregator.shards"),
            "aggregator.shards.3"
        );
        assert_eq!(all_partitions(3).len(), 3);
    }
}
