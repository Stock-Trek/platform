use super::*;
use platform_archive::RoutingKey;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};
use stock_trek_types::cex::asset_id::AssetId;

fn pair() -> TradingPair {
    TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
}

#[test]
fn shards_are_stable_and_within_range() {
    let first = ShardId::for_trading_pair(CexId::Binance, pair(), 16);
    let again = ShardId::for_trading_pair(CexId::Binance, pair(), 16);

    assert_eq!(first, again);
    assert!(first.get() < 16);
}

#[test]
fn different_pairs_can_land_on_different_shards() {
    let bitcoin = ShardId::for_trading_pair(CexId::Binance, pair(), 16);
    let ethereum = ShardId::for_trading_pair(
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
            ShardId::for_trading_pair(CexId::Binance, pair(), shard_count),
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
    assert_eq!(ShardId::all(3).len(), 3);
}
