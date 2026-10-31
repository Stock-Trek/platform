use super::*;
use platform_market_data::event::MarketEvent;
use stock_trek_types::cex::asset_id::AssetId;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

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
