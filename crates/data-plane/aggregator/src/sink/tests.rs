use super::*;
use platform_market_data::{candle::CandleInterval, decoders::trade::Trade};
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

fn closed_candle(source_offset: u64, state_epoch: u64) -> ClosedCandle {
    let trade = Trade {
        exchange: CexId::Binance,
        trading_pair: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        price: Decimal::from_str_exact("100").unwrap(),
        quantity: Decimal::ONE,
        timestamp: TimestampMilliseconds(61_000),
    };
    let mut candle = Candle::open(&trade, CandleInterval::from_label("1m").unwrap()).unwrap();
    candle.close();
    ClosedCandle::new(
        candle,
        TimestampMilliseconds(121_000),
        source_offset,
        state_epoch,
    )
}

#[test]
fn closed_candle_targets_are_deterministic_and_key_scoped() {
    let first = closed_candle(7, 0);
    let replay = closed_candle(7, 0);

    let first_target = NatsCandleSink::publish_target("market.candles", &first);
    let replay_target = NatsCandleSink::publish_target("market.candles", &replay);

    assert_eq!(first_target, replay_target);
    assert_eq!(
        first_target.0,
        "market.candles.binance.bitcoin.tether_usd.1m"
    );
    assert!(first_target.1.contains("binance.bitcoin.tether_usd.1m"));
    assert!(first_target.1.contains("epoch=0"));
}

#[test]
fn closed_candle_targets_change_with_revision_and_epoch() {
    let base = NatsCandleSink::publish_target("market.candles", &closed_candle(7, 0));
    let other_offset = NatsCandleSink::publish_target("market.candles", &closed_candle(8, 0));
    let other_epoch = NatsCandleSink::publish_target("market.candles", &closed_candle(7, 1));

    assert_ne!(base.1, other_offset.1);
    assert_ne!(base.1, other_epoch.1);
}
