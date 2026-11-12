use super::fixtures_tests::{FakeSink, Fixtures};
use crate::processor::{CandleProcessor, ProcessorUpdate};
use crate::runner::ProcessorRunner;
use crate::shard_id::ShardId;
use crate::state::AggregatorState;
use platform_market_data::candle::{AlignedWindow, Candle, CandleKey, ClosedCandle};
use platform_market_data::decoders::trade::Trade;
use platform_metrics::descriptions::{
    MARKET_DATA_AGGREGATOR_CLOSED_CANDLES_EVICTED_TOTAL,
    MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_GAP_WINDOWS_TOTAL,
    MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_TRADES_TOTAL,
};
use platform_metrics::test_utils::TestRecorder;
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

#[test]
fn closed_open_keys_keep_a_newer_open_window() {
    let interval = AlignedWindow::try_from("1m").unwrap();
    let pair = TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD);
    let key = CandleKey::new(CexId::Binance, pair, interval);
    let trade_at = |timestamp| Trade {
        exchange: CexId::Binance,
        trading_pair: pair,
        price: Decimal::ONE,
        quantity: Decimal::ONE,
        timestamp: TimestampMilliseconds(timestamp),
    };

    let mut closed = Candle::open(&trade_at(61_000), interval).unwrap();
    closed.close();
    let newer_open = Candle::open(&trade_at(121_000), interval).unwrap();
    let update = ProcessorUpdate {
        closed: vec![ClosedCandle::new(
            closed.clone(),
            TimestampMilliseconds(126_000),
            1,
            0,
        )],
        ..ProcessorUpdate::default()
    };

    let mut state = AggregatorState::new();
    assert_eq!(
        Fixtures::closed_open_keys(&update, &state),
        vec![key.clone()]
    );

    state.open_candles.entry(key).or_default().push(newer_open);
    assert!(Fixtures::closed_open_keys(&update, &state).is_empty());
}

#[tokio::test]
async fn count_only_retention_updates_reach_metrics_without_publishing_candles() {
    let recorder = TestRecorder::default();
    let processor = CandleProcessor::with_config(Fixtures::config().processor_config());
    let interval = AlignedWindow::try_from("1m").unwrap();
    let update = ProcessorUpdate {
        closed_candle_evictions: 3,
        closed_candle_evictions_by_interval: BTreeMap::from([(interval, 3)]),
        out_of_contract_trades: 2,
        out_of_contract_gap_windows: 1,
        ..ProcessorUpdate::default()
    };

    metrics::with_local_recorder(&recorder, || {
        ProcessorRunner::<super::fixtures_tests::FakeSource, FakeSink>::record_update_metrics(
            ShardId::new(0),
            &processor,
            &update,
        );
    });

    assert_eq!(
        recorder.counter(
            MARKET_DATA_AGGREGATOR_CLOSED_CANDLES_EVICTED_TOTAL,
            &[("shard", "0"), ("interval", "1m")],
        ),
        3
    );
    assert_eq!(
        recorder.counter(MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_TRADES_TOTAL, &[]),
        2
    );
    assert_eq!(
        recorder.counter(
            MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_GAP_WINDOWS_TOTAL,
            &[],
        ),
        1
    );

    let sink = FakeSink::default();
    let writer = crate::runner::output_writer::CandleOutputWriter::start(sink.clone());
    writer.enqueue(update, processor.state()).await.unwrap();
    writer.flush().await.unwrap();
    writer.shutdown().await.unwrap();

    assert_eq!(sink.open_updates(), 0);
    assert_eq!(sink.closed_updates(), 0);
    assert_eq!(sink.deleted_open_updates(), 0);
}
