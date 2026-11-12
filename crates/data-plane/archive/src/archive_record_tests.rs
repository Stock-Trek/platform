use crate::{ArchiveKind, ArchiveRecord, ArchiveRecordKey, ArchiveSortKey, ArchivedMarketEvent};
use platform_market_data::{
    candle::{AlignedWindow, CandleKey, ClosedCandle, MarketOhlcv},
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

fn event() -> ArchiveRecord {
    ArchiveRecord::MarketEvent(
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btc/usdt@trade".to_string(),
            sequence: Some(3),
            exchange_timestamp: Some(TimestampMilliseconds(120_000)),
            received_at: TimestampMilliseconds(130_000),
            payload: "payload".to_string(),
        }
        .into(),
    )
}

fn closed_candle() -> ArchiveRecord {
    let candle = platform_market_data::candle::Candle {
        key: CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            AlignedWindow::try_from("1m").unwrap(),
        ),
        start_time_millis_inc: 60_000,
        end_time_millis_exc: 120_000,
        is_candle_closed: true,
        ohlcv: MarketOhlcv::new(1.0, 2.0, 0.5, 1.5, 10.0, 15.0).unwrap(),
        trade_count: 5,
        is_complete: true,
    };
    ArchiveRecord::ClosedCandle(ClosedCandle::new(
        candle,
        TimestampMilliseconds(121_000),
        4,
        0,
    ))
}

#[test]
fn events_and_candles_use_their_own_timestamps_and_kinds() {
    let event = event();
    assert_eq!(event.kind(), ArchiveKind::Events);
    assert_eq!(event.timestamp(), TimestampMilliseconds(120_000));
    assert_eq!(
        event.sort_key(),
        ArchiveSortKey::MarketEvent {
            timestamp: 120_000,
            sequence: 3,
            state_epoch: 0,
        }
    );

    let candle = closed_candle();
    assert_eq!(candle.kind(), ArchiveKind::Candles);
    assert_eq!(candle.timestamp(), TimestampMilliseconds(60_000));
    assert_eq!(
        candle.sort_key(),
        ArchiveSortKey::ClosedCandle {
            start_time: 60_000,
            close_watermark: 121_000,
            source_offset: 4,
            state_epoch: 0,
        }
    );
}

#[test]
fn event_state_epoch_is_part_of_its_archive_identity() {
    let event = event();
    let ArchiveRecord::MarketEvent(archived) = &event else {
        panic!("expected a market event");
    };
    let later_epoch = ArchiveRecord::MarketEvent(ArchivedMarketEvent::new(
        archived.event.clone(),
        archived.state_epoch + 1,
    ));

    assert_ne!(event.key(), later_epoch.key());
    assert!(event.sort_key() < later_epoch.sort_key());
}

#[test]
fn closed_candle_keys_use_the_deterministic_revision_id() {
    let candle = closed_candle();
    let ArchiveRecord::ClosedCandle(closed) = &candle else {
        panic!("expected a closed candle");
    };

    assert_eq!(
        candle.key(),
        ArchiveRecordKey::ClosedCandle {
            revision_id: closed.message_id(),
        }
    );
}

#[test]
fn candle_sort_keys_order_revision_provenance() {
    let candle = closed_candle();
    let ArchiveRecord::ClosedCandle(closed) = &candle else {
        panic!("expected a closed candle");
    };

    let later_watermark = ArchiveRecord::ClosedCandle(ClosedCandle::new(
        closed.candle().clone(),
        TimestampMilliseconds(122_000),
        closed.revision().source_offset,
        closed.revision().state_epoch,
    ));
    assert!(candle.sort_key() < later_watermark.sort_key());

    let later_epoch = ArchiveRecord::ClosedCandle(ClosedCandle::new(
        closed.candle().clone(),
        closed.revision().close_watermark,
        closed.revision().source_offset,
        1,
    ));
    assert!(candle.sort_key() < later_epoch.sort_key());
}

#[test]
fn partitions_sanitize_path_segments() {
    let partition = event().partition();
    assert_eq!(
        partition.path(),
        format!(
            "kind=events/exchange={}/stream=btc_usdt@trade",
            CexId::Binance
        )
    );

    let partition = closed_candle().partition();
    assert_eq!(
        partition.path(),
        format!(
            "kind=candles/exchange={}/base={}/quote={}/interval=1m",
            CexId::Binance,
            AssetId::Bitcoin,
            AssetId::TetherUSD
        )
    );
}
