use crate::processor::ProcessorConfig;
use platform_market_data::{AlignedWindow, GapMarker, MarketEvent, Trade};
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

pub(super) struct Fixtures;

impl Fixtures {
    pub(super) fn trade(price: &str, timestamp: i64) -> Trade {
        Self::trade_for(
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            price,
            timestamp,
        )
    }

    pub(super) fn trade_for(pair: TradingPair, price: &str, timestamp: i64) -> Trade {
        Trade {
            exchange: CexId::Binance,
            trading_pair: pair,
            price: Decimal::from_str_exact(price).unwrap(),
            quantity: Decimal::ONE,
            timestamp: TimestampMilliseconds(timestamp),
        }
    }

    pub(super) fn event(sequence: u64, timestamp: i64) -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(timestamp)),
            received_at: TimestampMilliseconds(timestamp + 1),
            payload: String::new(),
        }
    }

    pub(super) fn one_minute() -> AlignedWindow {
        AlignedWindow::try_from("1m").unwrap()
    }

    pub(super) fn gap(detected_at: i64) -> GapMarker {
        GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 1,
            observed_sequence: 3,
            missing_from: 2,
            missing_to: 2,
            detected_at: TimestampMilliseconds(detected_at),
        }
    }

    pub(super) fn retention_config(
        intervals: Vec<AlignedWindow>,
        caps: &[(AlignedWindow, usize)],
        allowed_lateness: DurationMilliseconds,
        correction_cutoff: DurationMilliseconds,
    ) -> ProcessorConfig {
        ProcessorConfig::new(
            intervals,
            allowed_lateness,
            caps.iter().copied().collect::<BTreeMap<_, _>>(),
            correction_cutoff,
        )
    }
}
