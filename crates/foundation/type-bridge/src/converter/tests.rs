use super::*;
use crate::{BinanceConverter, MinMaxStep};
use rust_decimal::RoundingStrategy;
use std::collections::HashMap;
use stock_trek_types::cex::{
    activation::Activation, preferences::CexRoundingPreferences, quantity::Quantity, side::Side,
    tag::Tag, time_in_force::TimeInForce,
};

fn precision() -> TradingPrecision {
    let limits = MinMaxStep {
        min: Decimal::ZERO,
        max: Decimal::from(100_000),
        step: Decimal::new(1, 2),
    };
    TradingPrecision::new(limits, limits, limits)
}

fn preferences() -> CexPreferences {
    CexPreferences {
        max_network_delay_millis: 0,
        rounding: CexRoundingPreferences {
            activation_price_triggered_above: RoundingStrategy::MidpointNearestEven,
            activation_price_triggered_below: RoundingStrategy::MidpointNearestEven,
            price: RoundingStrategy::MidpointNearestEven,
            quantity: RoundingStrategy::MidpointNearestEven,
            callback_rate_bps: RoundingStrategy::MidpointNearestEven,
        },
    }
}

fn order_request() -> OrderRequest<AssetId, f64> {
    OrderRequest::Limit {
        base: AssetId::Bitcoin,
        quote: AssetId::TetherUSD,
        side: Side::Buy,
        activation: Activation::Immediate,
        limit_price: 100.123,
        time_in_force: TimeInForce::GoodTillCancelled,
        quantity: Quantity::OfBase(1.234),
        tag: Tag::new("test"),
    }
}

fn exchange_info(include_pair: bool) -> TBExchangeInfo {
    let mut trading_precisions = HashMap::new();
    if include_pair {
        trading_precisions.insert(
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            precision(),
        );
    }
    TBExchangeInfo { trading_precisions }
}

#[test]
fn trading_converter_uses_the_precision_for_the_order_pair() {
    let converter = BinanceConverter::default().into_trading_converter(exchange_info(true));

    let request = converter
        .order_request(order_request(), &preferences())
        .unwrap();

    assert_eq!(request.symbol, "BTCUSDT");
    assert_eq!(request.price, Some(Decimal::new(10012, 2)));
    assert_eq!(request.quantity, Some(Decimal::new(123, 2)));
}

#[test]
fn trading_converter_rejects_pairs_without_precision() {
    let converter = BinanceConverter::default().into_trading_converter(exchange_info(false));

    let result = converter.order_request(order_request(), &preferences());

    assert!(matches!(
        result,
        Err(BpError::Validation(ValidationError::MissingValue(_)))
    ));
}
