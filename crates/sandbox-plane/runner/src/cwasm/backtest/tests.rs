use platform_runner::command::SimulationParameters;
use rand::{SeedableRng, rngs::StdRng};
use serde_json::Value;
use std::collections::BTreeMap;
use stock_trek::{
    actions::ResolvedAction,
    types::{AccountId, AssetId, CexId, OrderRequest, Quantity, Side},
};

use super::orders::OrderProcessor;
use super::portfolio::Portfolio;
use super::{Balances, PriceMap};

fn balances() -> Balances {
    BTreeMap::from([(
        CexId::Binance,
        BTreeMap::from([(
            AccountId::new("main"),
            BTreeMap::from([(AssetId::TetherUSD, 1000.0), (AssetId::Bitcoin, 2.0)]),
        )]),
    )])
}

fn prices() -> PriceMap {
    BTreeMap::from([((CexId::Binance, AssetId::Bitcoin, AssetId::TetherUSD), 50.0)])
}

fn parameters() -> SimulationParameters {
    SimulationParameters::default()
}

fn market_buy(quote_quantity: f64) -> OrderRequest<AssetId, f64> {
    OrderRequest::MarketBuy {
        base: AssetId::Bitcoin,
        quote: AssetId::TetherUSD,
        activation: stock_trek::types::Activation::Immediate,
        quote_quantity,
        tag: stock_trek::types::Tag::new("test"),
    }
}

#[test]
fn market_buy_fills_from_quote_balance() {
    let mut balances = balances();
    let action = ResolvedAction::PlaceOrder {
        cex_id: CexId::Binance,
        account_id: AccountId::new("main"),
        order_request: market_buy(100.0),
    };
    let event = OrderProcessor::handle_action(
        &action,
        &mut balances,
        &prices(),
        &mut StdRng::seed_from_u64(1),
        &parameters(),
    )
    .unwrap();
    assert_eq!(
        event.get("type").and_then(Value::as_str),
        Some("place_order")
    );
    assert_eq!(
        event.get("partialFill").and_then(Value::as_bool),
        Some(false)
    );
    let assets = &balances[&CexId::Binance][&AccountId::new("main")];
    assert_eq!(assets[&AssetId::TetherUSD], 900.0);
    assert_eq!(assets[&AssetId::Bitcoin], 2.0 + 2.0);
}

#[test]
fn market_buy_partial_fills_when_quote_is_short() {
    let mut balances = balances();
    let accounts = balances.get_mut(&CexId::Binance).unwrap();
    let assets = accounts.get_mut(&AccountId::new("main")).unwrap();
    assets.insert(AssetId::TetherUSD, 10.0);
    let action = ResolvedAction::PlaceOrder {
        cex_id: CexId::Binance,
        account_id: AccountId::new("main"),
        order_request: market_buy(100.0),
    };
    let event = OrderProcessor::handle_action(
        &action,
        &mut balances,
        &prices(),
        &mut StdRng::seed_from_u64(1),
        &parameters(),
    )
    .unwrap();
    assert_eq!(
        event.get("partialFill").and_then(Value::as_bool),
        Some(true)
    );
    let assets = &balances[&CexId::Binance][&AccountId::new("main")];
    assert_eq!(assets.get(&AssetId::TetherUSD).copied().unwrap_or(0.0), 0.0);
    assert_eq!(assets[&AssetId::Bitcoin], 2.2);
}

#[test]
fn limit_buy_fills_only_when_crossed() {
    let mut balances = balances();
    let action = ResolvedAction::PlaceOrder {
        cex_id: CexId::Binance,
        account_id: AccountId::new("main"),
        order_request: OrderRequest::Limit {
            base: AssetId::Bitcoin,
            quote: AssetId::TetherUSD,
            side: Side::Buy,
            activation: stock_trek::types::Activation::Immediate,
            limit_price: 10.0,
            time_in_force: stock_trek::types::TimeInForce::GoodTillCancelled,
            quantity: Quantity::OfBase(1.0),
            tag: stock_trek::types::Tag::new("test"),
        },
    };
    let event = OrderProcessor::handle_action(
        &action,
        &mut balances,
        &prices(),
        &mut StdRng::seed_from_u64(1),
        &parameters(),
    )
    .unwrap();
    assert_eq!(
        event.get("type").and_then(Value::as_str),
        Some("order_not_filled")
    );
    let assets = &balances[&CexId::Binance][&AccountId::new("main")];
    assert_eq!(assets[&AssetId::TetherUSD], 1000.0);
}

#[test]
fn lost_order_changes_nothing() {
    let mut balances = balances();
    let action = ResolvedAction::PlaceOrder {
        cex_id: CexId::Binance,
        account_id: AccountId::new("main"),
        order_request: market_buy(100.0),
    };
    let mut parameters = parameters();
    parameters.lost_order_chance = 1.0;
    let event = OrderProcessor::handle_action(
        &action,
        &mut balances,
        &prices(),
        &mut StdRng::seed_from_u64(1),
        &parameters,
    )
    .unwrap();
    assert_eq!(
        event.get("type").and_then(Value::as_str),
        Some("order_lost")
    );
    let assets = &balances[&CexId::Binance][&AccountId::new("main")];
    assert_eq!(assets[&AssetId::TetherUSD], 1000.0);
}

#[test]
fn cancel_orders_event_is_recorded() {
    let mut balances = balances();
    let action = ResolvedAction::CancelAllOrders {
        tag: Some(stock_trek::types::Tag::new("test")),
    };
    let event = OrderProcessor::handle_action(
        &action,
        &mut balances,
        &prices(),
        &mut StdRng::seed_from_u64(1),
        &parameters(),
    )
    .unwrap();
    assert_eq!(
        event.get("type").and_then(Value::as_str),
        Some("cancel_orders")
    );
}

#[test]
fn asset_totals_serialize_cex_accounts_and_assets() {
    let totals = Portfolio::asset_totals(&balances());
    assert_eq!(
        totals["binance"]["main"]["tether_u_s_d"],
        serde_json::json!(1000.0)
    );
}
