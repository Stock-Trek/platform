use crate::SimulationParameters;
use parking_lot::Mutex;
use rand::{SeedableRng, rngs::StdRng};
use serde_json::Value;
use std::collections::BTreeMap;
use stock_trek::{
    actions::ResolvedAction,
    types::{AccountId, Activation, AssetId, CexId, OrderRequest, Quantity, Side, Tag},
};

use crate::cwasm::backtest::orders::OrderProcessor;
use crate::cwasm::backtest::portfolio::Portfolio;
use crate::cwasm::backtest::{Balances, PriceMap};

struct Fixture;

impl Fixture {
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

    fn rng() -> StdRng {
        StdRng::seed_from_u64(1)
    }

    fn market_buy(quote_quantity: f64) -> OrderRequest<AssetId, f64> {
        OrderRequest::MarketBuy {
            base: AssetId::Bitcoin,
            quote: AssetId::TetherUSD,
            activation: Activation::Immediate,
            quote_quantity,
            tag: Tag::new("test"),
        }
    }

    fn buy_action(quote_quantity: f64) -> ResolvedAction {
        ResolvedAction::PlaceOrder {
            cex_id: CexId::Binance,
            account_id: AccountId::new("main"),
            order_request: Self::market_buy(quote_quantity),
        }
    }

    fn assets(balances: &Balances) -> &BTreeMap<AssetId, f64> {
        &balances[&CexId::Binance][&AccountId::new("main")]
    }
}

#[test]
fn market_buy_fills_from_quote_balance() {
    let mut balances = Fixture::balances();
    let event = OrderProcessor::handle_action(
        &Fixture::buy_action(100.0),
        &mut balances,
        &Fixture::prices(),
        &mut Fixture::rng(),
        &Fixture::parameters(),
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
    let assets = Fixture::assets(&balances);
    assert_eq!(assets[&AssetId::TetherUSD], 900.0);
    assert_eq!(assets[&AssetId::Bitcoin], 4.0);
}

#[test]
fn market_buy_partial_fills_when_quote_is_short() {
    let mut balances = Fixture::balances();
    balances
        .get_mut(&CexId::Binance)
        .unwrap()
        .get_mut(&AccountId::new("main"))
        .unwrap()
        .insert(AssetId::TetherUSD, 10.0);
    let event = OrderProcessor::handle_action(
        &Fixture::buy_action(100.0),
        &mut balances,
        &Fixture::prices(),
        &mut Fixture::rng(),
        &Fixture::parameters(),
    )
    .unwrap();
    assert_eq!(
        event.get("partialFill").and_then(Value::as_bool),
        Some(true)
    );
    let assets = Fixture::assets(&balances);
    assert_eq!(assets.get(&AssetId::TetherUSD).copied().unwrap_or(0.0), 0.0);
    assert_eq!(assets[&AssetId::Bitcoin], 2.2);
}

#[test]
fn limit_buy_fills_only_when_crossed() {
    let mut balances = Fixture::balances();
    let action = ResolvedAction::PlaceOrder {
        cex_id: CexId::Binance,
        account_id: AccountId::new("main"),
        order_request: OrderRequest::Limit {
            base: AssetId::Bitcoin,
            quote: AssetId::TetherUSD,
            side: Side::Buy,
            activation: Activation::Immediate,
            limit_price: 10.0,
            time_in_force: stock_trek::types::TimeInForce::GoodTillCancelled,
            quantity: Quantity::OfBase(1.0),
            tag: Tag::new("test"),
        },
    };
    let event = OrderProcessor::handle_action(
        &action,
        &mut balances,
        &Fixture::prices(),
        &mut Fixture::rng(),
        &Fixture::parameters(),
    )
    .unwrap();
    assert_eq!(
        event.get("type").and_then(Value::as_str),
        Some("order_not_filled")
    );
    assert_eq!(Fixture::assets(&balances)[&AssetId::TetherUSD], 1000.0);
}

#[test]
fn lost_order_changes_nothing() {
    let mut balances = Fixture::balances();
    let mut parameters = Fixture::parameters();
    parameters.lost_order_chance = 1.0;
    let event = OrderProcessor::handle_action(
        &Fixture::buy_action(100.0),
        &mut balances,
        &Fixture::prices(),
        &mut Fixture::rng(),
        &parameters,
    )
    .unwrap();
    assert_eq!(
        event.get("type").and_then(Value::as_str),
        Some("order_lost")
    );
    assert_eq!(Fixture::assets(&balances)[&AssetId::TetherUSD], 1000.0);
}

#[test]
fn cancel_orders_event_is_recorded() {
    let mut balances = Fixture::balances();
    let action = ResolvedAction::CancelAllOrders {
        tag: Some(Tag::new("test")),
    };
    let event = OrderProcessor::handle_action(
        &action,
        &mut balances,
        &Fixture::prices(),
        &mut Fixture::rng(),
        &Fixture::parameters(),
    )
    .unwrap();
    assert_eq!(
        event.get("type").and_then(Value::as_str),
        Some("cancel_orders")
    );
}

#[test]
fn asset_totals_serialize_cex_accounts_and_assets() {
    let totals = Portfolio::asset_totals(&Fixture::balances());
    assert_eq!(
        totals[CexId::Binance.to_string()]["main"][AssetId::TetherUSD.to_string()],
        serde_json::json!(1000.0)
    );
}
