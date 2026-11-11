use parking_lot::Mutex;
use platform_runner::command::SimulationParameters;
use rand::{SeedableRng, rngs::StdRng};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::Duration,
};
use stock_trek::{
    actions::ResolvedAction,
    types::{AccountId, AssetId, CexId, OrderRequest, Quantity, Side},
};

use super::orders::OrderProcessor;
use super::portfolio::Portfolio;
use super::{BackTest, Balances, PriceMap};

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

#[test]
fn simulations_respect_the_concurrency_limit() {
    let active = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    let seen = Mutex::new(Vec::new());
    let results = BackTest::run_simulations(12, 3, |index| {
        let running = active.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(running, Ordering::SeqCst);
        seen.lock().push(index);
        thread::sleep(Duration::from_millis(2));
        active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    });
    assert_eq!(results.len(), 12);
    assert!(results.iter().all(Result::is_ok));
    assert!(peak.load(Ordering::SeqCst) <= 3);
    let mut seen = seen.into_inner();
    seen.sort_unstable();
    assert_eq!(seen, (0..12).collect::<Vec<_>>());
}

#[test]
fn a_panicking_simulation_is_reported_without_stopping_the_others() {
    let results = BackTest::run_simulations(4, 2, |index| {
        if index == 2 {
            panic!("simulation failed");
        }
        Ok(())
    });
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert_eq!(
        results[2],
        Err(platform_exit_codes::ExitCode::BotErrorPanic.code())
    );
}
