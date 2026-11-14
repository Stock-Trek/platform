use crate::SimulationParameters;
use rand::{Rng, rngs::StdRng};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use stock_trek::{
    actions::ResolvedAction,
    types::{AccountId, AssetId, CexId, Side},
};

use crate::cwasm::backtest::{
    BacktestError, Balances, PlatformResult, PriceMap, order_events::OrderEvents,
};

pub struct OrderProcessor;

impl OrderProcessor {
    pub fn handle_action(
        action: &ResolvedAction,
        balances: &mut Balances,
        prices: &PriceMap,
        rng: &mut StdRng,
        parameters: &SimulationParameters,
    ) -> PlatformResult<Value> {
        match action {
            ResolvedAction::PlaceOrder {
                cex_id,
                account_id,
                order_request,
            } => {
                if rng.gen_range(0.0..1.0) < parameters.lost_order_chance {
                    return Ok(json!({
                        "type": "order_lost",
                        "cex": cex_id.to_string(),
                        "account": account_id.0,
                        "orderRequest": OrderEvents::request_value(order_request),
                    }));
                }
                Self::place_order(
                    cex_id,
                    account_id,
                    order_request,
                    balances,
                    prices,
                    rng,
                    parameters,
                )
            }
            ResolvedAction::CancelAllOrders { tag } => Ok(OrderEvents::cancelled(None, None, tag)),
            ResolvedAction::CancelAllOrdersInCex { cex_id, tag } => {
                Ok(OrderEvents::cancelled(Some(cex_id), None, tag))
            }
            ResolvedAction::CancelAllOrdersInCexAccount {
                cex_id,
                account_id,
                tag,
            } => Ok(OrderEvents::cancelled(Some(cex_id), Some(account_id), tag)),
            _ => Err(BacktestError::UnsupportedAction.into()),
        }
    }

    pub(super) fn account_balances<'a>(
        balances: &'a mut Balances,
        cex_id: CexId,
        account_id: &AccountId,
    ) -> &'a mut BTreeMap<AssetId, f64> {
        balances
            .entry(cex_id)
            .or_default()
            .entry(account_id.clone())
            .or_default()
    }

    pub(super) fn add_balance(assets: &mut BTreeMap<AssetId, f64>, asset: AssetId, delta: f64) {
        let balance = assets.entry(asset).or_insert(0.0);
        *balance += delta;
        if *balance == 0.0 {
            assets.remove(&asset);
        }
    }

    pub(super) fn slipped_price(
        prices: &PriceMap,
        cex_id: CexId,
        base: &AssetId,
        quote: &AssetId,
        side: Option<&Side>,
        rng: &mut StdRng,
        parameters: &SimulationParameters,
    ) -> PlatformResult<(f64, bool)> {
        let market_price =
            *prices
                .get(&(cex_id, *base, *quote))
                .ok_or(BacktestError::MissingMarket {
                    cex: cex_id,
                    base: *base,
                    quote: *quote,
                })?;
        if rng.gen_range(0.0..1.0) < parameters.slippage_chance {
            let slipped = match side {
                Some(&Side::Buy) => market_price * (1.0 + parameters.slippage_proportion),
                Some(&Side::Sell) => market_price * (1.0 - parameters.slippage_proportion),
                None => market_price,
            };
            return Ok((slipped, true));
        }
        Ok((market_price, false))
    }
}
