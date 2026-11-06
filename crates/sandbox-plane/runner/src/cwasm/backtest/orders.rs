use platform_exit_codes::ExitCode;
use platform_runner::command::SimulationParameters;
use rand::{Rng, rngs::StdRng};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use stock_trek::{
    actions::ResolvedAction,
    types::{AccountId, AssetId, CexId, OrderRequest, Quantity, Side},
};

use super::{Balances, PriceMap, order_events::OrderEvents};

pub(super) struct OrderProcessor;

impl OrderProcessor {
    pub(super) fn handle_action(
        action: &ResolvedAction,
        balances: &mut Balances,
        prices: &PriceMap,
        rng: &mut StdRng,
        parameters: &SimulationParameters,
    ) -> Result<Value, i32> {
        match action {
            ResolvedAction::PlaceOrder {
                cex_id,
                account_id,
                order_request,
            } => Self::place_order(
                cex_id,
                account_id,
                order_request,
                balances,
                prices,
                rng,
                parameters,
            ),
            ResolvedAction::CancelAllOrders { tag } => Ok(OrderEvents::cancelled(None, None, tag)),
            ResolvedAction::CancelAllOrdersInCex { cex_id, tag } => {
                Ok(OrderEvents::cancelled(Some(cex_id), None, tag))
            }
            ResolvedAction::CancelAllOrdersInCexAccount {
                cex_id,
                account_id,
                tag,
            } => Ok(OrderEvents::cancelled(Some(cex_id), Some(account_id), tag)),
            _ => {
                eprintln!("cwasm-invoker: unsupported action in back test");
                Err(ExitCode::BotErrorResolvingAction.code())
            }
        }
    }

    fn place_order(
        cex_id: &CexId,
        account_id: &AccountId,
        order_request: &OrderRequest<AssetId, f64>,
        balances: &mut Balances,
        prices: &PriceMap,
        rng: &mut StdRng,
        parameters: &SimulationParameters,
    ) -> Result<Value, i32> {
        if rng.gen_range(0.0..1.0) < parameters.lost_order_chance {
            return Ok(json!({
                "type": "order_lost",
                "cex": cex_id.to_string(),
                "account": account_id.0,
                "orderRequest": OrderEvents::request_value(order_request),
            }));
        }
        let assets = Self::account_balances(balances, *cex_id, account_id);
        match order_request {
            OrderRequest::MarketBuy {
                base,
                quote,
                quote_quantity,
                ..
            } => {
                let (price, slipped) = Self::slipped_price(
                    prices,
                    *cex_id,
                    base,
                    quote,
                    Some(&Side::Buy),
                    rng,
                    parameters,
                )?;
                let available = assets.get(quote).copied().unwrap_or(0.0);
                let spent = available.min(*quote_quantity);
                let received = spent / price;
                Self::add_balance(assets, *quote, -spent);
                Self::add_balance(assets, *base, received);
                Ok(OrderEvents::placed(
                    "place_order",
                    cex_id,
                    account_id,
                    order_request,
                    price,
                    received,
                    spent,
                    spent < *quote_quantity,
                    slipped,
                ))
            }
            OrderRequest::MarketSell {
                base,
                quote,
                base_quantity,
                ..
            } => {
                let (price, slipped) = Self::slipped_price(
                    prices,
                    *cex_id,
                    base,
                    quote,
                    Some(&Side::Sell),
                    rng,
                    parameters,
                )?;
                let available = assets.get(base).copied().unwrap_or(0.0);
                let sold = available.min(*base_quantity);
                let received = sold * price;
                Self::add_balance(assets, *base, -sold);
                Self::add_balance(assets, *quote, received);
                Ok(OrderEvents::placed(
                    "place_order",
                    cex_id,
                    account_id,
                    order_request,
                    price,
                    sold,
                    received,
                    sold < *base_quantity,
                    slipped,
                ))
            }
            OrderRequest::Limit {
                base,
                quote,
                side,
                limit_price,
                quantity,
                ..
            } => {
                let (price, slipped) =
                    Self::slipped_price(prices, *cex_id, base, quote, Some(side), rng, parameters)?;
                let crossed = match side {
                    Side::Buy => price <= *limit_price,
                    Side::Sell => price >= *limit_price,
                };
                if !crossed {
                    return Ok(json!({
                        "type": "order_not_filled",
                        "cex": cex_id.to_string(),
                        "account": account_id.0,
                        "orderRequest": OrderEvents::request_value(order_request),
                    }));
                }
                match quantity {
                    Quantity::OfBase(requested) => match side {
                        Side::Buy => {
                            let available = assets.get(quote).copied().unwrap_or(0.0);
                            let received = (available / price).min(*requested);
                            let spent = received * price;
                            Self::add_balance(assets, *quote, -spent);
                            Self::add_balance(assets, *base, received);
                            Ok(OrderEvents::placed(
                                "place_order",
                                cex_id,
                                account_id,
                                order_request,
                                price,
                                received,
                                spent,
                                received < *requested,
                                slipped,
                            ))
                        }
                        Side::Sell => {
                            let available = assets.get(base).copied().unwrap_or(0.0);
                            let sold = available.min(*requested);
                            let received = sold * price;
                            Self::add_balance(assets, *base, -sold);
                            Self::add_balance(assets, *quote, received);
                            Ok(OrderEvents::placed(
                                "place_order",
                                cex_id,
                                account_id,
                                order_request,
                                price,
                                sold,
                                received,
                                sold < *requested,
                                slipped,
                            ))
                        }
                    },
                    Quantity::OfQuote(requested) => match side {
                        Side::Buy => {
                            let available = assets.get(quote).copied().unwrap_or(0.0);
                            let spent = available.min(*requested);
                            let received = spent / price;
                            Self::add_balance(assets, *quote, -spent);
                            Self::add_balance(assets, *base, received);
                            Ok(OrderEvents::placed(
                                "place_order",
                                cex_id,
                                account_id,
                                order_request,
                                price,
                                received,
                                spent,
                                spent < *requested,
                                slipped,
                            ))
                        }
                        Side::Sell => {
                            let available = assets.get(base).copied().unwrap_or(0.0);
                            let received = (available * price).min(*requested);
                            let sold = received / price;
                            Self::add_balance(assets, *base, -sold);
                            Self::add_balance(assets, *quote, received);
                            Ok(OrderEvents::placed(
                                "place_order",
                                cex_id,
                                account_id,
                                order_request,
                                price,
                                sold,
                                received,
                                received < *requested,
                                slipped,
                            ))
                        }
                    },
                }
            }
        }
    }

    fn account_balances<'a>(
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

    fn add_balance(assets: &mut BTreeMap<AssetId, f64>, asset: AssetId, delta: f64) {
        let balance = assets.entry(asset).or_insert(0.0);
        *balance += delta;
        if *balance == 0.0 {
            assets.remove(&asset);
        }
    }

    fn slipped_price(
        prices: &PriceMap,
        cex_id: CexId,
        base: &AssetId,
        quote: &AssetId,
        side: Option<&Side>,
        rng: &mut StdRng,
        parameters: &SimulationParameters,
    ) -> Result<(f64, bool), i32> {
        let market_price = *prices.get(&(cex_id, *base, *quote)).ok_or_else(|| {
            eprintln!("cwasm-invoker: no market data for an order on {cex_id} {base}/{quote}");
            ExitCode::BotErrorResolvingAction.code()
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
