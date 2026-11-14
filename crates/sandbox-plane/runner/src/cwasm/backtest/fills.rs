use platform_runner::SimulationParameters;
use rand::rngs::StdRng;
use serde_json::{Value, json};
use stock_trek::types::{AccountId, AssetId, CexId, OrderRequest, Quantity, Side};

use crate::cwasm::backtest::{
    Balances, PlatformResult, PriceMap, order_events::OrderEvents, orders::OrderProcessor,
};

impl OrderProcessor {
    pub(super) fn place_order(
        cex_id: &CexId,
        account_id: &AccountId,
        order_request: &OrderRequest<AssetId, f64>,
        balances: &mut Balances,
        prices: &PriceMap,
        rng: &mut StdRng,
        parameters: &SimulationParameters,
    ) -> PlatformResult<Value> {
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
}
