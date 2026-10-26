use crate::{BpResult, trading_precision::TradingPrecision};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use stock_trek_types::cex::{
    activation::Activation, asset_id::AssetId, order_request::OrderRequest,
    preferences::CexRoundingPreferences, quantity::Quantity, trigger_direction::TriggerDirection,
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct FloatsToDecimals {
    trading_precision: TradingPrecision,
    rounding: CexRoundingPreferences,
}

impl FloatsToDecimals {
    pub fn new(trading_precision: TradingPrecision, rounding: CexRoundingPreferences) -> Self {
        Self {
            trading_precision,
            rounding,
        }
    }
    pub fn floats_to_decimals(
        &self,
        order_request: OrderRequest<AssetId, f64>,
    ) -> BpResult<OrderRequest<AssetId, Decimal>> {
        match order_request {
            OrderRequest::Limit {
                activation,
                base,
                limit_price,
                quantity,
                quote,
                side,
                tag,
                time_in_force,
            } => Ok(OrderRequest::Limit {
                base,
                quote,
                side,
                activation: self.activation(activation)?,
                limit_price: self.price(limit_price)?,
                time_in_force,
                quantity: self.quantity(quantity, true)?,
                tag,
            }),
            OrderRequest::MarketBuy {
                base,
                quote,
                activation,
                quote_quantity,
                tag,
            } => Ok(OrderRequest::MarketBuy {
                base,
                quote,
                activation: self.activation(activation)?,
                quote_quantity: self.quote_quantity(quote_quantity)?,
                tag,
            }),
            OrderRequest::MarketSell {
                base,
                quote,
                activation,
                base_quantity,
                tag,
            } => Ok(OrderRequest::MarketSell {
                base,
                quote,
                activation: self.activation(activation)?,
                base_quantity: self.base_quantity(base_quantity, false)?,
                tag,
            }),
        }
    }

    fn activation(&self, activation: Activation<f64>) -> BpResult<Activation<Decimal>> {
        match activation {
            Activation::Immediate => Ok(Activation::Immediate),
            Activation::PriceTriggered {
                activation_price,
                basis,
                direction,
                mode,
            } => Ok(Activation::PriceTriggered {
                activation_price: self.trading_precision.to_valid_quote(
                    activation_price,
                    self.activation_price_rounding_strategy(direction),
                )?,
                basis,
                direction,
                mode,
            }),
            Activation::Trailing {
                activation_price,
                basis,
                callback_rate_bps,
                direction,
            } => Ok(Activation::Trailing {
                activation_price: self.trading_precision.to_valid_quote(
                    activation_price,
                    self.activation_price_rounding_strategy(direction),
                )?,
                basis,
                callback_rate_bps: self.callback_rate_bps(callback_rate_bps),
                direction,
            }),
        }
    }
    fn base_quantity(&self, quantity: f64, is_limit: bool) -> BpResult<Decimal> {
        if is_limit {
            self.trading_precision
                .to_valid_base_limit(quantity, self.rounding.quantity)
        } else {
            self.trading_precision
                .to_valid_base_market(quantity, self.rounding.quantity)
        }
    }
    fn quote_quantity(&self, quantity: f64) -> BpResult<Decimal> {
        self.trading_precision
            .to_valid_quote(quantity, self.rounding.quantity)
    }
    fn price(&self, price: f64) -> BpResult<Decimal> {
        self.trading_precision
            .to_valid_quote(price, self.rounding.price)
    }
    fn callback_rate_bps(&self, callback_rate_bps: u32) -> u32 {
        Decimal::from(callback_rate_bps)
            .round_dp_with_strategy(0, self.rounding.callback_rate_bps)
            .to_u32()
            .unwrap_or(callback_rate_bps)
    }
    fn quantity(&self, quantity: Quantity<f64>, is_limit: bool) -> BpResult<Quantity<Decimal>> {
        match quantity {
            Quantity::OfBase(q) => Ok(Quantity::OfBase(self.base_quantity(q, is_limit)?)),
            Quantity::OfQuote(q) => Ok(Quantity::OfQuote(self.quote_quantity(q)?)),
        }
    }
    fn activation_price_rounding_strategy(&self, direction: TriggerDirection) -> RoundingStrategy {
        match direction {
            TriggerDirection::Above => self.rounding.activation_price_triggered_above,
            TriggerDirection::Below => self.rounding.activation_price_triggered_below,
        }
    }
}
