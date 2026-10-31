use crate::cex::{
    activation::Activation, quantity::Quantity, side::Side, tag::Tag, time_in_force::TimeInForce,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use strum::Display;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Display, Clone, Hash)]
pub enum OrderRequest<Asset, Number> {
    Limit {
        base: Asset,
        quote: Asset,
        side: Side,
        activation: Activation<Number>,
        limit_price: Number,
        time_in_force: TimeInForce,
        quantity: Quantity<Number>,
        tag: Tag,
    },
    MarketBuy {
        base: Asset,
        quote: Asset,
        activation: Activation<Number>,
        quote_quantity: Number,
        tag: Tag,
    },
    MarketSell {
        base: Asset,
        quote: Asset,
        activation: Activation<Number>,
        base_quantity: Number,
        tag: Tag,
    },
}
