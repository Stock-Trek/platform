use crate::cex::order_status::OrderStatus;
use rust_decimal::Decimal;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct OrderResponse {
    pub client_order_id: String,
    pub filled_base_quantity: Decimal,
    pub filled_quote_quantity: Decimal,
    pub order_id: String,
    pub status: OrderStatus,
}
