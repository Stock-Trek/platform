use super::{asset_balance_wire::AssetBalanceWire, pending_order_wire::PendingOrderWire};
use crate::{
    AccountKey, AccountPortfolio, PORTFOLIO_UPDATE_SCHEMA_VERSION, PendingOrder, PortfolioUpdate,
};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use stock_trek_types::cex::{
    asset_id::AssetId,
    order_request::OrderRequest,
    tag::Tag,
};

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct PortfolioUpdateWire {
    pub(super) schema_version: u32,
    pub(super) account: AccountKey,
    pub(super) owner: Option<String>,
    pub(super) version: u64,
    pub(super) occurred_at: DateTime<Utc>,
    pub(super) exchange_time: Option<DateTime<Utc>>,
    pub(super) balances: Vec<AssetBalanceWire>,
    pub(super) pending_orders: Vec<PendingOrderWire>,
}

impl From<&PortfolioUpdate> for PortfolioUpdateWire {
    fn from(update: &PortfolioUpdate) -> Self {
        Self {
            schema_version: PORTFOLIO_UPDATE_SCHEMA_VERSION,
            account: update.account().clone(),
            owner: update.owner().map(str::to_string),
            version: update.version(),
            occurred_at: update.occurred_at(),
            exchange_time: update.exchange_time(),
            balances: update
                .portfolio()
                .asset_totals()
                .map(|(asset, total)| AssetBalanceWire {
                    asset: *asset,
                    total: *total,
                })
                .collect(),
            pending_orders: update
                .portfolio()
                .pending_orders_iter()
                .map(|(_, order)| PendingOrderWire {
                    request: order.order_request().clone(),
                    filled_base_quantity: order.filled_base_quantity(),
                    filled_quote_quantity: order.filled_quote_quantity(),
                })
                .collect(),
        }
    }
}

impl TryFrom<PortfolioUpdateWire> for PortfolioUpdate {
    type Error = String;

    fn try_from(wire: PortfolioUpdateWire) -> Result<Self, Self::Error> {
        if wire.schema_version != PORTFOLIO_UPDATE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported portfolio update schema version {}",
                wire.schema_version
            ));
        }
        let balances = wire
            .balances
            .into_iter()
            .map(|balance| (balance.asset, balance.total));
        let mut orders = BTreeMap::<Tag, Vec<PendingOrder>>::new();
        for order in wire.pending_orders {
            let tag = PortfolioUpdateWire::tag(&order.request).clone();
            let order = PendingOrder::new(
                order.request,
                order.filled_base_quantity,
                order.filled_quote_quantity,
            )
            .map_err(|error| error.to_string())?;
            orders.entry(tag).or_default().push(order);
        }
        let portfolio =
            AccountPortfolio::new(balances, orders).map_err(|error| error.to_string())?;
        Ok(PortfolioUpdate::versioned(
            wire.account,
            wire.owner,
            portfolio,
            wire.version,
            wire.occurred_at,
        )
        .with_optional_exchange_time(wire.exchange_time))
    }
}

impl PortfolioUpdateWire {
    fn tag(request: &OrderRequest<AssetId, f64>) -> &Tag {
        match request {
            OrderRequest::Limit { tag, .. }
            | OrderRequest::MarketBuy { tag, .. }
            | OrderRequest::MarketSell { tag, .. } => tag,
        }
    }
}
