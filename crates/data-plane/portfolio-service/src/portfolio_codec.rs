use crate::{
    AccountKey, AccountPortfolio, PORTFOLIO_UPDATE_SCHEMA_VERSION, PendingOrder, PortfolioUpdate,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};
use std::collections::BTreeMap;
use stock_trek_types::cex::{asset_id::AssetId, order_request::OrderRequest, tag::Tag};

#[derive(Serialize, Deserialize)]
struct PortfolioUpdateWire {
    schema_version: u32,
    account: AccountKey,
    owner: Option<String>,
    version: u64,
    occurred_at: DateTime<Utc>,
    exchange_time: Option<DateTime<Utc>>,
    balances: Vec<AssetBalanceWire>,
    pending_orders: Vec<PendingOrderWire>,
}

#[derive(Serialize, Deserialize)]
struct AssetBalanceWire {
    asset: AssetId,
    total: f64,
}

#[derive(Serialize, Deserialize)]
struct PendingOrderWire {
    request: OrderRequest<AssetId, f64>,
    filled_base_quantity: f64,
    filled_quote_quantity: f64,
}

impl Serialize for PortfolioUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        PortfolioUpdateWire::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PortfolioUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PortfolioUpdateWire::deserialize(deserializer)?;
        wire.try_into().map_err(D::Error::custom)
    }
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

impl PortfolioUpdate {
    pub fn to_bytes(&self) -> BpResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(payload: &[u8]) -> BpResult<Self> {
        serde_json::from_slice(payload).map_err(BpError::external)
    }

    pub(crate) fn with_optional_exchange_time(
        mut self,
        exchange_time: Option<DateTime<Utc>>,
    ) -> Self {
        if let Some(exchange_time) = exchange_time {
            self = self.with_exchange_time(exchange_time);
        }
        self
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
