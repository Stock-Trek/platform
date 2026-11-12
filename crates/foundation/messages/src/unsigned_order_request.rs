use serde::{Deserialize, Serialize};
use stock_trek_types::cex::{
    account_id::AccountId, asset_id::AssetId, cex_id::CexId, order_request::OrderRequest,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsignedOrderRequest {
    cex: CexId,
    account: AccountId,
    owner: String,
    order_request: OrderRequest<AssetId, f64>,
}

impl UnsignedOrderRequest {
    pub fn new(
        cex: CexId,
        account: AccountId,
        owner: impl Into<String>,
        order_request: OrderRequest<AssetId, f64>,
    ) -> Self {
        Self {
            cex,
            account,
            owner: owner.into().trim().to_string(),
            order_request,
        }
    }

    pub fn cex(&self) -> CexId {
        self.cex
    }

    pub fn account(&self) -> &AccountId {
        &self.account
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn order_request(&self) -> &OrderRequest<AssetId, f64> {
        &self.order_request
    }

    pub fn order_request_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&self.order_request)
    }

    pub fn canonical_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into().trim().to_string();
        self
    }
}
