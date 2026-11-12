use platform_auth_service::SignedOrderRequest;
use platform_errors::{BuildError, BuildResult};
use platform_messages::UnsignedOrderRequest;
use serde::Serialize;
use stock_trek_types::cex::{
    account_id::AccountId, asset_id::AssetId, cex_id::CexId, order_request::OrderRequest,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedOrderSubmission {
    cex: CexId,
    account: AccountId,
    owner: String,
    token_id: String,
    order_request: OrderRequest<AssetId, f64>,
    signature: String,
}

impl SignedOrderSubmission {
    pub fn new(request: &UnsignedOrderRequest, signed: &SignedOrderRequest) -> Self {
        Self {
            cex: request.cex(),
            account: request.account().clone(),
            owner: request.owner().trim().to_string(),
            token_id: signed.token_id().to_string(),
            order_request: request.order_request().clone(),
            signature: signed.envelope().to_string(),
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

    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn order_request(&self) -> &OrderRequest<AssetId, f64> {
        &self.order_request
    }

    pub fn signature(&self) -> &str {
        &self.signature
    }

    pub fn canonical_bytes(&self) -> BuildResult<Vec<u8>> {
        UnsignedOrderRequest::new(
            self.cex,
            self.account.clone(),
            self.owner.clone(),
            self.order_request.clone(),
        )
        .canonical_bytes()
        .map_err(BuildError::external)
    }
}
