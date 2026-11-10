use chrono::Utc;
use platform_auth_service::{AuthService, SignedOrderRequest};
use platform_errors::{BpError, BpResult};
use platform_messages::order_request::UnsignedOrderRequest;
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
            owner: request.owner().to_string(),
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
}

pub struct OrderRequestService {
    auth_service: AuthService,
}

impl OrderRequestService {
    pub fn new(auth_service: AuthService) -> Self {
        Self { auth_service }
    }

    pub async fn sign(&self, request: &UnsignedOrderRequest) -> BpResult<SignedOrderSubmission> {
        if request.owner().trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        let order_request = request.order_request_bytes().map_err(BpError::external)?;
        let now = Utc::now();
        let signed = self
            .auth_service
            .sign_order_request_for_owner(request.owner(), &order_request, now, now)
            .await?;
        Ok(SignedOrderSubmission::new(request, &signed))
    }
}
