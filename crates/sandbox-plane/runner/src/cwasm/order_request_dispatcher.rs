use platform_auth_service::AuthService;
use platform_errors::{BpError, BpResult};
use serde_json::json;
use stock_trek::{
    actions::ResolvedAction,
    types::{AccountId, AssetId, CexId, OrderRequest},
};

pub struct OrderRequestDispatcher {
    url: reqwest::Url,
    owner: String,
    auth_service: AuthService,
    client: reqwest::Client,
}

impl OrderRequestDispatcher {
    pub fn new(url: &str, owner: impl Into<String>, auth_service: AuthService) -> BpResult<Self> {
        let url = reqwest::Url::parse(url)
            .map_err(|error| BpError::config_invalid_source("order_request_url", error))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(BpError::config_invalid(
                "order_request_url",
                "must use the http or https scheme",
            ));
        }
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        let client = reqwest::Client::builder()
            .build()
            .map_err(BpError::external)?;
        Ok(Self {
            url,
            owner,
            auth_service,
            client,
        })
    }

    pub async fn dispatch(&self, actions: impl IntoIterator<Item = String>) -> BpResult<()> {
        for action in actions {
            let action: ResolvedAction =
                serde_json::from_str(&action).map_err(BpError::external)?;
            if let ResolvedAction::PlaceOrder {
                cex_id,
                account_id,
                order_request,
            } = action
            {
                self.submit(&cex_id, &account_id, &order_request).await?;
            }
        }
        Ok(())
    }

    async fn submit(
        &self,
        cex_id: &CexId,
        account_id: &AccountId,
        order_request: &OrderRequest<AssetId, f64>,
    ) -> BpResult<()> {
        let bytes = serde_json::to_vec(order_request).map_err(BpError::external)?;
        let request_value = serde_json::to_value(order_request).map_err(BpError::external)?;
        let signed = self
            .auth_service
            .sign_order_request_for_owner(&self.owner, &bytes)
            .await?;
        let payload = json!({
            "cex": cex_id.to_string(),
            "account": account_id.0,
            "tokenId": signed.token_id(),
            "orderRequest": request_value,
            "signature": signed.signature(),
        });
        let response = self
            .client
            .post(self.url.clone())
            .json(&payload)
            .send()
            .await
            .map_err(|error| {
                BpError::http_request("submit order request", self.url.as_str(), error)
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(BpError::http_rejected(
                "submit order request",
                self.url.as_str(),
                status.as_u16(),
                body,
            ));
        }
        Ok(())
    }
}
