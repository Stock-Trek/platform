use chrono::{Duration, Utc};
use platform_auth_service::{
    ApiToken, OrderRequestContext, OrderRequestSigner, SignedOrderRequest,
};
use platform_errors::{BpError, BpResult};
use secrecy::SecretString;
use serde_json::json;
use stock_trek::{
    actions::ResolvedAction,
    types::{AccountId, AssetId, CexId, OrderRequest},
};

const ORDER_SIGNATURE_VALIDITY_SECONDS: i64 = 300;

pub struct OrderRequestDispatcher {
    url: reqwest::Url,
    token_id: String,
    signer: OrderRequestSigner,
    client: reqwest::Client,
}

impl OrderRequestDispatcher {
    pub fn new(url: &str, token_id: impl Into<String>, secret: &SecretString) -> BpResult<Self> {
        let url = reqwest::Url::parse(url)
            .map_err(|error| BpError::config_invalid_source("order_request_url", error))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(BpError::config_invalid(
                "order_request_url",
                "must use the http or https scheme",
            ));
        }
        let token_id = token_id.into();
        ApiToken::validate_id(&token_id)?;
        let signer = OrderRequestSigner::new(secret)?;
        let client = reqwest::Client::builder()
            .build()
            .map_err(BpError::external)?;
        Ok(Self {
            url,
            token_id,
            signer,
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
        let context = OrderRequestContext::generate(
            &self.token_id,
            Utc::now(),
            Duration::seconds(ORDER_SIGNATURE_VALIDITY_SECONDS),
        )?;
        let signature = self.signer.sign_order_request(&context, &bytes)?;
        let signed = SignedOrderRequest::from_context(&context, signature);
        let payload = json!({
            "cex": cex_id.to_string(),
            "account": account_id.0,
            "tokenId": self.token_id,
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
