use super::JwtCache;
use platform_errors::{BpError, BpResult};
use platform_messages::order_request::UnsignedOrderRequest;
use platform_sigv4::SigV4Config;
use platform_types::url_scheme::UrlScheme;
use stock_trek::{
    actions::ResolvedAction,
    types::{AccountId, AssetId, CexId, OrderRequest},
};

pub struct OrderRequestDispatcher {
    url: reqwest::Url,
    owner: String,
    client: reqwest::Client,
    jwt: JwtCache,
}

impl OrderRequestDispatcher {
    #[allow(dead_code)]
    pub fn new(url: &str, auth_service_url: &str, owner: impl Into<String>) -> BpResult<Self> {
        let owner = owner.into();
        Self::with_jwt(url, owner.clone(), JwtCache::new(auth_service_url, owner)?)
    }

    pub fn with_sigv4(
        url: &str,
        auth_service_url: &str,
        owner: impl Into<String>,
        sigv4: &SigV4Config,
    ) -> BpResult<Self> {
        let owner = owner.into();
        Self::with_jwt(
            url,
            owner.clone(),
            JwtCache::with_sigv4(auth_service_url, owner, sigv4)?,
        )
    }

    fn with_jwt(url: &str, owner: String, jwt: JwtCache) -> BpResult<Self> {
        let url = reqwest::Url::parse(url)
            .map_err(|error| BpError::config_invalid_source("order_request_url", error))?;
        if !UrlScheme::from_url(&url).is_some_and(UrlScheme::is_http) {
            return Err(BpError::config_invalid(
                "order_request_url",
                "must use the http or https scheme",
            ));
        }
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        let client = reqwest::Client::builder()
            .build()
            .map_err(BpError::external)?;
        Ok(Self {
            url,
            owner,
            client,
            jwt,
        })
    }

    pub async fn prime(&self) -> BpResult<()> {
        self.jwt.token().await.map(|_| ())
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
                self.submit(cex_id, account_id, order_request).await?;
            }
        }
        Ok(())
    }

    async fn submit(
        &self,
        cex_id: CexId,
        account_id: AccountId,
        order_request: OrderRequest<AssetId, f64>,
    ) -> BpResult<()> {
        let payload =
            UnsignedOrderRequest::new(cex_id, account_id, self.owner.clone(), order_request);
        let jwt = self.jwt.token().await?;
        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(jwt)
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
