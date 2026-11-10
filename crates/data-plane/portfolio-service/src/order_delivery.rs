use crate::SignedOrderSubmission;
use platform_errors::{BpError, BpResult};
use platform_types::url_scheme::UrlScheme;

pub struct OrderRequestDelivery {
    url: reqwest::Url,
    client: reqwest::Client,
}

impl OrderRequestDelivery {
    pub fn new(url: &str) -> BpResult<Self> {
        let url = reqwest::Url::parse(url)
            .map_err(|error| BpError::config_invalid_source("order_request_url", error))?;
        if !UrlScheme::from_url(&url).is_some_and(UrlScheme::is_http) {
            return Err(BpError::config_invalid(
                "order_request_url",
                "must use the http or https scheme",
            ));
        }
        let client = reqwest::Client::builder()
            .build()
            .map_err(BpError::external)?;
        Ok(Self { url, client })
    }

    pub async fn deliver(&self, submission: &SignedOrderSubmission) -> BpResult<()> {
        let response = self
            .client
            .post(self.url.clone())
            .json(submission)
            .send()
            .await
            .map_err(|error| {
                BpError::http_request("deliver signed order request", self.url.as_str(), error)
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(BpError::http_rejected(
                "deliver signed order request",
                self.url.as_str(),
                status.as_u16(),
                body,
            ));
        }
        Ok(())
    }
}
