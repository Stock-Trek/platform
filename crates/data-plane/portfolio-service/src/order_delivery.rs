use crate::SignedOrderSubmission;
use platform_errors::{PlatformError, PlatformResult};
use platform_types::UrlScheme;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub struct OrderRequestDelivery {
    url: reqwest::Url,
    client: reqwest::Client,
}

impl OrderRequestDelivery {
    pub fn new(url: &str) -> PlatformResult<Self> {
        let url = reqwest::Url::parse(url)
            .map_err(|error| PlatformError::config_invalid_source("order_request_url", error))?;
        if !UrlScheme::from_url(&url).is_some_and(UrlScheme::is_http) {
            return Err(PlatformError::config_invalid(
                "order_request_url",
                "must use the http or https scheme",
            ));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(PlatformError::external)?;
        Ok(Self { url, client })
    }

    pub async fn deliver(&self, submission: &SignedOrderSubmission) -> PlatformResult<()> {
        let response = self
            .client
            .post(self.url.clone())
            .json(submission)
            .send()
            .await
            .map_err(|error| {
                PlatformError::http_request(
                    "deliver signed order request",
                    self.url.as_str(),
                    error,
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(PlatformError::http_rejected(
                "deliver signed order request",
                self.url.as_str(),
                status.as_u16(),
                body,
            ));
        }
        Ok(())
    }
}
