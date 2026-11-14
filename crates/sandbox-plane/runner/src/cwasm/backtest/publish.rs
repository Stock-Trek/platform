use futures::SinkExt;
use serde_json::Value;
use std::{sync::LazyLock, time::Duration};
use tokio::time::timeout;

use crate::cwasm::backtest::{BacktestError, spec::Spec};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

struct ClientCache;

impl ClientCache {
    fn get() -> Result<&'static reqwest::Client, String> {
        static CLIENT: LazyLock<Result<reqwest::Client, String>> =
            LazyLock::new(ClientCache::build);
        match &*CLIENT {
            Ok(client) => Ok(client),
            Err(error) => Err(error.clone()),
        }
    }

    fn build() -> Result<reqwest::Client, String> {
        reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|error| format!("client could not be built: {error}"))
    }
}

pub struct ResultPublisher;

impl ResultPublisher {
    pub async fn stream(spec: &Spec, result: &Value) -> Result<(), BacktestError> {
        Self::stream_result(&spec.results_stream_url, result)
            .await
            .map_err(|error| BacktestError::Publish(format!("results stream failed: {error}")))
    }

    pub async fn post(spec: &Spec, result: &Value) -> Result<(), BacktestError> {
        Self::post_result(&spec.results_post_url, result)
            .await
            .map_err(|error| BacktestError::Publish(format!("results post failed: {error}")))
    }

    async fn stream_result(url: &str, result: &Value) -> Result<(), String> {
        let builder = tokio_websockets::ClientBuilder::new()
            .uri(url)
            .map_err(|error| format!("invalid results stream url: {error}"))?;
        let (mut stream, _) = timeout(CONNECT_TIMEOUT, builder.connect())
            .await
            .map_err(|_| format!("connection timed out after {CONNECT_TIMEOUT:?}"))?
            .map_err(|error| format!("connection failed: {error}"))?;
        timeout(
            REQUEST_TIMEOUT,
            stream.send(tokio_websockets::Message::text(result.to_string())),
        )
        .await
        .map_err(|_| format!("send timed out after {REQUEST_TIMEOUT:?}"))?
        .map_err(|error| format!("send failed: {error}"))?;
        timeout(REQUEST_TIMEOUT, stream.close())
            .await
            .map_err(|_| format!("close timed out after {REQUEST_TIMEOUT:?}"))?
            .map_err(|error| format!("close failed: {error}"))?;
        Ok(())
    }

    async fn post_result(url: &str, result: &Value) -> Result<(), String> {
        let response = ClientCache::get()?
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(result.to_string())
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    format!("post timed out after {REQUEST_TIMEOUT:?}")
                } else {
                    format!("post failed: {error}")
                }
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("post returned status {status}"));
        }
        Ok(())
    }
}
