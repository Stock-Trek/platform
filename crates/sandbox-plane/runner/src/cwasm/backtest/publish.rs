use futures::SinkExt;
use serde_json::Value;
use std::time::Duration;
use tokio::time::timeout;

use crate::cwasm::backtest::{BacktestError, PlatformResult, spec::Spec};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub struct ResultPublisher;

impl ResultPublisher {
    pub async fn publish(spec: &Spec, result: &Value) -> PlatformResult<()> {
        let stream = Self::stream_result(&spec.results_stream_url, result).await;
        let post = Self::post_result(&spec.results_post_url, result).await;
        match (stream, post) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(stream), Ok(())) => {
                Err(BacktestError::Publish(format!("results stream failed: {stream}")).into())
            }
            (Ok(()), Err(post)) => {
                Err(BacktestError::Publish(format!("results post failed: {post}")).into())
            }
            (Err(stream), Err(post)) => Err(BacktestError::Publish(format!(
                "results stream failed: {stream}; results post failed: {post}"
            ))
            .into()),
        }
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
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|error| format!("client could not be built: {error}"))?;
        let response = client
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
