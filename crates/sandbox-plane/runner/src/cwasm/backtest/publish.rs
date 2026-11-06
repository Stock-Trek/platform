use futures::SinkExt;
use serde_json::Value;

use super::spec::Spec;

pub(super) struct ResultPublisher;

impl ResultPublisher {
    pub(super) async fn publish(spec: &Spec, result: &Value) {
        if let Err(error) = Self::stream_result(&spec.results_stream_url, result).await {
            eprintln!("cwasm-invoker: failed to stream simulation result: {error}");
        }
        if let Err(error) = Self::post_result(&spec.results_post_url, result).await {
            eprintln!("cwasm-invoker: failed to post simulation result: {error}");
        }
    }

    async fn stream_result(url: &str, result: &Value) -> Result<(), String> {
        let builder = tokio_websockets::ClientBuilder::new()
            .uri(url)
            .map_err(|error| format!("invalid results stream url: {error}"))?;
        let (mut stream, _) = builder
            .connect()
            .await
            .map_err(|error| format!("connection failed: {error}"))?;
        stream
            .send(tokio_websockets::Message::text(result.to_string()))
            .await
            .map_err(|error| format!("send failed: {error}"))?;
        stream
            .close()
            .await
            .map_err(|error| format!("close failed: {error}"))?;
        Ok(())
    }

    async fn post_result(url: &str, result: &Value) -> Result<(), String> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|error| format!("client could not be built: {error}"))?;
        let response = client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(result.to_string())
            .send()
            .await
            .map_err(|error| format!("post failed: {error}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("post returned status {status}"));
        }
        Ok(())
    }
}
