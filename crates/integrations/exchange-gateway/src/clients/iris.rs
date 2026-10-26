use crate::{
    clients::client::WebsocketClient, panic_guard::PanicUtils,
    websocket_listener::WebsocketListener,
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, ConnectorError, TimeoutError};
use futures_timer::Delay;
use iris::{Client as IrisClient, Config as IrisConfig, ConnectionError, Listener as IrisListener};
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};
use tracing::error;

pub struct IrisWebsocketClient {
    client: IrisClient<String, serde_json::Value>,
}

impl IrisWebsocketClient {
    pub fn with_config(url: &str, config: IrisConfig, listener: Arc<WebsocketListener>) -> Self {
        let client = IrisClient::new(
            config,
            Arc::new(IrisListenerAdapter { delegate: listener }),
            url,
        );
        Self { client }
    }
    async fn send_message_with_delay<D>(&self, message: String, delay: D) -> BpResult<()>
    where
        D: Future<Output = ()> + Send + 'static,
    {
        let mut send = Box::pin(self.client.send(message));
        let mut delay = Box::pin(delay);
        poll_fn(move |cx| match send.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(Self::map_send_error)),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(BpError::send_unknown(BpError::Timeout(
                    TimeoutError::TimedOut,
                )))),
                Poll::Pending => Poll::Pending,
            },
        })
        .await
    }
    fn map_send_error(error: ConnectionError) -> BpError {
        match error {
            ConnectionError::ConnectionClosed | ConnectionError::SendMessage(_) => {
                BpError::send_not_sent_external(error)
            }
            error => BpError::send_unknown_external(error),
        }
    }
}

#[async_trait]
impl WebsocketClient for IrisWebsocketClient {
    async fn connect(&self) -> BpResult<()> {
        self.client.connect().await.map_err(BpError::external)
    }
    fn is_connected(&self) -> bool {
        self.client.is_connected()
    }
    async fn send(&self, message: String, timeout: Duration) -> BpResult<()> {
        self.send_message_with_delay(message, Delay::new(timeout))
            .await
    }
    async fn disconnect(&self) -> BpResult<()> {
        self.client.disconnect().await.map_err(BpError::external)
    }
}

impl std::fmt::Debug for IrisWebsocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrisWebsocketClient")
            .field("client", &"<websocket::WebsocketClient>")
            .finish()
    }
}

struct IrisListenerAdapter {
    delegate: Arc<WebsocketListener>,
}

#[async_trait]
impl IrisListener<serde_json::Value> for IrisListenerAdapter {
    async fn on_message(&self, message: serde_json::Value) {
        let future = self.delegate.on_message(message);
        let _ = match PanicUtils::catch_panic_async(future).await {
            Ok(result) => result,
            Err(panic) => {
                let error = BpError::Connector(ConnectorError::CallbackPanicked(
                    panic.message().to_string(),
                ));
                error!(error = %error, "websocket on_message callback panicked");
                Ok(())
            }
        };
    }
}

impl std::fmt::Debug for IrisListenerAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrisListenerAdapter")
            .field("delegate", &"<Listener>")
            .finish()
    }
}
