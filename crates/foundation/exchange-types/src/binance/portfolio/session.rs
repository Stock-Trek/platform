use super::transport::{
    AccountSnapshot, BinanceAccountEvent, BinanceEventStream, BinanceTransport,
};
use crate::api_key_credential::ApiKeyCredentials;
use platform_errors::BpResult;
use std::sync::Arc;
use stock_trek_types::cex::account_id::AccountId;
use tokio::sync::Mutex;

pub struct BinanceSession {
    account: AccountId,
    credentials: ApiKeyCredentials,
    listen_key: String,
    stream: Mutex<Box<dyn BinanceEventStream>>,
    transport: Arc<dyn BinanceTransport>,
}

impl BinanceSession {
    pub async fn open(
        transport: Arc<dyn BinanceTransport>,
        account: &AccountId,
        credentials: &ApiKeyCredentials,
    ) -> BpResult<Self> {
        let listen_key = transport.create_listen_key(credentials).await?;
        let stream = match transport.open_stream(&listen_key).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = transport.close_listen_key(credentials, &listen_key).await;
                return Err(error);
            }
        };
        Ok(Self {
            account: account.clone(),
            credentials: credentials.clone(),
            listen_key,
            stream: Mutex::new(stream),
            transport,
        })
    }

    pub async fn snapshot(&self) -> BpResult<AccountSnapshot> {
        self.transport.account_snapshot(&self.credentials).await
    }

    pub async fn refresh(&self) -> BpResult<()> {
        self.transport
            .keepalive_listen_key(&self.credentials, &self.listen_key)
            .await
    }

    pub async fn close(&mut self) -> BpResult<()> {
        self.transport
            .close_listen_key(&self.credentials, &self.listen_key)
            .await
    }

    pub async fn next(&mut self) -> BpResult<Option<BinanceAccountEvent>> {
        self.stream.lock().await.next().await
    }
}

impl std::fmt::Debug for BinanceSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BinanceSession")
            .field("account", &self.account)
            .field("listen_key", &"[redacted]")
            .finish()
    }
}
