use super::{AccountEventStream, PortfolioTransport};
use platform_errors::BuildResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    portfolio::{AccountEvent, AccountSnapshot},
};
use std::sync::Arc;
use stock_trek_types::cex::account_id::AccountId;
use tokio::sync::Mutex;

pub struct PortfolioSession {
    account: AccountId,
    credentials: ApiKeyCredentials,
    listen_key: String,
    stream: Mutex<Box<dyn AccountEventStream>>,
    transport: Arc<dyn PortfolioTransport>,
}

impl PortfolioSession {
    pub async fn open(
        transport: Arc<dyn PortfolioTransport>,
        account: &AccountId,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<Self> {
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

    pub async fn snapshot(&self) -> BuildResult<AccountSnapshot> {
        self.transport.account_snapshot(&self.credentials).await
    }

    pub async fn refresh(&self) -> BuildResult<()> {
        self.transport
            .keepalive_listen_key(&self.credentials, &self.listen_key)
            .await
    }

    pub async fn close(&mut self) -> BuildResult<()> {
        self.transport
            .close_listen_key(&self.credentials, &self.listen_key)
            .await
    }

    pub async fn next(&mut self) -> BuildResult<Option<AccountEvent>> {
        self.stream.lock().await.next().await
    }
}

impl std::fmt::Debug for PortfolioSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PortfolioSession")
            .field("account", &self.account)
            .field("listen_key", &"[redacted]")
            .finish()
    }
}
