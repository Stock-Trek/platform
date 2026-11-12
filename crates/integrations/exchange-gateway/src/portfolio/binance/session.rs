use super::transport::{BinanceAccountEvent, BinanceEventStream, BinanceTransport};
use crate::portfolio::{PortfolioEvent, PortfolioPosition, PortfolioSession, PortfolioSnapshot};
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
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

    fn event(event: BinanceAccountEvent) -> PortfolioEvent {
        match event {
            BinanceAccountEvent::Position(event) => {
                PortfolioEvent::Position(PortfolioPosition::new(
                    event.version(),
                    event.balances().clone(),
                    event.occurred_at(),
                ))
            }
            BinanceAccountEvent::ListenKeyExpired => PortfolioEvent::SessionExpired,
        }
    }
}

#[async_trait]
impl PortfolioSession for BinanceSession {
    async fn snapshot(&self) -> BpResult<PortfolioSnapshot> {
        let snapshot = self.transport.account_snapshot(&self.credentials).await?;
        Ok(PortfolioSnapshot::new(
            snapshot.balances().clone(),
            snapshot.occurred_at(),
        ))
    }

    async fn refresh(&self) -> BpResult<()> {
        self.transport
            .keepalive_listen_key(&self.credentials, &self.listen_key)
            .await
    }

    async fn close(&mut self) -> BpResult<()> {
        self.transport
            .close_listen_key(&self.credentials, &self.listen_key)
            .await
    }

    async fn next(&mut self) -> BpResult<Option<PortfolioEvent>> {
        Ok(self.stream.lock().await.next().await?.map(Self::event))
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
