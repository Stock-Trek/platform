use super::{
    PortfolioEvent, PortfolioExchangeTransport, PortfolioPosition, PortfolioSession,
    PortfolioSnapshot,
};
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_exchange_gateway::binance::GatewayBinanceTransport;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    binance::portfolio::{
        AccountSnapshot, BinanceAccountEvent, BinancePortfolioConfig, BinanceSession,
        BinanceTransport,
    },
};
use std::{sync::Arc, time::Duration};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub struct BinanceExchangeTransport {
    config: Arc<BinancePortfolioConfig>,
    transport: Arc<dyn BinanceTransport>,
}

impl BinanceExchangeTransport {
    pub fn new(config: BinancePortfolioConfig) -> Self {
        let transport = Arc::new(GatewayBinanceTransport::new(config.clone()));
        Self {
            config: Arc::new(config),
            transport,
        }
    }

    pub fn with_transport(
        config: BinancePortfolioConfig,
        transport: Arc<dyn BinanceTransport>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            transport,
        }
    }
}

#[async_trait]
impl PortfolioExchangeTransport for BinanceExchangeTransport {
    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn refresh_interval(&self) -> Duration {
        self.config.listen_key_refresh()
    }

    fn reconnect_backoff(&self) -> Duration {
        self.config.reconnect_backoff()
    }

    async fn open_session(
        &self,
        account: &AccountId,
        credentials: &ApiKeyCredentials,
    ) -> BpResult<Box<dyn PortfolioSession>> {
        Ok(Box::new(
            BinanceSession::open(Arc::clone(&self.transport), account, credentials).await?,
        ))
    }
}

#[async_trait]
impl PortfolioSession for BinanceSession {
    async fn snapshot(&self) -> BpResult<PortfolioSnapshot> {
        let snapshot: AccountSnapshot = BinanceSession::snapshot(self).await?;
        Ok(PortfolioSnapshot::new(
            snapshot.balances().clone(),
            snapshot.occurred_at(),
        ))
    }

    async fn refresh(&self) -> BpResult<()> {
        BinanceSession::refresh(self).await
    }

    async fn close(&mut self) -> BpResult<()> {
        BinanceSession::close(self).await
    }

    async fn next(&mut self) -> BpResult<Option<PortfolioEvent>> {
        Ok(BinanceSession::next(self).await?.map(PortfolioEvent::from))
    }
}

impl From<BinanceAccountEvent> for PortfolioEvent {
    fn from(event: BinanceAccountEvent) -> Self {
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
