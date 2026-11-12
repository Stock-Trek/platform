use super::{
    config::BinancePortfolioConfig, gateway_transport::GatewayBinanceTransport,
    session::BinanceSession, transport::BinanceTransport,
};
use crate::portfolio::{PortfolioExchangeTransport, PortfolioSession};
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
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

    pub fn with_transport(mut self, transport: Arc<dyn BinanceTransport>) -> Self {
        self.transport = transport;
        self
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
