use super::{
    ConnectorHealthSink, ExchangePortfolioConnector, GatewayPortfolioTransport, NoopPortfolioFeed,
    PortfolioCredentialSource, PortfolioFeed, PortfolioTransport, StaticPortfolioCredentials,
};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    binance::{exchange::BinanceExchange, portfolio::BinancePortfolioConfig},
};
use platform_type_bridge::BinanceConverter;
use secrecy::{ExposeSecret, SecretString};
use std::{collections::BTreeMap, fmt, sync::Arc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[derive(Clone)]
pub struct PortfolioConnectorConfig {
    transports: Vec<Arc<dyn PortfolioTransport>>,
    credentials: Arc<dyn PortfolioCredentialSource>,
}

impl PortfolioConnectorConfig {
    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let mut transports: Vec<Arc<dyn PortfolioTransport>> = Vec::new();
        let mut credentials = BTreeMap::new();
        if let Some(binance) = BinancePortfolioConfig::from_vars(vars)? {
            for credential in binance.credentials() {
                credentials.insert(
                    (CexId::Binance, AccountId::new(credential.account())),
                    ApiKeyCredentials {
                        api_key: credential.api_key().to_string(),
                        secret: SecretString::from(credential.secret().expose_secret().to_string()),
                    },
                );
            }
            transports.push(Arc::new(GatewayPortfolioTransport::new(
                BinanceExchange,
                binance.rest_url(),
                binance.websocket_url(),
                binance.listen_key_refresh(),
                binance.reconnect_backoff(),
                BinanceConverter::ticker_mapping_static(),
            )));
        }
        Ok(Self {
            transports,
            credentials: Arc::new(StaticPortfolioCredentials::new(credentials)),
        })
    }

    pub fn feed(&self, health: Arc<dyn ConnectorHealthSink>) -> Arc<dyn PortfolioFeed> {
        if self.transports.is_empty() {
            return Arc::new(NoopPortfolioFeed::new());
        }
        ExchangePortfolioConnector::new(
            self.transports.iter().cloned(),
            Arc::clone(&self.credentials),
            health,
        )
        .feed()
    }
}

impl fmt::Debug for PortfolioConnectorConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortfolioConnectorConfig")
            .field(
                "exchanges",
                &self
                    .transports
                    .iter()
                    .map(|transport| transport.cex_id())
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}
