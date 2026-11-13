use crate::connector::portfolio::{
    ConnectorHealthSink, ExchangePortfolioConnector, NoopPortfolioFeed, PortfolioCredentialSource,
    PortfolioFeed, PortfolioTransport, StaticPortfolioCredentials,
};
use platform_config::EnvVars;
use platform_errors::{BuildError, BuildResult};
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

const CONNECTOR_CEXES: &str = "PORTFOLIO_CONNECTOR_CEXES";

pub struct PortfolioExchangeConfig {
    transport: Arc<dyn PortfolioTransport>,
    credentials: BTreeMap<AccountId, ApiKeyCredentials>,
}

impl PortfolioExchangeConfig {
    pub fn new(
        transport: Arc<dyn PortfolioTransport>,
        credentials: impl IntoIterator<Item = (AccountId, ApiKeyCredentials)>,
    ) -> Self {
        Self {
            transport,
            credentials: credentials.into_iter().collect(),
        }
    }

    pub fn transport(&self) -> Arc<dyn PortfolioTransport> {
        Arc::clone(&self.transport)
    }
}

#[derive(Clone)]
pub struct PortfolioConnectorConfig {
    transports: Vec<Arc<dyn PortfolioTransport>>,
    credentials: Arc<dyn PortfolioCredentialSource>,
}

impl PortfolioConnectorConfig {
    pub fn from_vars(
        vars: &EnvVars,
        exchanges: impl IntoIterator<Item = PortfolioExchangeConfig>,
    ) -> BuildResult<Self> {
        let mut transports: Vec<Arc<dyn PortfolioTransport>> = Vec::new();
        let mut credentials = BTreeMap::new();
        let selected = Self::selected_cexes(vars)?;
        for exchange in exchanges {
            let cex_id = exchange.transport.cex_id();
            if selected
                .as_ref()
                .is_some_and(|selected| !selected.contains(&cex_id))
            {
                continue;
            }
            for (account, credential) in exchange.credentials {
                credentials.insert((cex_id, account), credential);
            }
            transports.push(exchange.transport);
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

    pub fn cex_ids(&self) -> Vec<CexId> {
        self.transports
            .iter()
            .map(|transport| transport.cex_id())
            .collect()
    }

    pub fn selected_cexes(vars: &EnvVars) -> BuildResult<Option<BTreeSet<CexId>>> {
        let Some(value) = vars.optional(CONNECTOR_CEXES)? else {
            return Ok(None);
        };
        let mut selected = BTreeSet::new();
        for exchange in value.split(',').map(str::trim) {
            if exchange.is_empty() {
                return Err(BuildError::config_invalid(
                    CONNECTOR_CEXES,
                    "contains an empty exchange",
                ));
            }
            let exchange = exchange.parse::<CexId>().map_err(|_| {
                BuildError::config_invalid(CONNECTOR_CEXES, format!("unknown exchange {exchange}"))
            })?;
            if !selected.insert(exchange) {
                return Err(BuildError::config_invalid(
                    CONNECTOR_CEXES,
                    format!("duplicate exchange {exchange}"),
                ));
            }
        }
        Ok(Some(selected))
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
