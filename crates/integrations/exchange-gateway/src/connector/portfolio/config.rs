use crate::connector::portfolio::{
    ConnectorHealthSink, ExchangePortfolioConnector, NoopPortfolioFeed, PortfolioAccounts,
    PortfolioCredentialSource, PortfolioExchange, PortfolioFeed,
};
use platform_config::EnvVars;
use platform_errors::{PlatformError, PlatformResult};
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use std::{collections::BTreeSet, fmt, sync::Arc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

const CONNECTOR_CEXES: &str = "PORTFOLIO_CONNECTOR_CEXES";

pub struct PortfolioExchangeConfig {
    exchange: Arc<dyn PortfolioExchange>,
    credentials: Vec<(AccountId, ApiKeyCredentials)>,
}

impl PortfolioExchangeConfig {
    pub fn new(exchange: Arc<dyn PortfolioExchange>) -> Self {
        Self {
            exchange,
            credentials: Vec::new(),
        }
    }

    pub fn with_credentials(
        exchange: Arc<dyn PortfolioExchange>,
        credentials: impl IntoIterator<Item = (AccountId, ApiKeyCredentials)>,
    ) -> Self {
        Self {
            exchange,
            credentials: credentials.into_iter().collect(),
        }
    }

    pub fn exchange(&self) -> Arc<dyn PortfolioExchange> {
        Arc::clone(&self.exchange)
    }
}

#[derive(Clone)]
pub struct PortfolioConnectorConfig {
    exchanges: Vec<Arc<dyn PortfolioExchange>>,
    accounts: Arc<PortfolioAccounts>,
}

impl PortfolioConnectorConfig {
    pub fn from_vars(
        vars: &EnvVars,
        exchanges: impl IntoIterator<Item = PortfolioExchangeConfig>,
    ) -> PlatformResult<Self> {
        let mut supported_exchanges: Vec<Arc<dyn PortfolioExchange>> = Vec::new();
        let mut accounts = Vec::new();
        let selected = Self::selected_cexes(vars)?;
        for exchange in exchanges {
            let cex_id = exchange.exchange.cex_id();
            if selected
                .as_ref()
                .is_some_and(|selected| !selected.contains(&cex_id))
            {
                continue;
            }
            accounts.extend(
                exchange
                    .credentials
                    .into_iter()
                    .map(|(account, credentials)| ((cex_id, account), credentials)),
            );
            supported_exchanges.push(exchange.exchange);
        }
        Ok(Self {
            exchanges: supported_exchanges,
            accounts: Arc::new(PortfolioAccounts::new(accounts)),
        })
    }

    pub fn add_account(
        &self,
        cex_id: CexId,
        account: AccountId,
        credentials: ApiKeyCredentials,
    ) -> PlatformResult<()> {
        self.ensure_supported(cex_id)?;
        self.accounts.insert(cex_id, account, credentials);
        Ok(())
    }

    pub fn remove_account(
        &self,
        cex_id: CexId,
        account: &AccountId,
    ) -> PlatformResult<Option<ApiKeyCredentials>> {
        self.ensure_supported(cex_id)?;
        Ok(self.accounts.remove(cex_id, account))
    }

    pub fn accounts(&self) -> Arc<PortfolioAccounts> {
        Arc::clone(&self.accounts)
    }

    pub fn feed(&self, health: Arc<dyn ConnectorHealthSink>) -> Arc<dyn PortfolioFeed> {
        if self.exchanges.is_empty() {
            return Arc::new(NoopPortfolioFeed::new());
        }
        ExchangePortfolioConnector::new(
            self.exchanges.iter().cloned(),
            Arc::clone(&self.accounts) as Arc<dyn PortfolioCredentialSource>,
            health,
        )
        .feed()
    }

    pub fn cex_ids(&self) -> Vec<CexId> {
        self.exchanges
            .iter()
            .map(|exchange| exchange.cex_id())
            .collect()
    }

    fn ensure_supported(&self, cex_id: CexId) -> PlatformResult<()> {
        if self
            .exchanges
            .iter()
            .any(|exchange| exchange.cex_id() == cex_id)
        {
            return Ok(());
        }
        Err(PlatformError::bad_value(
            "portfolio exchange",
            cex_id.to_string(),
            "is not supported",
        ))
    }

    pub fn selected_cexes(vars: &EnvVars) -> PlatformResult<Option<BTreeSet<CexId>>> {
        let Some(value) = vars.optional(CONNECTOR_CEXES)? else {
            return Ok(None);
        };
        let mut selected = BTreeSet::new();
        for exchange in value.split(',').map(str::trim) {
            if exchange.is_empty() {
                return Err(PlatformError::config_invalid(
                    CONNECTOR_CEXES,
                    "contains an empty exchange",
                ));
            }
            let exchange = exchange.parse::<CexId>().map_err(|_| {
                PlatformError::config_invalid(
                    CONNECTOR_CEXES,
                    format!("unknown exchange {exchange}"),
                )
            })?;
            if !selected.insert(exchange) {
                return Err(PlatformError::config_invalid(
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
                    .exchanges
                    .iter()
                    .map(|exchange| exchange.cex_id())
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}
