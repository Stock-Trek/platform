use crate::connector::portfolio::{
    PortfolioConnectorConfig, PortfolioEventStream, PortfolioExchangeConfig, PortfolioTransport,
};
use async_trait::async_trait;
use platform_config::EnvVars;
use platform_errors::{BuildError, BuildResult};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    portfolio::{AccountSnapshot, ConnectRequest, PortfolioAuth},
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

const CONNECTOR_CEXES: &str = "PORTFOLIO_CONNECTOR_CEXES";

struct Fixture;

impl Fixture {
    fn config(
        vars: &[(&str, &str)],
        exchanges: impl IntoIterator<Item = PortfolioExchangeConfig>,
    ) -> BuildResult<PortfolioConnectorConfig> {
        PortfolioConnectorConfig::from_vars(
            &EnvVars::from_map(
                vars.iter()
                    .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
            ),
            exchanges,
        )
    }

    fn exchanges() -> Vec<PortfolioExchangeConfig> {
        vec![PortfolioExchangeConfig::new(
            Arc::new(TestTransport),
            BTreeMap::<AccountId, ApiKeyCredentials>::new(),
        )]
    }
}

struct TestTransport;

#[async_trait]
impl PortfolioTransport for TestTransport {
    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn reconnect_backoff(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn connect_request(&self) -> BuildResult<ConnectRequest> {
        Err(BuildError::config_invalid("test", "unavailable"))
    }

    async fn portfolio_auth(&self, _credentials: &ApiKeyCredentials) -> BuildResult<PortfolioAuth> {
        Err(BuildError::config_invalid("test", "unavailable"))
    }

    async fn account_snapshot(
        &self,
        _credentials: &ApiKeyCredentials,
    ) -> BuildResult<AccountSnapshot> {
        Err(BuildError::config_invalid("test", "unavailable"))
    }

    async fn open_stream(
        &self,
        _request: ConnectRequest,
    ) -> BuildResult<Box<dyn PortfolioEventStream>> {
        Err(BuildError::config_invalid("test", "unavailable"))
    }
}

#[test]
fn configured_exchange_is_selected_by_default() {
    let config = Fixture::config(&[], Fixture::exchanges()).unwrap();

    assert_eq!(config.cex_ids(), vec![CexId::Binance]);
}

#[test]
fn topology_can_select_the_connector() {
    let binance = CexId::Binance.to_string();
    let vars = [(CONNECTOR_CEXES, binance.as_str())];
    let config = Fixture::config(&vars, Fixture::exchanges()).unwrap();

    assert_eq!(config.cex_ids(), vec![CexId::Binance]);
}

#[test]
fn no_exchanges_is_valid() {
    assert!(Fixture::config(&[], []).unwrap().cex_ids().is_empty());
}

#[test]
fn invalid_topology_is_rejected() {
    for value in ["unknown", "binance,unknown", "binance,binance", "binance,"] {
        let vars = [(CONNECTOR_CEXES, value)];
        assert!(
            Fixture::config(&vars, Fixture::exchanges()).is_err(),
            "{value}"
        );
    }
}
