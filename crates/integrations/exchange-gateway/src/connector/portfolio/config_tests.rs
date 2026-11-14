use crate::connector::portfolio::{
    PortfolioAccounts, PortfolioConnectorConfig, PortfolioCredentialSource,
    PortfolioExchangeConfig,
    portfolio_test_fakes_tests::{Fixture, TestTransport},
};
use platform_config::EnvVars;
use platform_errors::PlatformResult;
use std::{sync::Arc, time::Duration};
use stock_trek_types::cex::cex_id::CexId;

const CONNECTOR_CEXES: &str = "PORTFOLIO_CONNECTOR_CEXES";

struct ConfigFixture;

impl ConfigFixture {
    fn config(
        vars: &[(&str, &str)],
        exchanges: impl IntoIterator<Item = PortfolioExchangeConfig>,
    ) -> PlatformResult<PortfolioConnectorConfig> {
        PortfolioConnectorConfig::from_vars(
            &EnvVars::from_map(
                vars.iter()
                    .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
            ),
            exchanges,
        )
    }

    fn exchanges() -> Vec<PortfolioExchangeConfig> {
        vec![PortfolioExchangeConfig::new(Arc::new(TestTransport::new(
            Duration::from_secs(60),
            Duration::from_secs(1),
        )))]
    }
}

#[test]
fn configured_exchange_is_selected_by_default() {
    let config = ConfigFixture::config(&[], ConfigFixture::exchanges()).unwrap();

    assert_eq!(config.cex_ids(), vec![CexId::Binance]);
}

#[test]
fn topology_can_select_the_connector() {
    let binance = CexId::Binance.to_string();
    let vars = [(CONNECTOR_CEXES, binance.as_str())];
    let config = ConfigFixture::config(&vars, ConfigFixture::exchanges()).unwrap();

    assert_eq!(config.cex_ids(), vec![CexId::Binance]);
}

#[test]
fn no_exchanges_is_valid() {
    assert!(ConfigFixture::config(&[], []).unwrap().cex_ids().is_empty());
}

#[tokio::test]
async fn accounts_can_be_added_and_removed_after_the_config_is_built() {
    let config = ConfigFixture::config(&[], ConfigFixture::exchanges()).unwrap();
    assert!(config.accounts().accounts(CexId::Binance).is_empty());

    config
        .add_account(CexId::Binance, Fixture::account(), Fixture::credentials())
        .expect("add account");
    let credentials = config
        .accounts()
        .credentials(CexId::Binance, &Fixture::account())
        .await
        .expect("credentials");
    assert_eq!(credentials.api_key, "api-key");

    config
        .remove_account(CexId::Binance, &Fixture::account())
        .expect("remove account")
        .expect("removed credentials");
    assert!(config.accounts().accounts(CexId::Binance).is_empty());
}

#[test]
fn accounts_can_be_replaced_while_running() {
    let accounts =
        PortfolioAccounts::new([((CexId::Binance, Fixture::account()), Fixture::credentials())]);
    accounts.replace(
        CexId::Binance,
        [(Fixture::account(), Fixture::credentials())],
    );
    assert_eq!(
        accounts.accounts(CexId::Binance),
        [Fixture::account()].into()
    );
}

#[test]
fn invalid_topology_is_rejected() {
    for value in ["unknown", "binance,unknown", "binance,binance", "binance,"] {
        let vars = [(CONNECTOR_CEXES, value)];
        assert!(
            ConfigFixture::config(&vars, ConfigFixture::exchanges()).is_err(),
            "{value}"
        );
    }
}
