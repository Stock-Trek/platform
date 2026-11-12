use super::PortfolioConnectorConfig;
use platform_config::EnvVars;
use platform_errors::BuildResult;
use stock_trek_types::cex::cex_id::CexId;

const ENABLED: &str = "PORTFOLIO_BINANCE_ENABLED";
const CREDENTIALS: &str = "PORTFOLIO_BINANCE_CREDENTIALS";
const CONNECTOR_CEXES: &str = "PORTFOLIO_CONNECTOR_CEXES";

struct Fixture;

impl Fixture {
    fn config(vars: &[(&str, &str)]) -> BuildResult<PortfolioConnectorConfig> {
        PortfolioConnectorConfig::from_vars(&EnvVars::from_map(
            vars.iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
        ))
    }

    fn enabled() -> Vec<(&'static str, &'static str)> {
        vec![
            (ENABLED, "true"),
            (
                CREDENTIALS,
                r#"[{"account":"account-1","api_key":"key-1","secret":"secret-1"}]"#,
            ),
        ]
    }
}

#[test]
fn configured_exchange_is_selected_by_default() {
    let config = Fixture::config(&Fixture::enabled()).unwrap();

    assert_eq!(config.cex_ids(), vec![CexId::Binance]);
}

#[test]
fn topology_can_select_the_binance_connector() {
    let mut vars = Fixture::enabled();
    vars.push((CONNECTOR_CEXES, "binance"));
    let config = Fixture::config(&vars).unwrap();

    assert_eq!(config.cex_ids(), vec![CexId::Binance]);
}

#[test]
fn topology_can_disable_all_connectors() {
    let mut vars = Fixture::enabled();
    vars.retain(|(name, _)| *name != ENABLED);
    vars.retain(|(name, _)| *name != CREDENTIALS);
    vars.push((ENABLED, "false"));
    vars.push((CONNECTOR_CEXES, "binance"));

    assert!(Fixture::config(&vars).unwrap().cex_ids().is_empty());
}

#[test]
fn invalid_topology_is_rejected() {
    for value in ["unknown", "binance,unknown", "binance,binance", "binance,"] {
        let mut vars = Fixture::enabled();
        vars.push((CONNECTOR_CEXES, value));
        assert!(Fixture::config(&vars).is_err(), "{value}");
    }
}
