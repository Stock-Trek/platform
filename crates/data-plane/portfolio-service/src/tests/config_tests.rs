use crate::PortfolioConfig;
use platform_config::env_vars::EnvVars;

#[test]
fn config_reads_the_log_filter() {
    let vars = EnvVars::from_map([("RUST_LOG", "info")]);
    let config = PortfolioConfig::from_vars(&vars).expect("config");
    assert_eq!(config.log_filter(), "info");
}

#[test]
fn config_requires_a_log_filter() {
    let vars = EnvVars::from_map(Vec::<(String, String)>::new());
    assert!(PortfolioConfig::from_vars(&vars).is_err());
}

#[test]
fn config_rejects_an_invalid_log_filter() {
    let vars = EnvVars::from_map([("RUST_LOG", "not a filter [")]);
    assert!(PortfolioConfig::from_vars(&vars).is_err());
}
