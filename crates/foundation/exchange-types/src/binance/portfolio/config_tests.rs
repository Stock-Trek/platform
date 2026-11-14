use crate::binance::portfolio::{config::BinancePortfolioConfig, credentials::CREDENTIALS};
use platform_config::EnvVars;
use secrecy::ExposeSecret;
use std::time::Duration;

const ENABLED: &str = "PORTFOLIO_BINANCE_ENABLED";
const TESTNET: &str = "PORTFOLIO_BINANCE_TESTNET";
const REST_URL: &str = "PORTFOLIO_BINANCE_REST_URL";
const WEBSOCKET_URL: &str = "PORTFOLIO_BINANCE_WEBSOCKET_URL";
const REFRESH: &str = "PORTFOLIO_BINANCE_LISTEN_KEY_REFRESH_SECS";
const BACKOFF: &str = "PORTFOLIO_BINANCE_RECONNECT_BACKOFF_SECS";

struct Fixture;

impl Fixture {
    fn config(
        vars: &[(&str, &str)],
    ) -> platform_errors::PlatformResult<Option<BinancePortfolioConfig>> {
        BinancePortfolioConfig::from_vars(&EnvVars::from_map(
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
fn parses_connector_credentials_and_defaults() {
    let config = Fixture::config(&Fixture::enabled())
        .expect("config")
        .expect("enabled");

    assert_eq!(config.rest_url(), "https://api.binance.com/api/v3");
    assert_eq!(config.websocket_url(), "wss://stream.binance.com:9443/ws");
    assert_eq!(config.credentials()[0].account(), "account-1");
    assert_eq!(config.credentials()[0].api_key(), "key-1");
    assert_eq!(config.credentials()[0].secret().expose_secret(), "secret-1");
    assert_eq!(config.listen_key_refresh(), Duration::from_secs(1800));
    assert_eq!(config.reconnect_backoff(), Duration::from_secs(5));
}

#[test]
fn disabled_connector_needs_no_credentials() {
    assert!(Fixture::config(&[]).expect("config").is_none());
    assert!(
        Fixture::config(&[(ENABLED, "false")])
            .expect("config")
            .is_none()
    );
}

#[test]
fn testnet_selects_testnet_endpoints() {
    let config = Fixture::config(&[
        (ENABLED, "true"),
        (TESTNET, "true"),
        (
            CREDENTIALS,
            r#"[{"account":"account-1","api_key":"key-1","secret":"secret-1"}]"#,
        ),
    ])
    .expect("config")
    .expect("enabled");

    assert_eq!(config.rest_url(), "https://testnet.binance.vision/api/v3");
    assert_eq!(
        config.websocket_url(),
        "wss://stream.testnet.binance.vision/ws"
    );
}

#[test]
fn explicit_endpoints_override_testnet_defaults() {
    let config = Fixture::config(&[
        (ENABLED, "true"),
        (TESTNET, "true"),
        (REST_URL, "https://rest.example.com/api/v3"),
        (WEBSOCKET_URL, "wss://stream.example.com/ws/"),
        (
            CREDENTIALS,
            r#"[{"account":"account-1","api_key":"key-1","secret":"secret-1"}]"#,
        ),
    ])
    .expect("config")
    .expect("enabled");

    assert_eq!(config.rest_url(), "https://rest.example.com/api/v3");
    assert_eq!(config.websocket_url(), "wss://stream.example.com/ws");
}

#[test]
fn connector_can_start_without_credentials() {
    let config = Fixture::config(&[(ENABLED, "true")])
        .expect("config")
        .expect("enabled");
    assert!(config.credentials().is_empty());
}

#[test]
fn rejects_invalid_credentials() {
    for credentials in [
        "not json",
        r#"[{"account":"","api_key":"key","secret":"secret"}]"#,
        r#"[{"account":"a","api_key":"","secret":"secret"}]"#,
        r#"[{"account":"a","api_key":"key","secret":""}]"#,
        r#"[{"account":"a","api_key":"k1","secret":"s1"},{"account":"a","api_key":"k2","secret":"s2"}]"#,
    ] {
        assert!(
            Fixture::config(&[(ENABLED, "true"), (CREDENTIALS, credentials)]).is_err(),
            "{credentials}"
        );
    }
}

#[test]
fn rejects_invalid_endpoints_and_durations() {
    for (name, value) in [
        (REST_URL, "not a url"),
        (REST_URL, "wss://rest.example.com"),
        (WEBSOCKET_URL, "not a url"),
        (WEBSOCKET_URL, "https://stream.example.com"),
        (REFRESH, "0"),
        (BACKOFF, "0"),
    ] {
        let mut vars = Fixture::enabled();
        vars.push((name, value));
        assert!(Fixture::config(&vars).is_err(), "{name}={value}");
    }
}
