use super::config_tests::Fixture;
use secrecy::ExposeSecret;

const ENABLED: &str = "PORTFOLIO_BINANCE_ENABLED";
const TESTNET: &str = "PORTFOLIO_BINANCE_TESTNET";
const CREDENTIALS: &str = "PORTFOLIO_BINANCE_CREDENTIALS";
const MAX_AGE: &str = "PORTFOLIO_MAX_UPDATE_AGE_SECS";
const STALE_POLICY: &str = "PORTFOLIO_STALE_ORDER_POLICY";

fn vars(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
    let mut vars = Fixture::all();
    vars.extend_from_slice(extra);
    vars
}

#[test]
fn parses_connector_credentials_and_defaults() {
    let config = Fixture::config(&vars(&[
        (ENABLED, "true"),
        (
            CREDENTIALS,
            r#"[{"account":"account-1","api_key":"key-1","secret":"secret-1"}]"#,
        ),
    ]))
    .expect("config");

    let binance = config.binance();
    assert!(binance.enabled());
    assert_eq!(binance.rest_url(), "https://api.binance.com/api/v3");
    assert_eq!(binance.websocket_url(), "wss://stream.binance.com:9443/ws");
    assert_eq!(binance.credentials()[0].account(), "account-1");
    assert_eq!(binance.credentials()[0].api_key(), "key-1");
    assert_eq!(
        binance.credentials()[0].secret().expose_secret(),
        "secret-1"
    );
}

#[test]
fn testnet_selects_testnet_endpoints() {
    let config = Fixture::config(&vars(&[
        (ENABLED, "true"),
        (TESTNET, "true"),
        (
            CREDENTIALS,
            r#"[{"account":"account-1","api_key":"key-1","secret":"secret-1"}]"#,
        ),
    ]))
    .expect("config");

    assert_eq!(
        config.binance().rest_url(),
        "https://testnet.binance.vision/api/v3"
    );
    assert_eq!(
        config.binance().websocket_url(),
        "wss://stream.testnet.binance.vision/ws"
    );
}

#[test]
fn enabling_the_connector_requires_credentials() {
    assert!(Fixture::config(&vars(&[(ENABLED, "true")])).is_err());
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
            Fixture::config(&vars(&[(ENABLED, "true"), (CREDENTIALS, credentials)])).is_err(),
            "{credentials}"
        );
    }
}

#[test]
fn parses_freshness_overrides_and_rejects_invalid_values() {
    let config =
        Fixture::config(&vars(&[(MAX_AGE, "30"), (STALE_POLICY, "reject")])).expect("config");
    assert_eq!(
        config.freshness().policy().max_age(),
        chrono::Duration::seconds(30)
    );
    assert_eq!(
        config.freshness().policy().stale_order_policy(),
        crate::StaleOrderPolicy::Reject
    );
    assert!(Fixture::config(&vars(&[(MAX_AGE, "0")])).is_err());
    assert!(Fixture::config(&vars(&[(STALE_POLICY, "sometimes")])).is_err());
}
