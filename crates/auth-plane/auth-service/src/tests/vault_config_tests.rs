use crate::VaultTokenStoreConfig;
use secrecy::{ExposeSecret, SecretString};
use std::time::Duration;

struct VaultConfigFixture;

impl VaultConfigFixture {
    fn config(address: &str, mount: &str) -> VaultTokenStoreConfig {
        VaultTokenStoreConfig::new(
            address,
            SecretString::from("vault-token"),
            mount,
            Duration::from_secs(10),
        )
    }
}

#[test]
fn vault_config_records_the_timeout() {
    let config = VaultConfigFixture::config("https://vault.example.com", "platform-tokens");

    assert_eq!(config.timeout(), Duration::from_secs(10));
    assert_eq!(config.address(), "https://vault.example.com");
    assert_eq!(config.mount(), "platform-tokens");
    assert!(config.validate().is_ok());
}

#[test]
fn vault_config_rejects_invalid_addresses_and_mounts() {
    let invalid_address = VaultConfigFixture::config("not a url", "platform-tokens");
    assert!(invalid_address.validate().is_err());

    let invalid_mount = VaultConfigFixture::config("https://vault.example.com", "../sys");
    assert!(invalid_mount.validate().is_err());
}

#[test]
fn vault_config_rejects_a_zero_timeout() {
    let config = VaultTokenStoreConfig::new(
        "https://vault.example.com",
        SecretString::from("vault-token"),
        "platform-tokens",
        Duration::ZERO,
    );

    assert!(config.validate().is_err());
}

#[test]
fn vault_config_normalizes_surrounding_whitespace() {
    let config = VaultTokenStoreConfig::new(
        " https://vault.example.com ",
        SecretString::from(" vault-token "),
        " platform-tokens ",
        Duration::from_secs(10),
    );

    assert_eq!(config.address(), "https://vault.example.com");
    assert_eq!(config.token().expose_secret(), "vault-token");
    assert_eq!(config.mount(), "platform-tokens");
    assert!(config.validate().is_ok());
}

#[test]
fn vault_config_rejects_an_invalid_token_header() {
    let config = VaultTokenStoreConfig::new(
        "https://vault.example.com",
        SecretString::from("token\ninvalid"),
        "platform-tokens",
        Duration::from_secs(10),
    );

    assert!(config.validate().is_err());
}

#[test]
fn vault_config_rejects_embedded_credentials() {
    let userinfo = VaultConfigFixture::config("https://user:pass@vault.example.com", "tokens");
    let username = VaultConfigFixture::config("https://user@vault.example.com", "tokens");

    assert!(userinfo.validate().is_err());
    assert!(username.validate().is_err());
}

#[test]
fn vault_config_rejects_addresses_with_queries_or_fragments() {
    let query = VaultConfigFixture::config("https://vault.example.com?trace=1", "tokens");
    let fragment = VaultConfigFixture::config("https://vault.example.com#section", "tokens");

    assert!(query.validate().is_err());
    assert!(fragment.validate().is_err());
}
