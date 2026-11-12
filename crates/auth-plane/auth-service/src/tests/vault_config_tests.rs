use crate::VaultTokenStoreConfig;
use platform_errors::{BuildError, BuildResult, ConfigError};
use secrecy::{ExposeSecret, SecretString};
use std::time::Duration;

struct VaultConfigFixture;

impl VaultConfigFixture {
    fn config(address: &str, mount: &str) -> BuildResult<VaultTokenStoreConfig> {
        VaultTokenStoreConfig::new(
            address,
            SecretString::from("vault-token"),
            mount,
            Duration::from_secs(10),
        )
    }

    fn assert_invalid(error: BuildError, expected: &str) {
        match error {
            BuildError::Config(ConfigError::Empty { name })
            | BuildError::Config(ConfigError::Invalid { name, .. }) => {
                assert_eq!(name, expected)
            }
            other => panic!("expected {expected} validation error, got {other:?}"),
        }
    }
}

#[test]
fn vault_config_rejects_invalid_addresses_and_mounts() {
    VaultConfigFixture::assert_invalid(
        VaultConfigFixture::config("not a url", "platform-tokens").unwrap_err(),
        "vault_address",
    );
    VaultConfigFixture::assert_invalid(
        VaultConfigFixture::config("https://vault.example.com", "../sys").unwrap_err(),
        "vault_mount",
    );
}

#[test]
fn vault_config_rejects_empty_fields() {
    for (address, mount, expected) in [
        ("", "platform-tokens", "vault_address"),
        ("https://vault.example.com", "", "vault_mount"),
    ] {
        VaultConfigFixture::assert_invalid(
            VaultConfigFixture::config(address, mount).unwrap_err(),
            expected,
        );
    }
}

#[test]
fn vault_config_rejects_a_zero_timeout() {
    let error = VaultTokenStoreConfig::new(
        "https://vault.example.com",
        SecretString::from("vault-token"),
        "platform-tokens",
        Duration::ZERO,
    )
    .unwrap_err();

    VaultConfigFixture::assert_invalid(error, "vault_timeout");
}

#[test]
fn vault_config_normalizes_surrounding_whitespace() {
    let config = VaultTokenStoreConfig::new(
        " https://vault.example.com ",
        SecretString::from(" vault-token "),
        " platform-tokens ",
        Duration::from_secs(10),
    )
    .unwrap();

    assert_eq!(config.address(), "https://vault.example.com");
    assert_eq!(config.token().expose_secret(), "vault-token");
    assert_eq!(config.mount(), "platform-tokens");
}

#[test]
fn vault_config_accepts_string_credentials() {
    let config = VaultTokenStoreConfig::new(
        "https://vault.example.com",
        "vault-token",
        "platform-tokens",
        Duration::from_secs(10),
    )
    .unwrap();

    assert_eq!(config.token().expose_secret(), "vault-token");
}

#[test]
fn vault_config_rejects_an_invalid_token_header() {
    let error = VaultTokenStoreConfig::new(
        "https://vault.example.com",
        SecretString::from("token\ninvalid"),
        "platform-tokens",
        Duration::from_secs(10),
    )
    .unwrap_err();

    VaultConfigFixture::assert_invalid(error, "vault_token");
}

#[test]
fn vault_config_rejects_embedded_credentials() {
    for address in [
        "https://user:pass@vault.example.com",
        "https://user@vault.example.com",
    ] {
        VaultConfigFixture::assert_invalid(
            VaultConfigFixture::config(address, "tokens").unwrap_err(),
            "vault_address",
        );
    }
}

#[test]
fn vault_config_rejects_addresses_with_queries_or_fragments() {
    for address in [
        "https://vault.example.com?trace=1",
        "https://vault.example.com#section",
    ] {
        VaultConfigFixture::assert_invalid(
            VaultConfigFixture::config(address, "tokens").unwrap_err(),
            "vault_address",
        );
    }
}
