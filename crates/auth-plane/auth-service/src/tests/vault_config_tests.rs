use crate::VaultTokenStoreConfig;
use secrecy::SecretString;

struct VaultConfigFixture;

impl VaultConfigFixture {
    fn config(address: &str, mount: &str) -> VaultTokenStoreConfig {
        VaultTokenStoreConfig::new(address, SecretString::from("vault-token"), mount)
    }
}

#[test]
fn vault_config_defaults_to_a_ten_second_timeout() {
    let config = VaultConfigFixture::config("https://vault.example.com", "platform-tokens");

    assert_eq!(config.timeout().as_secs(), 10);
    assert_eq!(config.address(), "https://vault.example.com");
    assert_eq!(config.mount(), "platform-tokens");
}

#[test]
fn vault_config_rejects_invalid_addresses_and_mounts() {
    let invalid_address = VaultConfigFixture::config("not a url", "platform-tokens");
    assert!(invalid_address.validate().is_err());

    let invalid_mount = VaultConfigFixture::config("https://vault.example.com", "../sys");
    assert!(invalid_mount.validate().is_err());
}
