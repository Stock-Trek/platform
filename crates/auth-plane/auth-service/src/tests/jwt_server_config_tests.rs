use crate::JwtServerConfig;
use p256::{
    SecretKey,
    elliptic_curve::Generate,
    pkcs8::{EncodePrivateKey, LineEnding},
};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use std::collections::HashMap;

const BIND_ADDRESS: &str = "AUTH_SERVICE_BIND_ADDR";
const VAULT_ADDRESS: &str = "AUTH_SERVICE_VAULT_ADDRESS";
const VAULT_MOUNT: &str = "AUTH_SERVICE_VAULT_MOUNT";
const VAULT_TOKEN: &str = "AUTH_SERVICE_VAULT_TOKEN";
const LOG_FILTER: &str = "RUST_LOG";
const JWT_KEY_ID: &str = "AUTH_SERVICE_JWT_KEY_ID";
const JWT_PRIVATE_KEY_PEM: &str = "AUTH_SERVICE_JWT_PRIVATE_KEY_PEM";

struct Fixture;

impl Fixture {
    fn key_pem() -> String {
        let secret = SecretKey::try_generate().unwrap();
        secret.to_pkcs8_pem(LineEnding::LF).unwrap().to_string()
    }

    fn vars() -> HashMap<String, String> {
        HashMap::from([
            (LOG_FILTER.to_string(), "info".to_string()),
            (BIND_ADDRESS.to_string(), "127.0.0.1:8090".to_string()),
            (
                VAULT_ADDRESS.to_string(),
                "https://vault.example.com".to_string(),
            ),
            (VAULT_MOUNT.to_string(), "platform-tokens".to_string()),
            (VAULT_TOKEN.to_string(), "vault-token".to_string()),
            (JWT_KEY_ID.to_string(), "auth-service".to_string()),
            (JWT_PRIVATE_KEY_PEM.to_string(), Self::key_pem()),
        ])
    }

    fn config(extra: &[(&str, &str)]) -> BpResult<JwtServerConfig> {
        let mut vars = Self::vars();
        for (name, value) in extra {
            vars.insert((*name).to_string(), (*value).to_string());
        }
        JwtServerConfig::from_vars(&EnvVars::from_map(vars))
    }
}

#[test]
fn imports_a_pkcs8_private_key_from_the_environment() {
    let config = Fixture::config(&[(JWT_KEY_ID, "rotation-2026")]).unwrap();
    let signer = config.jwt_signing_key().unwrap();

    assert_eq!(config.jwt_key_id(), "rotation-2026");
    assert_eq!(signer.key_id(), "rotation-2026");
}

#[test]
fn accepts_literal_newline_sequences_in_the_private_key() {
    let pem = Fixture::key_pem().replace('\n', "\\n");
    let config = Fixture::config(&[(JWT_PRIVATE_KEY_PEM, &pem)]).unwrap();

    assert!(config.jwt_signing_key().is_ok());
}

#[test]
fn rejects_a_malformed_private_key() {
    let config = Fixture::config(&[(JWT_PRIVATE_KEY_PEM, "not a pem key")]).unwrap();

    assert!(config.jwt_signing_key().is_err());
}
