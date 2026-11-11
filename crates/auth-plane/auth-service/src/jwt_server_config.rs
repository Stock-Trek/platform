use crate::{JwtSigningKey, VaultTokenStoreConfig};
use platform_config::env_names::RUST_LOG;
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_sigv4::SigV4Config;
use platform_types::url_scheme::UrlScheme;
use secrecy::SecretString;
use std::{fmt, net::SocketAddr, time::Duration};

const AUTH_SERVICE_BIND_ADDR: &str = "AUTH_SERVICE_BIND_ADDR";
const AUTH_SERVICE_VAULT_ADDRESS: &str = "AUTH_SERVICE_VAULT_ADDRESS";
const AUTH_SERVICE_VAULT_MOUNT: &str = "AUTH_SERVICE_VAULT_MOUNT";
const AUTH_SERVICE_VAULT_TOKEN: &str = "AUTH_SERVICE_VAULT_TOKEN";
const AUTH_SERVICE_JWT_KEY_ID: &str = "AUTH_SERVICE_JWT_KEY_ID";
const AUTH_SERVICE_JWT_PRIVATE_KEY_PEM: &str = "AUTH_SERVICE_JWT_PRIVATE_KEY_PEM";
const VAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct JwtServerConfig {
    log_filter: String,
    bind_address: SocketAddr,
    vault: VaultTokenStoreConfig,
    sigv4: SigV4Config,
    jwt_key_id: String,
    jwt_private_key_pem: SecretString,
}

impl JwtServerConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let vault = VaultTokenStoreConfig::new(
            vars.required_url(AUTH_SERVICE_VAULT_ADDRESS, &UrlScheme::HTTP)?,
            SecretString::from(vars.required(AUTH_SERVICE_VAULT_TOKEN)?),
            vars.required(AUTH_SERVICE_VAULT_MOUNT)?,
            VAULT_TIMEOUT,
        )?;
        let jwt_key_id = vars.required(AUTH_SERVICE_JWT_KEY_ID)?;
        let jwt_private_key_pem =
            SecretString::from(vars.required(AUTH_SERVICE_JWT_PRIVATE_KEY_PEM)?);
        Ok(Self {
            log_filter: vars.required_log_filter(RUST_LOG)?,
            bind_address: vars.required_parsed(AUTH_SERVICE_BIND_ADDR)?,
            vault,
            sigv4: SigV4Config::from_vars(vars)?,
            jwt_key_id,
            jwt_private_key_pem,
        })
    }

    pub fn log_filter(&self) -> &str {
        &self.log_filter
    }

    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    pub fn vault(&self) -> &VaultTokenStoreConfig {
        &self.vault
    }

    pub fn sigv4(&self) -> &SigV4Config {
        &self.sigv4
    }

    pub fn jwt_key_id(&self) -> &str {
        &self.jwt_key_id
    }

    pub fn jwt_signing_key(&self) -> BpResult<JwtSigningKey> {
        use secrecy::ExposeSecret;
        JwtSigningKey::from_pkcs8_pem(&self.jwt_key_id, self.jwt_private_key_pem.expose_secret())
    }
}

impl fmt::Debug for JwtServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwtServerConfig")
            .field("log_filter", &self.log_filter)
            .field("bind_address", &self.bind_address)
            .field("vault", &self.vault)
            .field("sigv4", &self.sigv4)
            .field("jwt_key_id", &self.jwt_key_id)
            .field("jwt_private_key_pem", &"<redacted>")
            .finish()
    }
}
