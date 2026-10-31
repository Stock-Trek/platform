use async_nats::{Client, ConnectOptions};
use platform_errors::{BpError, BpResult, NatsError};
use url::Url;

const DEFAULT_NATS_PORT: u16 = 4222;
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct NatsAddress {
    host: String,
    port: u16,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
}

impl NatsAddress {
    pub fn validate(value: &str) -> BpResult<()> {
        Self::validate_named("NATS_URL", value)
    }

    /// Validates a NATS address, reporting errors against `name` so callers can
    /// use the environment variable that actually held the value.
    pub fn validate_named(name: &str, value: &str) -> BpResult<()> {
        Self::parse_named(name, value).map(|_| ())
    }

    pub(crate) fn parse(value: &str) -> BpResult<Self> {
        Self::parse_named("NATS_URL", value)
    }

    fn parse_named(name: &str, value: &str) -> BpResult<Self> {
        let url = Url::parse(value).map_err(|error| BpError::config_invalid_source(name, error))?;
        if url.scheme() != "nats" {
            return Err(BpError::config_invalid(
                name,
                format!("must use the nats scheme, got {}", url.scheme()),
            ));
        }
        let host = url
            .host_str()
            .ok_or_else(|| BpError::config_invalid(name, "missing host"))?;
        Ok(Self {
            host: host.to_string(),
            port: url.port().unwrap_or(DEFAULT_NATS_PORT),
            username: (!url.username().is_empty()).then(|| url.username().to_string()),
            password: url.password().map(str::to_string),
        })
    }

    pub(crate) async fn connect(&self, name: &str) -> BpResult<Client> {
        let mut options = ConnectOptions::new()
            .name(name)
            .connection_timeout(CONNECT_TIMEOUT)
            .request_timeout(Some(CONNECT_TIMEOUT))
            .max_reconnects(None);
        if let Some(username) = &self.username {
            options = options
                .user_and_password(username.clone(), self.password.clone().unwrap_or_default());
        }
        options.connect(self.server_url()).await.map_err(|error| {
            BpError::Nats(NatsError::protocol(format!(
                "failed to connect to NATS at {}: {error}",
                self.server_url()
            )))
        })
    }

    fn server_url(&self) -> String {
        let host = self.host.trim_matches(['[', ']']);
        if host.contains(':') {
            format!("nats://[{host}]:{}", self.port)
        } else {
            format!("nats://{host}:{}", self.port)
        }
    }
}

#[cfg(test)]
mod tests;
