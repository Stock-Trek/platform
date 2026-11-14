use async_nats::{Client, ConnectOptions};
use platform_errors::{NatsError, PlatformError, PlatformResult};
use platform_types::UrlScheme;
use url::Url;

pub const DEFAULT_NATS_PORT: u16 = 4222;
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct NatsAddress {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl NatsAddress {
    /// Validates a NATS address, reporting errors against `name` so callers can
    /// use the environment variable that actually held the value.
    pub fn validate_named(name: &str, value: &str) -> PlatformResult<()> {
        Self::parse_named(name, value).map(|_| ())
    }

    pub fn parse(value: &str) -> PlatformResult<Self> {
        Self::parse_named("NATS_URL", value)
    }

    fn parse_named(name: &str, value: &str) -> PlatformResult<Self> {
        let url =
            Url::parse(value).map_err(|error| PlatformError::config_invalid_source(name, error))?;
        if !UrlScheme::from_url(&url).is_some_and(UrlScheme::is_nats) {
            return Err(PlatformError::config_invalid(
                name,
                format!("must use the nats scheme, got {}", url.scheme()),
            ));
        }
        let host = url
            .host_str()
            .ok_or_else(|| PlatformError::config_invalid(name, "missing host"))?;
        Ok(Self {
            host: host.to_string(),
            port: url.port().unwrap_or(DEFAULT_NATS_PORT),
            username: (!url.username().is_empty()).then(|| url.username().to_string()),
            password: url.password().map(str::to_string),
        })
    }

    pub async fn connect(&self, name: &str) -> PlatformResult<Client> {
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
            PlatformError::Nats(NatsError::protocol(format!(
                "failed to connect to NATS at {}: {error}",
                self.server_url()
            )))
        })
    }

    pub fn server_url(&self) -> String {
        let host = self.host.trim_matches(['[', ']']);
        if host.contains(':') {
            format!("nats://[{host}]:{}", self.port)
        } else {
            format!("nats://{host}:{}", self.port)
        }
    }
}
