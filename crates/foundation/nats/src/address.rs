use platform_errors::{BpError, BpResult};
use tokio::net::TcpStream;
use url::Url;

const DEFAULT_NATS_PORT: u16 = 4222;

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

    pub(crate) async fn tcp_connect(&self) -> BpResult<TcpStream> {
        Ok(TcpStream::connect((self.host.as_str(), self.port)).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nats_url_with_auth_and_default_port() {
        let address = NatsAddress::parse("nats://user:secret@localhost").unwrap();

        assert_eq!(address.host, "localhost");
        assert_eq!(address.port, DEFAULT_NATS_PORT);
        assert_eq!(address.username.as_deref(), Some("user"));
        assert_eq!(address.password.as_deref(), Some("secret"));
    }

    #[test]
    fn rejects_non_nats_urls() {
        assert!(NatsAddress::parse("http://localhost:4222").is_err());
    }
}
