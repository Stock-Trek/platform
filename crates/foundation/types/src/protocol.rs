use serde::{Deserialize, Serialize};
use strum::Display;
use url::Url;

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Protocol {
    Http,
    Websocket,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("Cannot use {name}={value}. invalid URL: {source}")]
    InvalidUrl {
        name: String,
        value: String,
        #[source]
        source: url::ParseError,
    },
    #[error(
        "Cannot use {name}={value}. URL scheme must be one of {}, got {scheme}",
        allowed.join(", ")
    )]
    UnsupportedScheme {
        name: String,
        value: String,
        allowed: &'static [&'static str],
        scheme: String,
    },
}

impl Protocol {
    fn allowed_schemes(self) -> &'static [&'static str] {
        match self {
            Self::Http => &["http", "https"],
            Self::Websocket => &["ws", "wss"],
        }
    }

    pub fn validate_url(self, name: &str, value: &str) -> Result<(), ProtocolError> {
        let url = Url::parse(value).map_err(|source| ProtocolError::InvalidUrl {
            name: name.into(),
            value: value.into(),
            source,
        })?;
        let allowed = self.allowed_schemes();
        if !allowed.contains(&url.scheme()) {
            return Err(ProtocolError::UnsupportedScheme {
                name: name.into(),
                value: value.into(),
                allowed,
                scheme: url.scheme().into(),
            });
        }
        Ok(())
    }
}
