use crate::portfolio::AuthError;
use async_trait::async_trait;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub url: Url,
    pub headers: Vec<(String, String)>,
}

impl ConnectRequest {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            headers: Vec::new(),
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn set_header(&mut self, name: impl Into<String>, value: impl Into<String>) {
        let name = name.into();
        let value = value.into();
        match self.headers.iter_mut().find(|(key, _)| key == &name) {
            Some((_, existing)) => *existing = value,
            None => self.headers.push((name, value)),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IncomingFrame {
    pub text: Option<String>,
    pub binary: Option<Vec<u8>>,
}

impl IncomingFrame {
    pub fn text(value: impl Into<String>) -> Self {
        Self {
            text: Some(value.into()),
            binary: None,
        }
    }

    pub fn binary(value: impl Into<Vec<u8>>) -> Self {
        Self {
            text: None,
            binary: Some(value.into()),
        }
    }
}

#[async_trait]
pub trait WsConnection: Send + Sync {
    async fn send_text(&self, payload: &str) -> Result<(), AuthError>;
    async fn send_binary(&self, payload: &[u8]) -> Result<(), AuthError>;
}
