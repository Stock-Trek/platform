use crate::{
    portfolio::{AuthBinder, AuthError, Credential},
    signer::Signer,
};
use async_trait::async_trait;
use url::Url;

#[derive(Debug, Default, Clone, Copy)]
pub struct BinanceAuthBinder;

impl BinanceAuthBinder {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AuthBinder for BinanceAuthBinder {
    fn websocket_url(
        &self,
        url: &str,
        credential: Option<&Credential>,
        _signer: &Signer,
    ) -> Result<String, AuthError> {
        let credential = credential.ok_or_else(|| {
            AuthError::Bind("binance listen key credential is missing".to_string())
        })?;
        let mut url = Url::parse(url)
            .map_err(|error| AuthError::Bind(format!("invalid binance websocket url: {error}")))?;
        url.path_segments_mut()
            .map_err(|_| AuthError::Bind("binance websocket url cannot be a base url".to_string()))?
            .pop_if_empty()
            .push(&credential.value);
        Ok(url.into())
    }
}
