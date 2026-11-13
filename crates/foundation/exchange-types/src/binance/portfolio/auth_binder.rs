use crate::portfolio::{AuthBinder, AuthError, ConnectRequest, Credential, Signer};
use async_trait::async_trait;

#[derive(Debug, Default, Clone, Copy)]
pub struct BinanceAuthBinder;

impl BinanceAuthBinder {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AuthBinder for BinanceAuthBinder {
    fn prepare_connect(
        &self,
        request: &mut ConnectRequest,
        credential: Option<&Credential>,
        _signer: &dyn Signer,
    ) -> Result<(), AuthError> {
        let credential = credential.ok_or_else(|| {
            AuthError::Bind("binance listen key credential is missing".to_string())
        })?;
        request
            .url
            .path_segments_mut()
            .map_err(|_| AuthError::Bind("binance websocket url cannot be a base url".to_string()))?
            .pop_if_empty()
            .push(&credential.value);
        Ok(())
    }
}
