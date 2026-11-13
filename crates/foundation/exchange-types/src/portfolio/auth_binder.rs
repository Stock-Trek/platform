use crate::{
    portfolio::{AuthError, Credential},
    signer::Signer,
};
use async_trait::async_trait;
use iris::{Client, Config};

pub type PortfolioIrisClient = Client<serde_json::Value, serde_json::Value>;

#[async_trait]
pub trait AuthBinder: Send + Sync {
    fn websocket_url(
        &self,
        url: &str,
        _credential: Option<&Credential>,
        _signer: &Signer,
    ) -> Result<String, AuthError> {
        Ok(url.to_string())
    }

    fn configure(
        &self,
        config: Config,
        _credential: Option<&Credential>,
        _signer: &Signer,
    ) -> Result<Config, AuthError> {
        Ok(config)
    }

    async fn bind(
        &self,
        _client: &PortfolioIrisClient,
        _credential: Option<&Credential>,
        _signer: &Signer,
    ) -> Result<(), AuthError> {
        Ok(())
    }
}
