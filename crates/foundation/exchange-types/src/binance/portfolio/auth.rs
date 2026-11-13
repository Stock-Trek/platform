use crate::{
    binance::{
        exchange::BinanceExchange,
        portfolio::{
            auth_binder::BinanceAuthBinder, credential_client::BinanceListenKeyClient,
            credential_provider::BinanceCredentialProvider,
        },
    },
    exchange::ETExchange,
    exchange_portfolio::PortfolioAuthContext,
    portfolio::PortfolioAuth,
};
use platform_errors::BuildResult;

impl BinanceExchange {
    pub fn build_portfolio_auth(
        &self,
        context: PortfolioAuthContext,
    ) -> BuildResult<PortfolioAuth> {
        let signer = Self::signer(context.credentials())?;
        let client = BinanceListenKeyClient::new(context.http(), context.credentials().clone());
        let provider = BinanceCredentialProvider::new(client);
        let binder = BinanceAuthBinder::new();
        let lifecycle = Self::portfolio_session_lifecycle(context.refresh_interval());
        Ok(PortfolioAuth::new(provider, binder, lifecycle, signer))
    }
}
