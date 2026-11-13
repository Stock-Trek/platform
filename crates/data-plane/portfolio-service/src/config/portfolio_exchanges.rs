use platform_config::EnvVars;
use platform_errors::BuildResult;
use platform_exchange_gateway::{ExchangePortfolio, PortfolioExchangeConfig};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    binance::{exchange::BinanceExchange, portfolio::config::BinancePortfolioConfig},
};
use platform_type_bridge::BinanceConverter;
use secrecy::{ExposeSecret, SecretString};
use std::{collections::BTreeSet, sync::Arc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub struct PortfolioExchanges;

impl PortfolioExchanges {
    pub fn from_vars(
        vars: &EnvVars,
        selected: Option<&BTreeSet<CexId>>,
    ) -> BuildResult<Vec<PortfolioExchangeConfig>> {
        let mut exchanges = Vec::new();
        if selected.is_none_or(|selected| selected.contains(&CexId::Binance))
            && let Some(binance) = BinancePortfolioConfig::from_vars(vars)?
        {
            let credentials = binance
                .credentials()
                .iter()
                .map(|credential| {
                    (
                        AccountId::new(credential.account()),
                        ApiKeyCredentials {
                            api_key: credential.api_key().to_string(),
                            secret: SecretString::from(
                                credential.secret().expose_secret().to_string(),
                            ),
                        },
                    )
                })
                .collect::<Vec<_>>();
            let exchange = ExchangePortfolio::new(
                BinanceExchange,
                binance.rest_url(),
                binance.websocket_url(),
                binance.listen_key_refresh(),
                binance.reconnect_backoff(),
                BinanceConverter::ticker_mapping_static(),
            );
            exchanges.push(PortfolioExchangeConfig::with_credentials(
                Arc::new(exchange),
                credentials,
            ));
        }
        Ok(exchanges)
    }
}
