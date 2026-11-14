use crate::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        klines::{BinanceKlinesRequest, BinanceKlinesResponse},
        response::{BinanceResponse, BinanceResponsePayload},
        time::{BinanceTimeRequest, BinanceTimeResponse},
        urls::BinanceUrls,
    },
    encode::ByteEncoder,
    encrypt::EncryptionAlgorithm,
    exchange::ETExchange,
    exchange_klines::{ETExchangeKlinesHttp, ETExchangeKlinesWebsocket},
    exchange_portfolio::{ETExchangePortfolio, PortfolioAuthContext},
    exchange_server_time::{ETExchangeServerTimeHttp, ETExchangeServerTimeWebsocket},
    portfolio::PortfolioAuth,
    rate_limited::{RateLimit, RateLimitRestriction},
    signer::Signer,
    urls::Urls,
};
use platform_errors::{BuildError, BuildResult, ExchangeError};
use platform_types::{DurationNanoseconds, UsageCount};
use std::{collections::HashMap, time::Duration};
use stock_trek_types::cex::cex_id::CexId;

#[derive(Clone, Copy)]
pub struct BinanceExchange;

impl ETExchange for BinanceExchange {
    type Credentials = ApiKeyCredentials;

    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount> {
        let mut map = HashMap::new();
        map.insert(
            RateLimit {
                restriction: RateLimitRestriction::Weight,
                interval_nanos: DurationNanoseconds(Duration::from_mins(1).as_nanos() as i64),
            },
            UsageCount(6_000),
        );
        map.insert(
            RateLimit {
                restriction: RateLimitRestriction::OrderCount,
                interval_nanos: DurationNanoseconds(Duration::from_secs(10).as_nanos() as i64),
            },
            UsageCount(50),
        );
        map.insert(
            RateLimit {
                restriction: RateLimitRestriction::OrderCount,
                interval_nanos: DurationNanoseconds(Duration::from_hours(24).as_nanos() as i64),
            },
            UsageCount(160_000),
        );
        map
    }

    fn signer(credentials: &ApiKeyCredentials) -> BuildResult<Signer> {
        Self::signer_hmac_sha256(credentials.clone())
    }

    fn urls(&self) -> &impl Urls {
        &BinanceUrls
    }
}

impl BinanceExchange {
    pub fn signer_hmac_sha256(credentials: ApiKeyCredentials) -> BuildResult<Signer> {
        Signer::try_new(
            credentials,
            EncryptionAlgorithm::HmacSha256,
            ByteEncoder::HexLower,
        )
    }

    pub fn signer_rsa_sha256(credentials: ApiKeyCredentials) -> BuildResult<Signer> {
        Signer::try_new(
            credentials,
            EncryptionAlgorithm::RsaSha256,
            ByteEncoder::Base64,
        )
    }

    pub fn signer_ed25519(credentials: ApiKeyCredentials) -> BuildResult<Signer> {
        Signer::try_new(
            credentials,
            EncryptionAlgorithm::Ed25519,
            ByteEncoder::Base64,
        )
    }

    pub fn success<R>(response: BinanceResponse<R>) -> BuildResult<R> {
        match response.payload {
            BinanceResponsePayload::Success(payload) => Ok(payload),
            BinanceResponsePayload::Failure(error) => Err(BuildError::Exchange(
                ExchangeError::BadResponse(error.to_string()),
            )),
        }
    }
}

impl ETExchangeServerTimeHttp for BinanceExchange {
    type ServerTimeRequestHttp = BinanceTimeRequest;
    type ServerTimeResponseHttp = BinanceResponse<BinanceTimeResponse>;

    fn server_time_request_http(&self) -> Self::ServerTimeRequestHttp {
        BinanceTimeRequest {}
    }
}

impl ETExchangeServerTimeWebsocket for BinanceExchange {
    type ServerTimeRequestWebsocket = BinanceTimeRequest;
    type ServerTimeResponseWebsocket = BinanceResponse<BinanceTimeResponse>;

    fn server_time_request_websocket(&self) -> Self::ServerTimeRequestWebsocket {
        BinanceTimeRequest {}
    }
}

impl ETExchangeKlinesHttp for BinanceExchange {
    type KlinesRequestHttp = BinanceKlinesRequest;
    type KlinesResponseHttp = BinanceResponse<BinanceKlinesResponse>;
}

impl ETExchangeKlinesWebsocket for BinanceExchange {
    type KlinesRequestWebsocket = BinanceKlinesRequest;
    type KlinesResponseWebsocket = BinanceResponse<BinanceKlinesResponse>;
}

impl ETExchangePortfolio for BinanceExchange {
    fn portfolio_auth(&self, context: PortfolioAuthContext) -> BuildResult<PortfolioAuth> {
        self.build_portfolio_auth(context)
    }
}
