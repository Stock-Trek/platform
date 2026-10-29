use crate::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        klines::{BinanceKlinesRequest, BinanceKlinesResponse},
        response::BinanceResponse,
        time::{BinanceTimeRequest, BinanceTimeResponse},
        urls::BinanceUrls,
    },
    encode::ByteEncoder,
    encrypt::EncryptionAlgorithm,
    exchange::ETExchange,
    rate_limited::{RateLimit, RateLimitRestriction},
    signer::Signer,
    urls::Urls,
};
use platform_errors::BpResult;
use platform_types::new_types::{DurationNanoseconds, UsageCount};
use std::{collections::HashMap, time::Duration};

pub struct BinanceExchange;

impl ETExchange for BinanceExchange {
    type Credentials = ApiKeyCredentials;
    type KlinesRequestHttp = BinanceKlinesRequest;
    type KlinesResponseHttp = BinanceResponse<BinanceKlinesResponse>;
    type KlinesRequestWebsocket = BinanceKlinesRequest;
    type KlinesResponseWebsocket = BinanceResponse<BinanceKlinesResponse>;
    type ServerTimeRequestHttp = BinanceTimeRequest;
    type ServerTimeResponseHttp = BinanceResponse<BinanceTimeResponse>;
    type ServerTimeRequestWebsocket = BinanceTimeRequest;
    type ServerTimeResponseWebsocket = BinanceResponse<BinanceTimeResponse>;

    fn name(&self) -> &'static str {
        "Binance"
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
    fn server_time_request_http(&self) -> Self::ServerTimeRequestHttp {
        BinanceTimeRequest {}
    }
    fn server_time_request_websocket(&self) -> Self::ServerTimeRequestWebsocket {
        BinanceTimeRequest {}
    }
    fn urls(&self) -> &impl Urls {
        &BinanceUrls
    }
}

impl BinanceExchange {
    pub fn signer_hmac_sha256(credentials: ApiKeyCredentials) -> BpResult<Signer> {
        Signer::try_new(
            credentials,
            EncryptionAlgorithm::HmacSha256,
            ByteEncoder::HexLower,
        )
    }
    pub fn signer_rsa_sha256(credentials: ApiKeyCredentials) -> BpResult<Signer> {
        Signer::try_new(
            credentials,
            EncryptionAlgorithm::RsaSha256,
            ByteEncoder::Base64,
        )
    }
    pub fn signer_ed25519(credentials: ApiKeyCredentials) -> BpResult<Signer> {
        Signer::try_new(
            credentials,
            EncryptionAlgorithm::Ed25519,
            ByteEncoder::Base64,
        )
    }
}
