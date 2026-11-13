use crate::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        account_status::{BinanceAccountStatusRequest, BinanceAccountStatusResponse},
        klines::{BinanceKlinesRequest, BinanceKlinesResponse},
        portfolio::{
            event_parser::EventParser,
            listen_key::{BinanceListenKeyRequest, BinanceListenKeyResponse},
        },
        response::{BinanceResponse, BinanceResponsePayload},
        time::{BinanceTimeRequest, BinanceTimeResponse},
        urls::BinanceUrls,
    },
    encode::ByteEncoder,
    encrypt::EncryptionAlgorithm,
    exchange::ETExchange,
    portfolio::{AccountEvent, AccountSnapshot, BalanceParser},
    rate_limited::{RateLimit, RateLimitRestriction},
    signer::Signer,
    urls::Urls,
};
use chrono::{TimeZone, Utc};
use platform_errors::{BuildError, BuildResult, ExchangeError};
use platform_types::{DurationNanoseconds, UsageCount};
use std::{collections::HashMap, time::Duration};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};

#[derive(Clone, Copy)]
pub struct BinanceExchange;

impl ETExchange for BinanceExchange {
    type Credentials = ApiKeyCredentials;
    type ListenKeyRequest = BinanceListenKeyRequest;
    type SnapshotRequest = BinanceAccountStatusRequest;
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

    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn signer(credentials: &ApiKeyCredentials) -> BuildResult<Signer> {
        Self::signer_hmac_sha256(credentials.clone())
    }

    fn create_listen_key_request() -> Self::ListenKeyRequest {
        BinanceListenKeyRequest::create()
    }

    fn keepalive_listen_key_request(listen_key: &str) -> Self::ListenKeyRequest {
        BinanceListenKeyRequest::keepalive(listen_key)
    }

    fn close_listen_key_request(listen_key: &str) -> Self::ListenKeyRequest {
        BinanceListenKeyRequest::close(listen_key)
    }

    fn listen_key(response: BinanceResponse<BinanceListenKeyResponse>) -> BuildResult<String> {
        Self::success(response)?.require_listen_key()
    }

    fn listen_key_ack(response: BinanceResponse<BinanceListenKeyResponse>) -> BuildResult<()> {
        Self::success(response).map(|_| ())
    }

    fn account_snapshot_request() -> Self::SnapshotRequest {
        BinanceAccountStatusRequest::new()
    }

    fn account_snapshot(
        response: BinanceResponse<BinanceAccountStatusResponse>,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BuildResult<AccountSnapshot> {
        let account = Self::success(response)?;
        let occurred_at = Utc
            .timestamp_millis_opt(account.updateTime)
            .single()
            .ok_or_else(|| {
                BuildError::Exchange(ExchangeError::BadResponse(
                    "invalid account timestamp".to_string(),
                ))
            })?;
        let balances = BalanceParser::parse(
            account.balances.into_iter().map(|balance| {
                (
                    balance.asset,
                    balance.free.to_string(),
                    balance.locked.to_string(),
                )
            }),
            mapping,
        )?;
        Ok(AccountSnapshot::new(balances, occurred_at))
    }

    fn account_event(
        value: &serde_json::Value,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BuildResult<Option<AccountEvent>> {
        EventParser::parse(value, mapping)
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

    fn success<R>(response: BinanceResponse<R>) -> BuildResult<R> {
        match response.payload {
            BinanceResponsePayload::Success(payload) => Ok(payload),
            BinanceResponsePayload::Failure(error) => Err(BuildError::Exchange(
                ExchangeError::BadResponse(error.to_string()),
            )),
        }
    }
}
