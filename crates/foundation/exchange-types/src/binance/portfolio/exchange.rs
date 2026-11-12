use super::{BinanceListenKeyRequest, BinanceListenKeyResponse, EventParser};
use crate::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        account_status::{BinanceAccountStatusRequest, BinanceAccountStatusResponse},
        exchange::BinanceExchange,
        response::{BinanceResponse, BinanceResponsePayload},
    },
    portfolio::{AccountEvent, AccountSnapshot, BalanceParser, PortfolioExchange},
    signer::Signer,
};
use chrono::{TimeZone, Utc};
use platform_errors::{BpError, BpResult, ExchangeError};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};

impl PortfolioExchange for BinanceExchange {
    type ListenKeyRequest = BinanceListenKeyRequest;
    type SnapshotRequest = BinanceAccountStatusRequest;

    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn signer(credentials: &ApiKeyCredentials) -> BpResult<Signer> {
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

    fn listen_key(response: BinanceResponse<BinanceListenKeyResponse>) -> BpResult<String> {
        Self::success(response)?.require_listen_key()
    }

    fn listen_key_ack(response: BinanceResponse<BinanceListenKeyResponse>) -> BpResult<()> {
        Self::success(response).map(|_| ())
    }

    fn account_snapshot_request() -> Self::SnapshotRequest {
        BinanceAccountStatusRequest::new()
    }

    fn account_snapshot(
        response: BinanceResponse<BinanceAccountStatusResponse>,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BpResult<AccountSnapshot> {
        let account = Self::success(response)?;
        let occurred_at = Utc
            .timestamp_millis_opt(account.updateTime)
            .single()
            .ok_or_else(|| {
                BpError::Exchange(ExchangeError::BadResponse(
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
    ) -> BpResult<Option<AccountEvent>> {
        EventParser::parse(value, mapping)
    }
}

impl BinanceExchange {
    fn success<R>(response: BinanceResponse<R>) -> BpResult<R> {
        match response.payload {
            BinanceResponsePayload::Success(payload) => Ok(payload),
            BinanceResponsePayload::Failure(error) => Err(BpError::Exchange(
                ExchangeError::BadResponse(error.to_string()),
            )),
        }
    }
}
