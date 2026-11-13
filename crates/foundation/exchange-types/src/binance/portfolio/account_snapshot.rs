use crate::{
    binance::{
        account_status::{BinanceAccountStatusRequest, BinanceAccountStatusResponse},
        exchange::BinanceExchange,
        portfolio::event_parser::EventParser,
        response::BinanceResponse,
    },
    exchange_account_snapshot::{AccountSnapshotContext, ETExchangeAccountSnapshot},
    portfolio::{AccountEvent, AccountSnapshot, BalanceParser},
};
use chrono::{TimeZone, Utc};
use platform_errors::{BuildError, BuildResult, ExchangeError};

impl ETExchangeAccountSnapshot for BinanceExchange {
    type SnapshotRequest = BinanceAccountStatusRequest;

    fn account_snapshot_request() -> Self::SnapshotRequest {
        BinanceAccountStatusRequest::new()
    }

    fn account_snapshot(
        response: BinanceResponse<BinanceAccountStatusResponse>,
        context: &AccountSnapshotContext<'_>,
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
            context.mapping(),
        )?;
        Ok(AccountSnapshot::new(balances, occurred_at))
    }

    fn account_event(
        value: &serde_json::Value,
        context: &AccountSnapshotContext<'_>,
    ) -> BuildResult<Option<AccountEvent>> {
        EventParser::parse(value, context.mapping())
    }
}
