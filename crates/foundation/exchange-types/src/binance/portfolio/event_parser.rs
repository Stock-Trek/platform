use super::BinanceUserDataEvent;
use crate::portfolio::{AccountEvent, AccountPositionEvent, BalanceParser};
use chrono::{TimeZone, Utc};
use platform_errors::{BuildError, BuildResult, ExchangeError};
use stock_trek_types::cex::asset_id::AssetId;

pub struct EventParser;

impl EventParser {
    pub fn parse<F>(value: &serde_json::Value, mapping: &F) -> BuildResult<Option<AccountEvent>>
    where
        F: Fn(&str) -> Option<AssetId> + ?Sized,
    {
        let event = BinanceUserDataEvent::from_value(value)?;
        if event.is_listen_key_expired() {
            return Ok(Some(AccountEvent::SessionExpired));
        }
        if !event.is_account_position() {
            return Ok(None);
        }
        let Some(version) = event.version() else {
            return Ok(None);
        };
        let Some(raw_balances) = event.balances() else {
            return Ok(None);
        };
        let balances = BalanceParser::parse(
            raw_balances
                .iter()
                .map(|balance| {
                    (
                        balance.asset().to_string(),
                        balance.free().to_string(),
                        balance.locked().to_string(),
                    )
                })
                .collect::<Vec<_>>(),
            mapping,
        )?;
        let occurred_at = Utc
            .timestamp_millis_opt(event.event_time())
            .single()
            .ok_or_else(|| {
                BuildError::Exchange(ExchangeError::BadResponse(
                    "invalid event timestamp".to_string(),
                ))
            })?;
        Ok(Some(AccountEvent::Position(AccountPositionEvent::new(
            version,
            balances,
            occurred_at,
        ))))
    }
}
