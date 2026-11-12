use super::{
    balances::BalanceParser,
    transport::{AccountPositionEvent, BinanceAccountEvent},
};
use chrono::{TimeZone, Utc};
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_types::binance::user_data::BinanceUserDataEvent;
use platform_type_bridge::TickerMapping;

pub(super) struct EventParser;

impl EventParser {
    pub(super) fn parse(
        value: &serde_json::Value,
        mapping: &TickerMapping,
    ) -> BpResult<Option<BinanceAccountEvent>> {
        let event = BinanceUserDataEvent::from_value(value)?;
        if event.is_listen_key_expired() {
            return Ok(Some(BinanceAccountEvent::ListenKeyExpired));
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
            mapping,
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
        )?;
        let occurred_at = Utc
            .timestamp_millis_opt(event.event_time())
            .single()
            .ok_or_else(|| {
                BpError::Exchange(ExchangeError::BadResponse(
                    "invalid event timestamp".to_string(),
                ))
            })?;
        Ok(Some(BinanceAccountEvent::Position(
            AccountPositionEvent::new(version, balances, occurred_at),
        )))
    }
}
