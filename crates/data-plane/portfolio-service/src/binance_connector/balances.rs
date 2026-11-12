use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_types::ticker::Ticker;
use platform_type_bridge::TickerMapping;
use std::collections::BTreeMap;
use stock_trek_types::cex::asset_id::AssetId;

pub(super) struct BalanceParser;

impl BalanceParser {
    pub(super) fn parse(
        mapping: &TickerMapping,
        balances: impl IntoIterator<Item = (String, String, String)>,
    ) -> BpResult<BTreeMap<AssetId, f64>> {
        let mut parsed = BTreeMap::new();
        for (asset, free, locked) in balances {
            let Some(asset_id) = mapping.asset_id(&Ticker(asset)) else {
                continue;
            };
            parsed.insert(*asset_id, Self::decimal(&free)? + Self::decimal(&locked)?);
        }
        Ok(parsed)
    }

    fn decimal(value: &str) -> BpResult<f64> {
        let parsed = value.parse::<f64>().map_err(|error| {
            BpError::Exchange(ExchangeError::BadResponse(format!(
                "invalid balance {value}: {error}"
            )))
        })?;
        if !parsed.is_finite() {
            return Err(BpError::Exchange(ExchangeError::BadResponse(
                "non-finite balance".to_string(),
            )));
        }
        Ok(parsed)
    }
}
