use platform_errors::{ExchangeError, PlatformError, PlatformResult};
use std::collections::BTreeMap;
use stock_trek_types::cex::asset_id::AssetId;

pub struct BalanceParser;

impl BalanceParser {
    pub fn parse<F>(
        balances: impl IntoIterator<Item = (String, String, String)>,
        mapping: &F,
    ) -> PlatformResult<BTreeMap<AssetId, f64>>
    where
        F: Fn(&str) -> Option<AssetId> + ?Sized,
    {
        let mut parsed = BTreeMap::new();
        for (asset, free, locked) in balances {
            let Some(asset_id) = mapping(&asset) else {
                continue;
            };
            parsed.insert(asset_id, Self::decimal(&free)? + Self::decimal(&locked)?);
        }
        Ok(parsed)
    }

    fn decimal(value: &str) -> PlatformResult<f64> {
        let parsed = value.parse::<f64>().map_err(|error| {
            PlatformError::Exchange(ExchangeError::BadResponse(format!(
                "invalid balance {value}: {error}"
            )))
        })?;
        if !parsed.is_finite() {
            return Err(PlatformError::Exchange(ExchangeError::BadResponse(
                "non-finite balance".to_string(),
            )));
        }
        Ok(parsed)
    }
}
