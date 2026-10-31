#[cfg(feature = "serde")]
use crate::util::serde_rounding_strategy;
use rust_decimal::RoundingStrategy;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy)]
pub struct CexPreferences {
    pub max_network_delay_millis: u32,
    pub rounding: CexRoundingPreferences,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy)]
pub struct CexRoundingPreferences {
    #[cfg_attr(feature = "serde", serde(with = "serde_rounding_strategy"))]
    pub activation_price_triggered_above: RoundingStrategy,
    #[cfg_attr(feature = "serde", serde(with = "serde_rounding_strategy"))]
    pub activation_price_triggered_below: RoundingStrategy,
    #[cfg_attr(feature = "serde", serde(with = "serde_rounding_strategy"))]
    pub price: RoundingStrategy,
    #[cfg_attr(feature = "serde", serde(with = "serde_rounding_strategy"))]
    pub quantity: RoundingStrategy,
    #[cfg_attr(feature = "serde", serde(with = "serde_rounding_strategy"))]
    pub callback_rate_bps: RoundingStrategy,
}
