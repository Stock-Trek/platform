use crate::{exchange::ExchangeError, retryability::Retryability};
use platform_types::new_types::DurationSeconds;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct BinanceError {
    pub code: i64,
    pub msg: String,
    pub data: Option<BinanceErrorData>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceErrorData {
    pub serverTime: Option<i64>,
    pub retryAfter: Option<i64>,
}

impl BinanceError {
    pub fn error_type(&self) -> &'static str {
        "binance"
    }

    pub fn retryability(&self) -> Retryability {
        Retryability::from_binance_code(self.code)
    }

    pub fn retryability_for_status(&self, status: u16) -> Retryability {
        Retryability::from_binance_code_and_status(self.code, status)
    }

    pub fn retry_after(&self) -> Option<DurationSeconds> {
        let data = self.data.as_ref()?;
        let retry_at_ms = data.retryAfter?;
        let now_ms = data.serverTime?;
        let backoff_ms = retry_at_ms.saturating_sub(now_ms).max(0) as u64;
        Some(DurationSeconds(backoff_ms.div_ceil(1000) as i64))
    }

    pub fn exchange_error(
        &self,
        status: u16,
        retry_after: Option<DurationSeconds>,
    ) -> ExchangeError {
        ExchangeError::Response {
            code: self.code,
            message: self.msg.clone(),
            status,
            retry_after,
            retryability: self.retryability_for_status(status),
        }
    }
}

impl std::fmt::Display for BinanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.msg)
    }
}

impl std::error::Error for BinanceError {}
