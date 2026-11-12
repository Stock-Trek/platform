use platform_errors::{BpError, BpResult};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey", default)]
    listen_key: Option<String>,
}

impl BinanceListenKeyResponse {
    pub fn require_listen_key(self) -> BpResult<String> {
        let listen_key = self
            .listen_key
            .filter(|listen_key| !listen_key.is_empty())
            .ok_or_else(|| BpError::bad_value("listen key", "", "is missing"))?;
        Ok(listen_key)
    }
}
