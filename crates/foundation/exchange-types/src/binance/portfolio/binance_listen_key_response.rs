use platform_errors::{BuildError, BuildResult};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey", default)]
    listen_key: Option<String>,
}

impl BinanceListenKeyResponse {
    pub fn require_listen_key(self) -> BuildResult<String> {
        let listen_key = self
            .listen_key
            .filter(|listen_key| !listen_key.is_empty())
            .ok_or_else(|| BuildError::bad_value("listen key", "", "is missing"))?;
        Ok(listen_key)
    }
}
