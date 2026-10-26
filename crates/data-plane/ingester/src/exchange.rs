use stock_trek_types::cex::cex_id::CexId;

/// Text helpers for exchange identifiers used in configuration and subjects.
pub trait CexIdExt: Sized {
    fn parse_exchange(value: &str) -> Option<Self>;

    fn slug(&self) -> String;
}

impl CexIdExt for CexId {
    fn parse_exchange(value: &str) -> Option<Self> {
        // TODO: replace this with `FromStr` once `CexId` exposes one.
        match value.trim().to_ascii_lowercase().as_str() {
            "binance" => Some(Self::Binance),
            _ => None,
        }
    }

    fn slug(&self) -> String {
        self.to_string().to_ascii_lowercase()
    }
}
