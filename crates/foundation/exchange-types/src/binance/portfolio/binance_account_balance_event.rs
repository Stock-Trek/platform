#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceAccountBalanceEvent {
    asset: String,
    free: String,
    locked: String,
}

impl BinanceAccountBalanceEvent {
    pub(super) fn new(asset: String, free: String, locked: String) -> Self {
        Self {
            asset,
            free,
            locked,
        }
    }

    pub fn asset(&self) -> &str {
        &self.asset
    }

    pub fn free(&self) -> &str {
        &self.free
    }

    pub fn locked(&self) -> &str {
        &self.locked
    }
}
