#[derive(Debug, Clone)]
pub(super) struct OwnershipEntry {
    pub owner: String,
    pub version: u64,
    pub active: bool,
}
