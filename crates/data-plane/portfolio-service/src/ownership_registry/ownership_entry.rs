#[derive(Debug, Clone)]
pub struct OwnershipEntry {
    pub owner: String,
    pub version: u64,
    pub active: bool,
}
