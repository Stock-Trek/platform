#[derive(Debug, Clone)]
pub(super) struct OwnershipEntry {
    pub(super) owner: String,
    pub(super) version: u64,
    pub(super) active: bool,
}
