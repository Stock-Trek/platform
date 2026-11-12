#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipApply {
    Applied {
        previous_owner: Option<String>,
        was_active: bool,
    },
    IgnoredStale,
    IgnoredConflict,
}
