pub(super) struct Decision {
    pub(super) previous_owner: Option<String>,
    pub(super) was_active: bool,
}

impl Decision {
    pub(super) fn new(previous_owner: Option<String>, was_active: bool) -> Self {
        Self {
            previous_owner,
            was_active,
        }
    }
}
