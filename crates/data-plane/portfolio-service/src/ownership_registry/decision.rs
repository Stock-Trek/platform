pub struct Decision {
    pub previous_owner: Option<String>,
    pub was_active: bool,
}

impl Decision {
    pub fn new(previous_owner: Option<String>, was_active: bool) -> Self {
        Self {
            previous_owner,
            was_active,
        }
    }
}
