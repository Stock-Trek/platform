use uuid::Uuid;

pub(crate) struct Id;

impl Id {
    pub(crate) fn mint(prefix: &str) -> String {
        format!("{prefix}_{}", Uuid::new_v4().simple())
    }
}
