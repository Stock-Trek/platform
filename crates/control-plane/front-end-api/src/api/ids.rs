use uuid::Uuid;

pub struct Id;

impl Id {
    pub fn mint(prefix: &str) -> String {
        format!("{prefix}_{}", Uuid::new_v4().simple())
    }
}
