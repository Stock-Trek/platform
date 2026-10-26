use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct TBSessionAuthentication {
    pub authorized_at: SystemTime,
    pub connected_at: SystemTime,
}
