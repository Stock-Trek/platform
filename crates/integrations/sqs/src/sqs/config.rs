#[derive(Clone)]
pub struct SqsConfig {
    pub queue_url: String,
}

impl SqsConfig {
    pub fn new(queue_url: String) -> Self {
        Self { queue_url }
    }
}
