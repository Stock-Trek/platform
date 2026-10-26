pub struct GitHubAppConfig {
    pub app_id: u64,
    pub private_key: String,
}

impl GitHubAppConfig {
    pub fn new(app_id: u64, private_key: String) -> Self {
        Self {
            app_id,
            private_key,
        }
    }
}
