use platform_errors::{BuildError, BuildResult};

/// Environment variable containing the SSM parameter path to load secrets from.
pub const SSM_PATH_ENV: &str = "SECRETS_SSM_PATH";

/// The SSM parameter path and decryption mode used to load secrets.
#[derive(Debug, Clone)]
pub struct SsmConfig {
    pub path: String,
    pub decrypt: bool,
}

impl SsmConfig {
    pub fn new(mut path: String, decrypt: bool) -> BuildResult<Self> {
        path = path.trim().to_string();
        if path.is_empty() {
            return Err(BuildError::config_empty(SSM_PATH_ENV));
        }
        if !path.starts_with('/') {
            path.insert(0, '/');
        }
        if path.ends_with('/') {
            return Err(BuildError::config_invalid(
                SSM_PATH_ENV,
                "must not end with '/'",
            ));
        }
        Ok(Self { path, decrypt })
    }
}
