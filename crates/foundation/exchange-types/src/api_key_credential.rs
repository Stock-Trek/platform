use secrecy::SecretString;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyCredentials {
    pub api_key: String,
    pub secret: SecretString,
}
