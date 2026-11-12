use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct CredentialEntry {
    pub(super) account: String,
    pub(super) api_key: String,
    pub(super) secret: String,
}
