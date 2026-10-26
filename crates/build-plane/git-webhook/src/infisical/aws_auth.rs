use serde::{Deserialize, Serialize};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InfisicalAwsAuthRequest {
    pub identity_id: String,
    pub iam_http_request_method: String,
    pub iam_request_headers: String,
    pub iam_request_body: String,
}

#[derive(Deserialize)]
pub struct InfisicalTokenResponse {
    #[serde(rename = "accessToken")]
    pub access_token: String,
    #[allow(unused)]
    #[serde(rename = "expiresIn")]
    pub expires_in: i64,
    #[allow(unused)]
    #[serde(rename = "accessTokenMaxTTL")]
    pub access_token_max_ttl: Option<i64>,
    #[allow(unused)]
    #[serde(rename = "tokenType")]
    pub token_type: Option<String>,
}
