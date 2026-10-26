use crate::infisical::{
    aws_auth::{InfisicalAwsAuthRequest, InfisicalTokenResponse},
    secrets::InfisicalSecretsResponse,
};
use aws_config::{Region, SdkConfig};
use aws_credential_types::{Credentials, provider::ProvideCredentials};
use aws_sigv4::http_request::{
    PayloadChecksumKind, SignableBody, SignableRequest, SigningInstructions, SigningParams,
    SigningSettings, sign,
};
use aws_smithy_runtime_api::client::identity::Identity;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::{BpError, BpResult, HttpError, StorageError, TimeoutError};
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    time::SystemTime,
};

const HEADER_CONTENT_LENGTH_KEY: &str = "content-length";
const HEADER_CONTENT_TYPE_VALUE: &str = "application/x-www-form-urlencoded";
const HEADER_SESSION_TOKEN_KEY: &str = "x-amz-security-token";
const INFISICAL_AWS_LOGIN_PATH: &str = "/api/v1/auth/aws-auth/login";
const INFISICAL_SECRETS_PATH: &str = "/api/v4/secrets";

pub struct InfisicalAwsAuth {
    http_client: reqwest::Client,
    base_url: String,
    timeouts: Timeouts,
}

impl InfisicalAwsAuth {
    pub fn new(http_client: reqwest::Client, base_url: String, timeouts: Timeouts) -> Self {
        Self {
            http_client,
            base_url,
            timeouts,
        }
    }
    pub async fn try_load_secrets(
        &self,
        sdk_config: &SdkConfig,
        deadline: Deadline,
        identity_id: &str,
        project_id: &str,
        environment: &str,
    ) -> BpResult<HashMap<String, String>> {
        let region = Self::region(sdk_config);
        let credentials = self.credentials(sdk_config, deadline).await?;
        let endpoint = Self::endpoint(&region);
        let headers = Self::headers(&credentials);
        let body = Self::body();
        let identity = credentials.into();
        let instructions =
            Self::signing_instructions(region.as_ref(), &identity, &endpoint, &headers, &body)?;
        let signed_headers = Self::signed_headers(instructions, &endpoint, &headers, body.len())?;
        let token_response = self
            .aws_login_response(
                &self.base_url,
                &signed_headers,
                &body,
                identity_id,
                deadline,
            )
            .await?;
        let secrets_response = self
            .secrets_response(
                &self.base_url,
                &token_response.access_token,
                project_id,
                environment,
                deadline,
            )
            .await?;
        let mut secrets = HashMap::new();
        for secret_response in secrets_response.secrets {
            secrets.insert(secret_response.secret_key, secret_response.secret_value);
        }
        Ok(secrets)
    }
}

// private helper functions
impl InfisicalAwsAuth {
    fn region(config: &SdkConfig) -> Region {
        config
            .region()
            .cloned()
            .unwrap_or_else(|| Region::from_static("us-east-1"))
    }

    async fn credentials(&self, config: &SdkConfig, deadline: Deadline) -> BpResult<Credentials> {
        let provider = config
            .credentials_provider()
            .ok_or_else(|| BpError::Storage(StorageError::MissingCredentials))?;
        self.within_deadline(deadline, "InfisicalAwsAuth provide_credentials", async {
            provider
                .provide_credentials()
                .await
                .map_err(|error| BpError::Storage(StorageError::Credentials(error)))
        })
        .await
    }

    async fn within_deadline<T>(
        &self,
        deadline: Deadline,
        operation: &'static str,
        future: impl std::future::Future<Output = BpResult<T>>,
    ) -> BpResult<T> {
        let timeout = self.timeouts.operation_timeout(deadline)?;
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| BpError::Timeout(TimeoutError::Operation { operation }))?
    }

    fn endpoint(region: &Region) -> String {
        format!(
            "https://sts.{}.{}",
            region,
            Self::dns_suffix(region.as_ref())
        )
    }

    fn dns_suffix(region: &str) -> &'static str {
        if region.starts_with("cn-") {
            "amazonaws.com.cn"
        } else {
            "amazonaws.com"
        }
    }

    fn headers(credentials: &Credentials) -> BTreeMap<String, String> {
        let mut headers = BTreeMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE.as_str().into(),
            HEADER_CONTENT_TYPE_VALUE.into(),
        );
        if let Some(token) = credentials.session_token() {
            headers.insert(HEADER_SESSION_TOKEN_KEY.into(), token.into());
        }
        headers
    }

    fn body() -> String {
        let mut params = BTreeMap::new();
        params.insert("Action".to_string(), vec!["GetCallerIdentity".to_string()]);
        params.insert("Version".to_string(), vec!["2011-06-15".to_string()]);
        params
            .iter()
            .flat_map(|(k, values)| values.iter().map(move |v| (k, v)))
            .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&")
    }

    fn signing_instructions<'a>(
        region: &'a str,
        identity: &'a Identity,
        endpoint: &'a str,
        headers: &'a BTreeMap<String, String>,
        body: &'a str,
    ) -> BpResult<SigningInstructions> {
        let signable_request = SignableRequest::new(
            Method::POST.as_str(),
            endpoint,
            headers.iter().map(|(k, v)| (k.as_str(), v.as_str())),
            SignableBody::Bytes(body.as_bytes()),
        )
        .map_err(|error| BpError::Http(HttpError::Signing(error)))?;
        let mut signing_settings = SigningSettings::default();
        signing_settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        let signing_params = SigningParams::V4(
            aws_sigv4::sign::v4::SigningParams::builder()
                .identity(identity)
                .region(region)
                .name("sts")
                .time(SystemTime::now())
                .settings(signing_settings)
                .build()
                .map_err(|error| BpError::Http(HttpError::SigningParams(error)))?,
        );
        let (instructions, _signature) = sign(signable_request, &signing_params)
            .map_err(|error| BpError::Http(HttpError::Signing(error)))?
            .into_parts();
        Ok(instructions)
    }

    /// This uses aws-sigv4 to create signed headers
    /// aws-sigv4 requires http::Request, so we build a temporary one for it to sign
    /// then read them out again to apply to the real one sent via the reqwest client
    fn signed_headers(
        instructions: SigningInstructions,
        endpoint: &str,
        unsigned_headers: &BTreeMap<String, String>,
        body_len: usize,
    ) -> BpResult<BTreeMap<String, String>> {
        let mut header_map = HeaderMap::new();
        for (key, value) in unsigned_headers {
            let name = HeaderName::from_str(key)
                .map_err(|error| BpError::Http(HttpError::InvalidHeaderName(error)))?;
            let value = HeaderValue::from_str(value)
                .map_err(|error| BpError::Http(HttpError::InvalidHeaderValue(error)))?;
            header_map.insert(name, value);
        }
        // Signing container. Never sent, built purely to allow aws-sigv4 to create signed headers
        let mut temp_signing_request = http::Request::builder()
            .method(http::Method::POST)
            .uri(endpoint)
            .body(())
            .map_err(|error| BpError::Http(HttpError::Http(error)))?;
        *temp_signing_request.headers_mut() = header_map;
        instructions.apply_to_request_http1x(&mut temp_signing_request);
        // read headers back out
        let mut signed_headers = temp_signing_request
            .headers()
            .iter()
            .map(|(k, v)| {
                let value = v
                    .to_str()
                    .map_err(|error| BpError::Http(HttpError::InvalidHeaderValueStr(error)))?
                    .to_string();
                Ok((k.as_str().to_string(), value))
            })
            .collect::<BpResult<BTreeMap<String, String>>>()?;
        signed_headers.insert(HEADER_CONTENT_LENGTH_KEY.into(), body_len.to_string());
        Ok(signed_headers)
    }

    async fn aws_login_response(
        &self,
        base_url: &str,
        headers: &BTreeMap<String, String>,
        body: &str,
        identity_id: &str,
        deadline: Deadline,
    ) -> BpResult<InfisicalTokenResponse> {
        let iam_http_request_method = Method::POST.to_string();
        let iam_request_headers = BASE64.encode(
            serde_json::to_vec(&headers).map_err(|error| BpError::Http(HttpError::Serde(error)))?,
        );
        let iam_request_body = BASE64.encode(body.as_bytes());
        let params = InfisicalAwsAuthRequest {
            identity_id: identity_id.into(),
            iam_http_request_method,
            iam_request_headers,
            iam_request_body,
        };
        let url = format!(
            "{}/{}",
            base_url.trim_end_matches("/"),
            INFISICAL_AWS_LOGIN_PATH.trim_start_matches("/")
        );
        self.within_deadline(deadline, "InfisicalAwsAuth aws-login", async {
            self.http_client
                .post(url)
                .json(&params)
                .send()
                .await
                .map_err(|error| BpError::Http(HttpError::Reqwest(error)))?
                .error_for_status()
                .map_err(|error| BpError::Http(HttpError::Reqwest(error)))?
                .json::<InfisicalTokenResponse>()
                .await
                .map_err(|error| BpError::Http(HttpError::Reqwest(error)))
        })
        .await
    }

    async fn secrets_response(
        &self,
        infisical_base_url: &str,
        access_token: &str,
        project_id: &str,
        environment: &str,
        deadline: Deadline,
    ) -> BpResult<InfisicalSecretsResponse> {
        let url = format!(
            "{}/{}?projectId={}&environment={}&viewSecretValue=true",
            infisical_base_url.trim_end_matches("/"),
            INFISICAL_SECRETS_PATH.trim_start_matches("/"),
            urlencoding::encode(project_id),
            urlencoding::encode(environment)
        );
        self.within_deadline(deadline, "InfisicalAwsAuth secrets-fetch", async {
            self.http_client
                .get(url)
                .bearer_auth(access_token)
                .send()
                .await
                .map_err(|error| BpError::Http(HttpError::Reqwest(error)))?
                .error_for_status()
                .map_err(|error| BpError::Http(HttpError::Reqwest(error)))?
                .json::<InfisicalSecretsResponse>()
                .await
                .map_err(|error| BpError::Http(HttpError::Reqwest(error)))
        })
        .await
    }
}
