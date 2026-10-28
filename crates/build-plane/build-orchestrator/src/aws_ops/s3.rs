use aws_config::SdkConfig;
use aws_credential_types::{
    Credentials,
    provider::{ProvideCredentials, SharedCredentialsProvider},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, KeyInit, Mac};
use platform_config::env_names::S3_SIGNED_URL_EXPIRY_SECS;
use platform_errors::{BpError, BpResult, StorageError, TimeoutError};
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use reqwest::Client;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    time::{Duration, SystemTime},
};
use time::OffsetDateTime;
use url::Url;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const MIN_OBJECT_SIZE_BYTES: u64 = 0;
const SERVICE: &str = "s3";

#[derive(Debug, Clone)]
pub struct S3Config {
    pub user_artifacts_bucket: String,
    pub max_source_archive_size_bytes: NonZeroU64,
    pub max_artifact_size_bytes: NonZeroU64,
    pub signed_url_expiry: Duration,
    pub endpoint_url: Option<String>,
}

pub struct PresignedPost {
    pub url: String,
    pub fields: BTreeMap<String, String>,
    max_artifact_size_bytes: NonZeroU64,
}

pub struct UploadedObject {
    pub signed_get_url: String,
}

struct ObjectLocation {
    scheme: String,
    host: String,
    path: String,
}

impl PresignedPost {
    pub fn max_artifact_size_bytes(&self) -> NonZeroU64 {
        self.max_artifact_size_bytes
    }
}

#[cfg(test)]
impl PresignedPost {
    pub(crate) fn for_test(
        url: String,
        fields: BTreeMap<String, String>,
        max_artifact_size_bytes: NonZeroU64,
    ) -> Self {
        Self {
            url,
            fields,
            max_artifact_size_bytes,
        }
    }
}

pub struct S3 {
    config: S3Config,
    credentials_provider: SharedCredentialsProvider,
    endpoint_url: Option<String>,
    region: String,
    timeouts: Timeouts,
    http: Client,
}

impl S3 {
    pub fn new(
        credentials_provider: SharedCredentialsProvider,
        region: String,
        endpoint_url: Option<String>,
        timeouts: Timeouts,
        config: S3Config,
    ) -> Self {
        Self {
            config,
            credentials_provider,
            endpoint_url,
            region,
            timeouts,
            http: Client::new(),
        }
    }

    pub fn from_sdk_config(
        sdk_config: &SdkConfig,
        timeouts: Timeouts,
        config: S3Config,
    ) -> BpResult<Self> {
        let credentials_provider = sdk_config
            .credentials_provider()
            .ok_or(BpError::Storage(StorageError::MissingCredentials))?;
        let region = sdk_config
            .region()
            .map(|region| region.as_ref().to_string())
            .unwrap_or_else(|| "us-east-1".to_string());
        let endpoint_url = config
            .endpoint_url
            .clone()
            .or_else(|| sdk_config.endpoint_url().map(str::to_string));
        Ok(Self::new(
            credentials_provider,
            region,
            endpoint_url,
            timeouts,
            config,
        ))
    }

    pub async fn signed_post(
        &self,
        key_prefix: &str,
        deadline: Deadline,
    ) -> BpResult<PresignedPost> {
        let credentials = self.credentials(deadline).await?;
        self.presigned_post(
            &credentials,
            key_prefix,
            SystemTime::now(),
            self.config.signed_url_expiry,
        )
    }

    pub async fn put_object(
        &self,
        key: &str,
        contents: Vec<u8>,
        deadline: Deadline,
    ) -> BpResult<UploadedObject> {
        let max_source_archive_size_bytes = self.config.max_source_archive_size_bytes.get();
        if contents.len() as u64 > max_source_archive_size_bytes {
            return Err(BpError::Storage(StorageError::ObjectTooLarge {
                key: key.to_owned(),
                size: contents.len(),
                max_bytes: max_source_archive_size_bytes,
            }));
        }
        let credentials = self.credentials(deadline).await?;
        let url = self.presigned_url(
            &credentials,
            "PUT",
            key,
            SystemTime::now(),
            self.config.signed_url_expiry,
        )?;
        let timeout = self.timeouts.operation_timeout(deadline)?;
        let response = tokio::time::timeout(
            timeout,
            self.http
                .put(url)
                .header(reqwest::header::CONTENT_TYPE, "application/zip")
                .body(contents)
                .send(),
        )
        .await
        .map_err(|_| {
            BpError::Timeout(TimeoutError::Operation {
                operation: "S3 upload object",
            })
        })?
        .map_err(|source| BpError::http_request("upload object", key, source))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(BpError::http_rejected(
                "upload object",
                key,
                status.as_u16(),
                body,
            ));
        }
        let signed_get_url = self.presigned_url(
            &credentials,
            "GET",
            key,
            SystemTime::now(),
            self.config.signed_url_expiry,
        )?;
        Ok(UploadedObject { signed_get_url })
    }

    async fn credentials(&self, deadline: Deadline) -> BpResult<Credentials> {
        let timeout = self.timeouts.operation_timeout(deadline)?;
        tokio::time::timeout(timeout, self.credentials_provider.provide_credentials())
            .await
            .map_err(|_| {
                BpError::Timeout(TimeoutError::Operation {
                    operation: "S3 provide_credentials",
                })
            })?
            .map_err(|error| BpError::Storage(StorageError::Credentials(error)))
    }
}

impl S3 {
    fn presigned_url(
        &self,
        credentials: &Credentials,
        method: &str,
        key: &str,
        now: SystemTime,
        expires_in: Duration,
    ) -> BpResult<String> {
        let location = self.object_location(key)?;
        let (date, amz_date) = Self::amz_date(now);
        let credential = format!(
            "{}/{}/{}/{}/aws4_request",
            credentials.access_key_id(),
            date,
            self.region,
            SERVICE
        );
        let mut query = vec![
            ("X-Amz-Algorithm".to_string(), ALGORITHM.to_string()),
            ("X-Amz-Credential".to_string(), credential),
            ("X-Amz-Date".to_string(), amz_date.clone()),
            (
                "X-Amz-Expires".to_string(),
                expires_in.as_secs().to_string(),
            ),
            ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
        ];
        if let Some(token) = credentials.session_token() {
            query.push(("X-Amz-Security-Token".to_string(), token.to_string()));
        }
        query.sort_by(|left, right| left.0.cmp(&right.0));
        let canonical_query = query
            .iter()
            .map(|(name, value)| format!("{}={}", Self::uri_encode(name), Self::uri_encode(value)))
            .collect::<Vec<_>>()
            .join("&");
        let canonical_headers = format!("host:{}\n", location.host);
        let canonical_request = format!(
            "{method}\n{}\n{canonical_query}\n{canonical_headers}\nhost\nUNSIGNED-PAYLOAD",
            location.path
        );
        let scope = format!("{date}/{}/{SERVICE}/aws4_request", self.region);
        let string_to_sign = format!(
            "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key = Self::signing_key(credentials.secret_access_key(), &date, &self.region);
        let signature = hex::encode(Self::hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        Ok(format!(
            "{}://{}{}?{canonical_query}&X-Amz-Signature={signature}",
            location.scheme, location.host, location.path
        ))
    }

    fn object_location(&self, key: &str) -> BpResult<ObjectLocation> {
        let key = Self::encode_path(key);
        match &self.endpoint_url {
            Some(endpoint) => {
                let url = Url::parse(endpoint)
                    .map_err(|source| BpError::Storage(StorageError::InvalidEndpoint { source }))?;
                let host = url.host_str().ok_or_else(|| {
                    BpError::Storage(StorageError::InvalidEndpointHost {
                        endpoint: endpoint.clone(),
                    })
                })?;
                let host = match url.port() {
                    Some(port) => format!("{host}:{port}"),
                    None => host.to_owned(),
                };
                let base_path = url.path().trim_end_matches('/');
                Ok(ObjectLocation {
                    scheme: url.scheme().to_owned(),
                    host,
                    path: format!(
                        "{base_path}/{}/{key}",
                        Self::uri_encode(&self.config.user_artifacts_bucket)
                    ),
                })
            }
            None => Ok(ObjectLocation {
                scheme: "https".to_owned(),
                host: format!(
                    "{}.s3.{}.{}",
                    self.config.user_artifacts_bucket,
                    self.region,
                    Self::dns_suffix(&self.region)
                ),
                path: format!("/{key}"),
            }),
        }
    }

    fn encode_path(value: &str) -> String {
        value
            .split('/')
            .map(Self::uri_encode)
            .collect::<Vec<_>>()
            .join("/")
    }

    fn uri_encode(value: &str) -> String {
        let mut encoded = String::new();
        for byte in value.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    encoded.push(char::from(byte));
                }
                byte => {
                    encoded.push('%');
                    encoded.push_str(&format!("{byte:02X}"));
                }
            }
        }
        encoded
    }

    fn presigned_post(
        &self,
        credentials: &Credentials,
        key_prefix: &str,
        now: SystemTime,
        expires_in: Duration,
    ) -> BpResult<PresignedPost> {
        let expires_at = now.checked_add(expires_in).ok_or_else(|| {
            BpError::config_invalid(
                S3_SIGNED_URL_EXPIRY_SECS,
                "expiry overflows the system clock",
            )
        })?;
        let (date, amz_date) = Self::amz_date(now);
        let credential = format!(
            "{}/{}/{}/{}/aws4_request",
            credentials.access_key_id(),
            date,
            self.region,
            SERVICE
        );
        let policy = Self::policy(
            &self.config.user_artifacts_bucket,
            key_prefix,
            &credential,
            &amz_date,
            expires_at,
            credentials.session_token(),
            self.config.max_artifact_size_bytes.get(),
        );
        let policy_base64 = BASE64.encode(serde_json::to_string(&policy)?);
        let signing_key = Self::signing_key(credentials.secret_access_key(), &date, &self.region);
        let signature = hex::encode(Self::hmac_sha256(&signing_key, policy_base64.as_bytes()));
        let mut fields = BTreeMap::from([
            ("key".to_string(), format!("{key_prefix}${{filename}}")),
            ("policy".to_string(), policy_base64),
            ("x-amz-algorithm".to_string(), ALGORITHM.to_string()),
            ("x-amz-credential".to_string(), credential),
            ("x-amz-date".to_string(), amz_date),
            ("x-amz-signature".to_string(), signature),
        ]);
        if let Some(token) = credentials.session_token() {
            fields.insert("x-amz-security-token".to_string(), token.to_string());
        }
        Ok(PresignedPost {
            url: self.bucket_url(),
            fields,
            max_artifact_size_bytes: self.config.max_artifact_size_bytes,
        })
    }

    fn bucket_url(&self) -> String {
        match &self.endpoint_url {
            Some(endpoint) => format!(
                "{}/{}/",
                endpoint.trim_end_matches('/'),
                self.config.user_artifacts_bucket
            ),
            None => format!(
                "https://{}.s3.{}.{}/",
                self.config.user_artifacts_bucket,
                self.region,
                Self::dns_suffix(&self.region)
            ),
        }
    }

    fn dns_suffix(region: &str) -> &'static str {
        if region.starts_with("cn-") {
            "amazonaws.com.cn"
        } else {
            "amazonaws.com"
        }
    }

    fn policy(
        bucket: &str,
        key_prefix: &str,
        credential: &str,
        amz_date: &str,
        expires_at: SystemTime,
        session_token: Option<&str>,
        max_artifact_size_bytes: u64,
    ) -> serde_json::Value {
        let mut conditions = vec![
            json!({ "bucket": bucket }),
            json!(["starts-with", "$key", key_prefix]),
            json!({ "x-amz-algorithm": ALGORITHM }),
            json!({ "x-amz-credential": credential }),
            json!({ "x-amz-date": amz_date }),
        ];
        conditions.push(json!([
            "content-length-range",
            MIN_OBJECT_SIZE_BYTES,
            max_artifact_size_bytes
        ]));
        if let Some(token) = session_token {
            conditions.push(json!({ "x-amz-security-token": token }));
        }
        json!({
            "expiration": Self::expiration(expires_at),
            "conditions": conditions,
        })
    }

    fn signing_key(secret_access_key: &str, date: &str, region: &str) -> Vec<u8> {
        let secret = format!("AWS4{secret_access_key}");
        let date_key = Self::hmac_sha256(secret.as_bytes(), date.as_bytes());
        let region_key = Self::hmac_sha256(&date_key, region.as_bytes());
        let service_key = Self::hmac_sha256(&region_key, SERVICE.as_bytes());
        Self::hmac_sha256(&service_key, b"aws4_request")
    }

    fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn amz_date(time: SystemTime) -> (String, String) {
        let date_time = OffsetDateTime::from(time);
        let date = format!(
            "{:04}{:02}{:02}",
            date_time.year(),
            u8::from(date_time.month()),
            date_time.day()
        );
        let amz_date = format!(
            "{date}T{:02}{:02}{:02}Z",
            date_time.hour(),
            date_time.minute(),
            date_time.second()
        );
        (date, amz_date)
    }

    fn expiration(time: SystemTime) -> String {
        let date_time = OffsetDateTime::from(time);
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z",
            date_time.year(),
            u8::from(date_time.month()),
            date_time.day(),
            date_time.hour(),
            date_time.minute(),
            date_time.second()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_time::timeouts::Timeout;

    #[test]
    fn presigns_get_url_with_sigv4() {
        let credentials = Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("token/with+chars=".to_string()),
            None,
            "test",
        );
        let s3 = S3::new(
            SharedCredentialsProvider::new(credentials.clone()),
            "us-east-1".to_string(),
            None,
            Timeouts {
                connect: Timeout::Disabled,
                operation: Timeout::Disabled,
            },
            S3Config {
                user_artifacts_bucket: "stock-trek-user-artifacts".to_string(),
                max_source_archive_size_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
                max_artifact_size_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
                signed_url_expiry: Duration::from_secs(900),
                endpoint_url: None,
            },
        );
        let url = s3
            .presigned_url(
                &credentials,
                "GET",
                "github/github.com/42/7/heads/main/abc123/source/algorithms.zip",
                SystemTime::UNIX_EPOCH + Duration::from_secs(1_735_787_045),
                Duration::from_secs(900),
            )
            .unwrap();

        assert_eq!(
            url,
            concat!(
                "https://stock-trek-user-artifacts.s3.us-east-1.amazonaws.com/",
                "github/github.com/42/7/heads/main/abc123/source/algorithms.zip?",
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&",
                "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20250102%2Fus-east-1%2Fs3%2Faws4_request&",
                "X-Amz-Date=20250102T030405Z&X-Amz-Expires=900&",
                "X-Amz-Security-Token=token%2Fwith%2Bchars%3D&",
                "X-Amz-SignedHeaders=host&",
                "X-Amz-Signature=d7e1bf04e1b60f2f22bdd583f5d7ba2345100a9b6865061ac753e5497e1ff760"
            )
        );
    }
}
