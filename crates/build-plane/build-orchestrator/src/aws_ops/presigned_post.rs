//! Hand-rolled SigV4 presigned POST signing.
//!
//! The AWS SDK can presign individual S3 requests, but it does not expose the
//! browser-style POST policy used by the build agent. Keep that signing logic
//! isolated here so ordinary S3 operations can use the SDK client.

use aws_credential_types::Credentials;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, KeyInit, Mac};
use platform_config::env_names::S3_SIGNED_URL_EXPIRY_SECS;
use platform_errors::{BpError, BpResult};
use serde_json::json;
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    time::{Duration, SystemTime},
};
use time::OffsetDateTime;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const MIN_OBJECT_SIZE_BYTES: u64 = 0;
const SERVICE: &str = "s3";

pub struct PresignedPost {
    pub url: String,
    pub fields: BTreeMap<String, String>,
    max_artifact_size_bytes: NonZeroU64,
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

pub struct PresignedPostSigner {
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
    max_artifact_size_bytes: NonZeroU64,
}

impl PresignedPostSigner {
    pub fn new(
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        max_artifact_size_bytes: NonZeroU64,
    ) -> Self {
        Self {
            bucket,
            region,
            endpoint_url,
            max_artifact_size_bytes,
        }
    }

    pub fn sign(
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
            &self.bucket,
            key_prefix,
            &credential,
            &amz_date,
            expires_at,
            credentials.session_token(),
            self.max_artifact_size_bytes.get(),
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
            max_artifact_size_bytes: self.max_artifact_size_bytes,
        })
    }

    fn bucket_url(&self) -> String {
        match &self.endpoint_url {
            Some(endpoint) => format!("{}/{}/", endpoint.trim_end_matches('/'), self.bucket),
            None => format!(
                "https://{}.s3.{}.{}/",
                self.bucket,
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
    use serde_json::{Value, json};

    #[test]
    fn signs_post_policy_with_session_token() {
        let credentials = Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("token/with+chars=".to_string()),
            None,
            "test",
        );
        let signer = PresignedPostSigner::new(
            "stock-trek-user-artifacts".to_string(),
            "us-east-1".to_string(),
            None,
            NonZeroU64::new(123).unwrap(),
        );
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_704_164_645);

        let post = signer
            .sign(&credentials, "artifacts/42/", now, Duration::from_secs(900))
            .unwrap();

        assert_eq!(
            post.url,
            "https://stock-trek-user-artifacts.s3.us-east-1.amazonaws.com/"
        );
        assert_eq!(post.fields["key"], "artifacts/42/${filename}");
        assert_eq!(post.fields["x-amz-date"], "20240102T030405Z");
        assert_eq!(
            post.fields["x-amz-credential"],
            "AKIAIOSFODNN7EXAMPLE/20240102/us-east-1/s3/aws4_request"
        );
        assert_eq!(post.fields["x-amz-security-token"], "token/with+chars=");
        assert_eq!(post.fields["x-amz-signature"].len(), 64);
        assert_eq!(post.max_artifact_size_bytes().get(), 123);

        let policy: Value =
            serde_json::from_slice(&BASE64.decode(&post.fields["policy"]).unwrap()).unwrap();
        assert_eq!(policy["expiration"], "2024-01-02T03:19:05.000Z");
        assert_eq!(
            policy["conditions"][5],
            json!(["content-length-range", 0, 123])
        );
        assert_eq!(
            policy["conditions"][6],
            json!({ "x-amz-security-token": "token/with+chars=" })
        );
    }
}
