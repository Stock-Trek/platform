//! Hand-rolled SigV4 presigned POST signing.
//!
//! The AWS SDK can presign individual S3 requests, but it does not expose the
//! browser-style POST policy used by the build agent and the run invoker. Keep
//! that signing logic isolated here so ordinary S3 operations can use the SDK
//! client.

use crate::SdkConfig;
use aws_credential_types::Credentials;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, KeyInit, Mac};
use platform_config::env_names::S3_SIGNED_URL_EXPIRY_SECS;
use platform_errors::{BpError, BpResult};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};
use time::OffsetDateTime;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SERVICE: &str = "s3";
const KEY_FIELD: &str = "key";

/// A presigned S3 POST: the upload URL plus the form fields the uploader must
/// send.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PresignedPost {
    pub url: String,
    pub fields: BTreeMap<String, String>,
}

/// Signs S3 POST policies with SigV4.
pub struct PresignedPostSigner {
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
}

impl PresignedPostSigner {
    pub fn from_sdk_config(sdk_config: &SdkConfig, bucket: String) -> Self {
        let region = sdk_config
            .region()
            .map(|region| region.as_ref().to_string())
            .unwrap_or_else(|| "us-east-1".to_string());
        let endpoint_url = sdk_config.endpoint_url().map(str::to_string);
        Self::new(bucket, region, endpoint_url)
    }

    pub fn new(bucket: String, region: String, endpoint_url: Option<String>) -> Self {
        Self {
            bucket,
            region,
            endpoint_url,
        }
    }

    /// Signs a POST policy that restricts uploads to the object key described
    /// by `key_field` and `key_condition`, appending `extra_conditions` to the
    /// policy.
    pub fn sign(
        &self,
        credentials: &Credentials,
        key_field: &str,
        key_condition: Value,
        extra_conditions: &[Value],
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
            key_condition,
            extra_conditions,
            &credential,
            &amz_date,
            expires_at,
            credentials.session_token(),
        );
        let policy_base64 = BASE64.encode(serde_json::to_string(&policy)?);
        let signing_key = Self::signing_key(credentials.secret_access_key(), &date, &self.region);
        let signature = hex::encode(Self::hmac_sha256(&signing_key, policy_base64.as_bytes()));
        let mut fields = BTreeMap::from([
            (KEY_FIELD.to_string(), key_field.to_string()),
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
        key_condition: Value,
        extra_conditions: &[Value],
        credential: &str,
        amz_date: &str,
        expires_at: SystemTime,
        session_token: Option<&str>,
    ) -> Value {
        let mut conditions = vec![
            json!({ "bucket": bucket }),
            key_condition,
            json!({ "x-amz-algorithm": ALGORITHM }),
            json!({ "x-amz-credential": credential }),
            json!({ "x-amz-date": amz_date }),
        ];
        conditions.extend(extra_conditions.iter().cloned());
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
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            date_time.year(),
            u8::from(date_time.month()),
            date_time.day(),
            date_time.hour(),
            date_time.minute(),
            date_time.second(),
            date_time.millisecond(),
        )
    }
}

#[cfg(test)]
mod tests;
