use aws_credential_types::Credentials;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use platform_config::env_names::S3_SIGNED_URL_EXPIRY_SECS;
use platform_errors::{BpError, BpResult};
use serde::Serialize;
use serde_json::json;
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SERVICE: &str = "s3";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PresignedPost {
    pub url: String,
    pub fields: BTreeMap<String, String>,
}

pub struct PresignedPostSigner {
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
}

impl PresignedPostSigner {
    pub fn new(bucket: String, region: String, endpoint_url: Option<String>) -> Self {
        Self {
            bucket,
            region,
            endpoint_url,
        }
    }

    pub fn sign(
        &self,
        credentials: &Credentials,
        key: &str,
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
            key,
            &credential,
            &amz_date,
            expires_at,
            credentials.session_token(),
        );
        let policy_base64 = BASE64.encode(serde_json::to_string(&policy)?);
        let signing_key = Self::signing_key(credentials.secret_access_key(), &date, &self.region);
        let signature = hex::encode(Self::hmac_sha256(&signing_key, policy_base64.as_bytes()));
        let mut fields = BTreeMap::from([
            ("key".to_string(), key.to_string()),
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
        key: &str,
        credential: &str,
        amz_date: &str,
        expires_at: SystemTime,
        session_token: Option<&str>,
    ) -> serde_json::Value {
        let mut conditions = vec![
            json!({ "bucket": bucket }),
            json!({ "key": key }),
            json!({ "x-amz-algorithm": ALGORITHM }),
            json!({ "x-amz-credential": credential }),
            json!({ "x-amz-date": amz_date }),
        ];
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
        let date_time = DateTime::<Utc>::from(time);
        let date = date_time.format("%Y%m%d").to_string();
        let amz_date = date_time.format("%Y%m%dT%H%M%SZ").to_string();
        (date, amz_date)
    }

    fn expiration(time: SystemTime) -> String {
        DateTime::<Utc>::from(time)
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string()
    }
}

#[cfg(test)]
mod tests;
