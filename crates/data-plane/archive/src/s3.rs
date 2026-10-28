use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3_transfer_manager::{Client as TransferClient, io::InputStream};
use bytes::Bytes;
use platform_aws::{load_default_sdk_config, s3_client, transfer_client};
use platform_errors::{BpError, BpResult};
use std::fmt;
use tracing::info;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Bucket(String);

impl Bucket {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self(bucket.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Bucket {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<&str> for Bucket {
    fn from(bucket: &str) -> Self {
        Self::new(bucket)
    }
}

impl From<String> for Bucket {
    fn from(bucket: String) -> Self {
        Self::new(bucket)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ObjectKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<&str> for ObjectKey {
    fn from(key: &str) -> Self {
        Self::new(key)
    }
}

impl From<String> for ObjectKey {
    fn from(key: String) -> Self {
        Self::new(key)
    }
}

/// An object returned by an S3 listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Object {
    pub key: ObjectKey,
    pub last_modified_millis: Option<i64>,
}

impl S3Object {
    pub fn last_modified_millis(&self) -> Option<i64> {
        self.last_modified_millis
    }
}

pub struct S3;

impl S3 {
    pub async fn clients() -> (S3Client, TransferClient) {
        let sdk_config = load_default_sdk_config().await;
        (s3_client(&sdk_config), transfer_client(&sdk_config))
    }

    pub async fn s3_client() -> S3Client {
        s3_client(&load_default_sdk_config().await)
    }

    pub async fn transfer_client() -> TransferClient {
        transfer_client(&load_default_sdk_config().await)
    }

    /// List objects together with the metadata needed to decide when a partition
    /// is settled.
    pub async fn list_objects(
        s3_client: &S3Client,
        bucket: &Bucket,
        prefix: &ObjectKey,
    ) -> BpResult<Vec<S3Object>> {
        let target = Self::object_target(bucket, prefix);
        info!("listing files with prefix {}", prefix);
        let mut files = Vec::new();
        let mut paginator = s3_client
            .list_objects_v2()
            .bucket(bucket.as_str())
            .prefix(prefix.as_str())
            .into_paginator()
            .send();
        while let Some(page_result) = paginator.next().await {
            let page =
                page_result.map_err(|error| BpError::s3("list objects", target.clone(), error))?;
            for object in page.contents() {
                if let Some(key) = object.key() {
                    files.push(S3Object {
                        key: ObjectKey::new(key),
                        last_modified_millis: object.last_modified().map(Self::modified_millis),
                    });
                }
            }
        }
        Ok(files)
    }

    pub async fn upload_bytes(
        transfer_client: &TransferClient,
        bucket: &Bucket,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> BpResult<()> {
        let target = Self::object_target(bucket, key);
        info!("upload bytes for key {}", key);
        let stream = InputStream::from(bytes);
        transfer_client
            .upload()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .body(stream)
            .initiate()
            .map_err(|error| BpError::s3("start upload", target.clone(), error))?
            .join()
            .await
            .map_err(|error| BpError::s3("upload object", target, error))?;
        Ok(())
    }

    pub fn object_target(bucket: &Bucket, key: &ObjectKey) -> String {
        format!("s3://{bucket}/{key}")
    }

    fn modified_millis(value: &aws_sdk_s3::primitives::DateTime) -> i64 {
        value
            .secs()
            .saturating_mul(1000)
            .saturating_add(i64::from(value.subsec_nanos() / 1_000_000))
    }
}
