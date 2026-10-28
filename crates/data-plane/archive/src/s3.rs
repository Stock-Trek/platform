use aws_sdk_s3::{
    Client as S3Client,
    error::SdkError,
    types::{Delete, ObjectIdentifier},
};
use aws_sdk_s3_transfer_manager::{Client as TransferClient, io::InputStream};
use bytes::Bytes;
use platform_aws::{load_default_sdk_config, s3_client, transfer_client};
use platform_errors::{BpError, BpResult, S3DeleteObjectFailure, StorageError};
use std::{fmt, io, path::Path};
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

    pub async fn has_file(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
    ) -> BpResult<bool> {
        let target = Self::object_target(bucket, key);
        info!("checking for file {}", key);
        match s3_client
            .head_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(error)) if error.err().is_not_found() => Ok(false),
            Err(error) => Err(BpError::s3("check object existence", target, error)),
        }
    }

    pub async fn object_size(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
    ) -> BpResult<u64> {
        let target = Self::object_target(bucket, key);
        info!("get size for file {}", key);
        let output = s3_client
            .head_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .send()
            .await
            .map_err(|error| BpError::s3("read object metadata", target.clone(), error))?;
        let size = output.content_length().ok_or_else(|| {
            BpError::s3(
                "read object metadata",
                target.clone(),
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HEAD response did not include the object size",
                ),
            )
        })?;
        u64::try_from(size).map_err(|_| {
            BpError::s3(
                "read object metadata",
                target,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("HEAD response reported an invalid object size: {size}"),
                ),
            )
        })
    }

    pub async fn list_files(
        s3_client: &S3Client,
        bucket: &Bucket,
        prefix: &ObjectKey,
    ) -> BpResult<Vec<ObjectKey>> {
        Ok(Self::list_objects(s3_client, bucket, prefix)
            .await?
            .into_iter()
            .map(|object| object.key)
            .collect())
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

    pub async fn get_bytes(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
    ) -> BpResult<Bytes> {
        let target = Self::object_target(bucket, key);
        let output = s3_client
            .get_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .send()
            .await
            .map_err(|error| BpError::s3("get object", target.clone(), error))?;
        output
            .body
            .collect()
            .await
            .map(|body| body.into_bytes())
            .map_err(|error| BpError::s3("read object body", target, error))
    }

    pub async fn delete(s3_client: &S3Client, bucket: &Bucket, key: &ObjectKey) -> BpResult<()> {
        let target = Self::object_target(bucket, key);
        s3_client
            .delete_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .send()
            .await
            .map_err(|error| BpError::s3("delete object", target, error))?;
        Ok(())
    }

    pub async fn copy_file(
        s3_client: &S3Client,
        bucket: &Bucket,
        source_key: &ObjectKey,
        destination_key: &ObjectKey,
    ) -> BpResult<()> {
        let target = Self::object_target(bucket, destination_key);
        info!("copy file {} to {}", source_key, destination_key);
        s3_client
            .copy_object()
            .bucket(bucket.as_str())
            .copy_source(Self::encode_copy_source(
                bucket.as_str(),
                source_key.as_str(),
            ))
            .key(destination_key.as_str())
            .send()
            .await
            .map_err(|error| BpError::s3("copy object", target, error))?;
        Ok(())
    }

    pub async fn delete_all(
        s3_client: &S3Client,
        bucket: &Bucket,
        keys: &[ObjectKey],
    ) -> BpResult<()> {
        info!("delete files {:?}", keys);
        let mut errors = Vec::new();
        for chunk in keys.chunks(1000) {
            let target = Self::objects_target(bucket, chunk.len());
            let mut objects = Vec::with_capacity(chunk.len());
            for key in chunk {
                match ObjectIdentifier::builder()
                    .set_key(Some(key.as_str().to_string()))
                    .build()
                {
                    Ok(object) => objects.push(object),
                    Err(error) => errors.push(BpError::s3(
                        "build delete identifier",
                        Self::object_target(bucket, key),
                        error,
                    )),
                }
            }
            if objects.is_empty() {
                continue;
            }
            let delete = match Delete::builder().set_objects(Some(objects)).build() {
                Ok(delete) => delete,
                Err(error) => {
                    errors.push(BpError::s3("build delete request", target.clone(), error));
                    continue;
                }
            };
            let output = match s3_client
                .delete_objects()
                .bucket(bucket.as_str())
                .delete(delete)
                .send()
                .await
            {
                Ok(output) => output,
                Err(error) => {
                    errors.push(BpError::s3("delete objects", target.clone(), error));
                    continue;
                }
            };
            if !output.errors().is_empty() {
                let failures = output
                    .errors()
                    .iter()
                    .map(|error| S3DeleteObjectFailure {
                        key: error.key().unwrap_or("unknown key").to_string(),
                        code: error.code().unwrap_or("unknown code").to_string(),
                        message: error.message().map(str::to_string),
                    })
                    .collect::<Vec<_>>();
                errors.push(BpError::Storage(StorageError::S3DeleteObjects {
                    target,
                    failures,
                }));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(BpError::multiple(errors))
        }
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

    pub async fn upload_file(
        transfer_client: &TransferClient,
        bucket: &Bucket,
        key: &ObjectKey,
        file_path: &Path,
    ) -> BpResult<()> {
        let target = Self::object_target(bucket, key);
        let stream = InputStream::from_path(file_path).map_err(|error| {
            BpError::io(
                "open file for upload",
                file_path.display().to_string(),
                error,
            )
        })?;
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

    fn objects_target(bucket: &Bucket, count: usize) -> String {
        format!("s3://{bucket}/{count} objects")
    }

    /// Percent-encodes an S3 path (`bucket/key`) for use as a `CopySource`.
    ///
    /// S3 expects the copy source to be URL-encoded so that keys containing
    /// spaces, unicode, or other reserved characters are transmitted intact.
    /// Path separators are left as-is because they delimit the bucket and key.
    fn encode_copy_source(bucket: &str, key: &str) -> String {
        let mut encoded = String::with_capacity(bucket.len() + key.len() + 1);
        for component in [bucket, key] {
            if !encoded.is_empty() {
                encoded.push('/');
            }
            for &byte in component.as_bytes() {
                if Self::is_unreserved(byte) {
                    encoded.push(byte as char);
                } else {
                    const HEX: &[u8; 16] = b"0123456789ABCDEF";
                    encoded.push('%');
                    encoded.push(HEX[(byte >> 4) as usize] as char);
                    encoded.push(HEX[(byte & 0x0f) as usize] as char);
                }
            }
        }
        encoded
    }

    fn is_unreserved(byte: u8) -> bool {
        matches!(
            byte,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/'
        )
    }
}
