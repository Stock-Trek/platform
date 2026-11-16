use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use bytes::Bytes;
use platform_aws::Aws;
use platform_errors::{PlatformError, PlatformResult};
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Object {
    pub key: ObjectKey,
    pub last_modified_millis: Option<i64>,
    pub size: i64,
}

impl S3Object {
    pub fn last_modified_millis(&self) -> Option<i64> {
        self.last_modified_millis
    }
}

pub struct S3;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct S3ObjectPage {
    pub objects: Vec<S3Object>,
    pub next_continuation_token: Option<String>,
}

impl S3 {
    pub async fn s3_client() -> S3Client {
        Aws::s3_client(&Aws::load_default_sdk_config().await)
    }

    pub async fn list_objects(
        s3_client: &S3Client,
        bucket: &Bucket,
        prefix: &ObjectKey,
    ) -> PlatformResult<Vec<S3Object>> {
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
            let page = page_result
                .map_err(|error| PlatformError::s3("list objects", target.clone(), error))?;
            files.extend(page.contents().iter().filter_map(|object| {
                object.key().map(|key| S3Object {
                    key: ObjectKey::new(key),
                    last_modified_millis: object.last_modified().map(Self::modified_millis),
                    size: object.size().unwrap_or_default(),
                })
            }));
        }
        Ok(files)
    }

    pub async fn list_objects_page(
        s3_client: &S3Client,
        bucket: &Bucket,
        prefix: &ObjectKey,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        max_keys: i32,
    ) -> PlatformResult<S3ObjectPage> {
        let target = Self::object_target(bucket, prefix);
        let mut request = s3_client
            .list_objects_v2()
            .bucket(bucket.as_str())
            .prefix(prefix.as_str())
            .max_keys(max_keys.clamp(1, 1000));
        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        } else if let Some(key) = start_after {
            request = request.start_after(key);
        }
        let page = request
            .send()
            .await
            .map_err(|error| PlatformError::s3("list an object page", target, error))?;
        let objects = page
            .contents()
            .iter()
            .filter_map(|object| {
                object.key().map(|key| S3Object {
                    key: ObjectKey::new(key),
                    last_modified_millis: object.last_modified().map(Self::modified_millis),
                    size: object.size().unwrap_or_default(),
                })
            })
            .collect();
        Ok(S3ObjectPage {
            objects,
            next_continuation_token: page.next_continuation_token().map(str::to_string),
        })
    }

    pub async fn upload_bytes(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> PlatformResult<()> {
        let target = Self::object_target(bucket, key);
        info!("upload bytes for key {}", key);
        s3_client
            .put_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|error| PlatformError::s3("upload object", target, error))?;
        Ok(())
    }

    #[allow(unused)]
    pub async fn download_bytes(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
    ) -> PlatformResult<Bytes> {
        let target = Self::object_target(bucket, key);
        let output = s3_client
            .get_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .send()
            .await
            .map_err(|error| PlatformError::s3("get object", target.clone(), error))?;
        output
            .body
            .collect()
            .await
            .map(|data| data.into_bytes())
            .map_err(|error| PlatformError::s3("read object body", target, error))
    }

    pub async fn delete_object(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
    ) -> PlatformResult<()> {
        let target = Self::object_target(bucket, key);
        info!("delete object for key {}", key);
        s3_client
            .delete_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .send()
            .await
            .map_err(|error| PlatformError::s3("delete object", target, error))?;
        Ok(())
    }

    pub fn object_target(bucket: &Bucket, key: &ObjectKey) -> String {
        format!("s3://{bucket}/{key}")
    }

    pub fn parse_uri(uri: &str) -> PlatformResult<(Bucket, ObjectKey)> {
        let path = uri.strip_prefix("s3://").ok_or_else(|| {
            PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("expected an s3:// URI, got {uri}"),
            ))
        })?;
        let (bucket, key) = path.split_once('/').ok_or_else(|| {
            PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("expected an s3://bucket/key URI, got {uri}"),
            ))
        })?;
        if bucket.is_empty() || key.is_empty() {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("expected an s3://bucket/key URI, got {uri}"),
            )));
        }
        Ok((Bucket::new(bucket), ObjectKey::new(key)))
    }

    fn modified_millis(value: &aws_sdk_s3::primitives::DateTime) -> i64 {
        value
            .secs()
            .saturating_mul(1000)
            .saturating_add(i64::from(value.subsec_nanos() / 1_000_000))
    }
}
