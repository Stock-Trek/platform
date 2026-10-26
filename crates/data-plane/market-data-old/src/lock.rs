use aws_sdk_s3::{
    Client as S3Client,
    error::{ProvideErrorMetadata, SdkError},
    operation::delete_object::DeleteObjectError,
    primitives::ByteStream,
};
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        interceptors::{Intercept, context::BeforeDeserializationInterceptorContextRef},
        runtime_components::RuntimeComponents,
    },
};
use aws_smithy_types::config_bag::ConfigBag;
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult, DataError};
use platform_metrics::market_data::MarketDataMetrics;
use platform_time::deadline::Deadline;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::Notify,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, interval, sleep},
};
use tracing::{info, warn};

use crate::s3::{Bucket, ObjectKey, S3};

const STALE_AFTER: Duration = Duration::from_secs(16 * 60);
const CLOCK_SKEW_MARGIN: Duration = Duration::from_secs(16 * 60);
const RENEW_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MAX_RENEW_FAILURES: u32 = 2;
const RENEW_SAFETY_MARGIN: Duration = Duration::from_secs(60);
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_INTERVAL: Duration = Duration::from_secs(5);
static NEXT_LOCK_TOKEN: AtomicU64 = AtomicU64::new(0);

const _: () = assert!(
    RENEW_INTERVAL.as_secs() * (MAX_RENEW_FAILURES as u64 + 1) < STALE_AFTER.as_secs(),
    "a lock must be abandoned before a contender can consider it stale"
);

#[derive(Clone, Debug, Default)]
struct ResponseDate {
    value: Arc<Mutex<Option<SystemTime>>>,
}

impl ResponseDate {
    fn get(&self) -> Option<SystemTime> {
        *self
            .value
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Intercept for ResponseDate {
    fn name(&self) -> &'static str {
        "LockResponseDate"
    }

    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.response().headers();
        let date = headers
            .get("date")
            .and_then(|value| httpdate::parse_http_date(value).ok())
            .or_else(|| {
                headers.get("x-amz-date").and_then(|value| {
                    httpdate::parse_http_date(value).ok().or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ")
                            .ok()
                            .map(|date| date.and_utc().into())
                    })
                })
            });
        let Some(date) = date else {
            return Ok(());
        };
        *self
            .value
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(date);
        Ok(())
    }
}

struct LockState {
    e_tag: String,
    released: bool,
    lost: bool,
}

pub struct LockGuard {
    s3_client: S3Client,
    bucket: Bucket,
    key: ObjectKey,
    token: String,
    state: Arc<Mutex<LockState>>,
    heartbeat: Option<JoinHandle<()>>,
    heartbeat_stop: Arc<Notify>,
    lost_notify: Arc<Notify>,
}

impl LockGuard {
    pub async fn acquire(s3_client: &S3Client, bucket: &Bucket, key: &ObjectKey) -> BpResult<Self> {
        let deadline = Instant::now() + ACQUIRE_TIMEOUT;
        Self::acquire_until(s3_client, bucket, key, deadline).await
    }

    pub async fn acquire_with_deadline(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
        deadline: Deadline,
    ) -> BpResult<Self> {
        let acquire_deadline = Instant::now() + deadline.checked_remaining()?.min(ACQUIRE_TIMEOUT);
        Self::acquire_until(s3_client, bucket, key, acquire_deadline).await
    }

    async fn acquire_until(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
        deadline: Instant,
    ) -> BpResult<Self> {
        loop {
            if let Some(guard) = Self::try_acquire(s3_client, bucket, key).await? {
                return Ok(guard);
            }
            MarketDataMetrics::record_lock_contention();
            if Self::take_over_stale(s3_client, bucket, key).await? {
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                warn!("timed out waiting for lock {}", key);
                return Err(BpError::Data(DataError::LockContention {
                    key: key.as_str().to_string(),
                }));
            }
            sleep((deadline - now).min(RETRY_INTERVAL)).await;
        }
    }

    async fn try_acquire(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
    ) -> BpResult<Option<Self>> {
        let target = S3::object_target(bucket, key);
        let token = Self::new_lock_token();
        let output = match s3_client
            .put_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .if_none_match("*")
            .body(ByteStream::from(token.clone().into_bytes()))
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if Self::is_precondition_failed(&error) => return Ok(None),
            Err(error) => return Err(BpError::s3("acquire lock", target, error)),
        };
        let Some(e_tag) = Self::resolve_e_tag(s3_client, bucket, key, output.e_tag()).await? else {
            Self::remove_unreleasable_lock(s3_client, bucket, key, &token).await;
            return Err(BpError::Data(DataError::MissingLockETag {
                key: key.as_str().to_string(),
            }));
        };
        info!("acquired lock {} ({})", key, e_tag);
        let state = Arc::new(Mutex::new(LockState {
            e_tag,
            released: false,
            lost: false,
        }));
        let heartbeat_stop = Arc::new(Notify::new());
        let lost_notify = Arc::new(Notify::new());
        let heartbeat = Self::spawn_heartbeat(
            s3_client.clone(),
            bucket.clone(),
            key.clone(),
            token.clone(),
            Arc::clone(&state),
            Arc::clone(&heartbeat_stop),
            Arc::clone(&lost_notify),
        );
        Ok(Some(Self {
            s3_client: s3_client.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            token,
            state,
            heartbeat: Some(heartbeat),
            heartbeat_stop,
            lost_notify,
        }))
    }

    fn spawn_heartbeat(
        s3_client: S3Client,
        bucket: Bucket,
        key: ObjectKey,
        token: String,
        state: Arc<Mutex<LockState>>,
        stop: Arc<Notify>,
        lost_notify: Arc<Notify>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(RENEW_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker.tick().await;
            let mut last_success = Instant::now();
            let mut consecutive_failures = 0;
            loop {
                tokio::select! {
                    biased;
                    _ = stop.notified() => break,
                    _ = ticker.tick() => {}
                }
                let e_tag = {
                    let state = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if state.released || state.lost {
                        break;
                    }
                    state.e_tag.clone()
                };
                match Self::renew(&s3_client, &bucket, &key, &e_tag, &token).await {
                    Ok(Some(e_tag)) => {
                        let mut state = state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        state.e_tag = e_tag;
                        drop(state);
                        last_success = Instant::now();
                        consecutive_failures = 0;
                    }
                    Ok(None) => {
                        let mut state = state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if !state.released {
                            state.lost = true;
                            drop(state);
                            MarketDataMetrics::record_lock_lost();
                            warn!("lock {} was taken over before it could be renewed", key);
                            lost_notify.notify_one();
                        }
                        break;
                    }
                    Err(error) => {
                        consecutive_failures += 1;
                        let next_renewal = Instant::now() + RENEW_INTERVAL;
                        let must_stop =
                            next_renewal + RENEW_SAFETY_MARGIN >= last_success + STALE_AFTER;
                        warn!(
                            "failed to renew lock {} ({consecutive_failures}/{MAX_RENEW_FAILURES} consecutive failures): {error}",
                            key
                        );
                        if consecutive_failures >= MAX_RENEW_FAILURES || must_stop {
                            let mut state = state
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            if !state.released {
                                state.lost = true;
                                drop(state);
                                MarketDataMetrics::record_lock_lost();
                                warn!(
                                    "lock {} could not be renewed; cancelling before another invocation can take it over",
                                    key
                                );
                                lost_notify.notify_one();
                            }
                            break;
                        }
                    }
                }
            }
        })
    }

    async fn renew(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
        e_tag: &str,
        token: &str,
    ) -> BpResult<Option<String>> {
        let target = S3::object_target(bucket, key);
        let output = match s3_client
            .put_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .if_match(e_tag)
            .body(ByteStream::from(token.as_bytes().to_vec()))
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if Self::is_precondition_failed(&error) => return Ok(None),
            Err(error) => return Err(BpError::s3("renew lock", target, error)),
        };
        match Self::resolve_e_tag(s3_client, bucket, key, output.e_tag()).await? {
            Some(e_tag) => Ok(Some(e_tag)),
            None => Err(BpError::Data(DataError::MissingLockETag {
                key: key.as_str().to_string(),
            })),
        }
    }

    async fn resolve_e_tag(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
        e_tag: Option<&str>,
    ) -> BpResult<Option<String>> {
        if let Some(e_tag) = e_tag.filter(|e_tag| !e_tag.is_empty()) {
            return Ok(Some(e_tag.to_string()));
        }
        let target = S3::object_target(bucket, key);
        match s3_client
            .head_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .send()
            .await
        {
            Ok(head) => Ok(head
                .e_tag()
                .filter(|e_tag| !e_tag.is_empty())
                .map(str::to_string)),
            Err(SdkError::ServiceError(error)) if error.err().is_not_found() => Ok(None),
            Err(error) => Err(BpError::s3("read lock metadata", target, error)),
        }
    }

    fn new_lock_token() -> String {
        let sequence = NEXT_LOCK_TOKEN.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        format!("locked:{}:{timestamp}:{sequence}", std::process::id())
    }

    async fn remove_unreleasable_lock(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
        token: &str,
    ) {
        let output = match s3_client
            .get_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .send()
            .await
        {
            Ok(output) => output,
            Err(SdkError::ServiceError(error)) if error.err().is_no_such_key() => return,
            Err(error) => {
                warn!(
                    "failed to inspect lock {} acquired without an ETag: {error}",
                    key
                );
                return;
            }
        };
        let e_tag = output
            .e_tag()
            .filter(|e_tag| !e_tag.is_empty())
            .map(str::to_string);
        let Some(e_tag) = e_tag else {
            warn!(
                "failed to remove lock {} acquired without an ETag: still missing an ETag",
                key
            );
            return;
        };
        let body = match output.body.collect().await {
            Ok(body) => body.into_bytes(),
            Err(error) => {
                warn!(
                    "failed to read lock {} acquired without an ETag: {error}",
                    key
                );
                return;
            }
        };
        if body.as_ref() != token.as_bytes() {
            warn!(
                "not removing lock {} because it is owned by another invocation",
                key
            );
            return;
        }
        match s3_client
            .delete_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .if_match(e_tag)
            .send()
            .await
        {
            Ok(_) => warn!("removed lock {} acquired without an ETag", key),
            Err(error) if Self::is_precondition_failed(&error) => {
                warn!("lock {} was taken over before it could be removed", key);
            }
            Err(error) => warn!(
                "failed to remove lock {} acquired without an ETag: {error}",
                key
            ),
        }
    }

    async fn take_over_stale(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
    ) -> BpResult<bool> {
        let target = S3::object_target(bucket, key);
        let response_date = ResponseDate::default();
        let head = match s3_client
            .head_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .customize()
            .interceptor(response_date.clone())
            .send()
            .await
        {
            Ok(head) => head,
            Err(SdkError::ServiceError(error)) if error.err().is_not_found() => return Ok(false),
            Err(error) => {
                return Err(BpError::s3("read lock metadata", target.clone(), error));
            }
        };
        let Some(last_modified) = head
            .last_modified()
            .and_then(|value| DateTime::from_timestamp(value.secs(), value.subsec_nanos()))
        else {
            return Ok(false);
        };
        let (now, stale_after) = match response_date.get().map(DateTime::<Utc>::from) {
            Some(server_now) => (server_now, STALE_AFTER),
            None => (Utc::now(), STALE_AFTER + CLOCK_SKEW_MARGIN),
        };
        let stale_after = chrono::Duration::from_std(stale_after).map_err(|_| {
            BpError::Data(DataError::DateTimeOutOfRange {
                operation: "convert lock staleness duration",
            })
        })?;
        if now.signed_duration_since(last_modified) < stale_after {
            return Ok(false);
        }
        let Some(e_tag) = head.e_tag().filter(|e_tag| !e_tag.is_empty()) else {
            return Ok(false);
        };
        match s3_client
            .delete_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .if_match(e_tag)
            .send()
            .await
        {
            Ok(_) => {
                info!("removed stale lock {}", key);
                Ok(true)
            }
            Err(error) if Self::is_precondition_failed(&error) => Ok(false),
            Err(error) => Err(BpError::s3("remove stale lock", target, error)),
        }
    }

    pub async fn release(&mut self) -> BpResult<()> {
        self.stop_heartbeat().await;
        let (released, lost, e_tag) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (state.released, state.lost, state.e_tag.clone())
        };
        if released {
            return Ok(());
        }
        if lost {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .released = true;
            warn!("lock {} was taken over and cannot be released", self.key);
            return Ok(());
        }
        let result = Self::delete_lock(
            &self.s3_client,
            &self.bucket,
            &self.key,
            &e_tag,
            &self.token,
        )
        .await;
        if result.is_ok() {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .released = true;
        }
        result
    }

    pub async fn wait_until_lost(&self) {
        loop {
            if self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .lost
            {
                return;
            }
            self.lost_notify.notified().await;
        }
    }

    async fn stop_heartbeat(&mut self) {
        if let Some(heartbeat) = self.heartbeat.take() {
            self.heartbeat_stop.notify_one();
            let _ = heartbeat.await;
        }
    }

    async fn delete_lock(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
        e_tag: &str,
        token: &str,
    ) -> BpResult<()> {
        let target = S3::object_target(bucket, key);
        if e_tag.is_empty() {
            return Err(BpError::Data(DataError::MissingLockETag {
                key: key.as_str().to_string(),
            }));
        }
        match Self::delete_if_match(s3_client, bucket, key, e_tag).await {
            Ok(()) => {
                info!("released lock {}", key);
                Ok(())
            }
            Err(error) if Self::is_precondition_failed(&error) => {
                match Self::owned_lock_e_tag(s3_client, bucket, key, token).await {
                    Ok(Some(current_e_tag)) if current_e_tag != e_tag => {
                        match Self::delete_if_match(s3_client, bucket, key, &current_e_tag).await {
                            Ok(()) => {
                                info!("released lock {}", key);
                                return Ok(());
                            }
                            Err(error) if Self::is_precondition_failed(&error) => {}
                            Err(error) => {
                                return Err(BpError::s3("release lock", target, error));
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(
                            "failed to verify ownership of lock {} before release: {error}",
                            key
                        );
                    }
                }
                warn!("lock {} was taken over before release", key);
                Ok(())
            }
            Err(error) => Err(BpError::s3("release lock", target, error)),
        }
    }

    async fn delete_if_match(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
        e_tag: &str,
    ) -> Result<(), Box<SdkError<DeleteObjectError>>> {
        s3_client
            .delete_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .if_match(e_tag)
            .send()
            .await
            .map(|_| ())
            .map_err(Box::new)
    }

    async fn owned_lock_e_tag(
        s3_client: &S3Client,
        bucket: &Bucket,
        key: &ObjectKey,
        token: &str,
    ) -> BpResult<Option<String>> {
        let target = S3::object_target(bucket, key);
        let output = match s3_client
            .get_object()
            .bucket(bucket.as_str())
            .key(key.as_str())
            .send()
            .await
        {
            Ok(output) => output,
            Err(SdkError::ServiceError(error)) if error.err().is_no_such_key() => return Ok(None),
            Err(error) => return Err(BpError::s3("inspect lock ownership", target, error)),
        };
        let e_tag = output
            .e_tag()
            .filter(|e_tag| !e_tag.is_empty())
            .map(str::to_string);
        let body = output
            .body
            .collect()
            .await
            .map_err(|error| BpError::s3("read lock body", target, error))?
            .into_bytes();
        if body.as_ref() != token.as_bytes() {
            return Ok(None);
        }
        Ok(e_tag)
    }

    fn is_precondition_failed<E>(error: &SdkError<E>) -> bool
    where
        E: ProvideErrorMetadata,
    {
        if error
            .raw_response()
            .is_some_and(|response| response.status().as_u16() == 412)
        {
            return true;
        }
        error
            .as_service_error()
            .and_then(|service_error| service_error.code())
            .is_some_and(|code| {
                code == "PreconditionFailed" || code == "ConditionalRequestConflict"
            })
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let lost = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.released || state.lost {
                return;
            }
            state.released = true;
            state.lost
        };
        if lost {
            return;
        }
        let s3_client = self.s3_client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let token = self.token.clone();
        let state = Arc::clone(&self.state);
        let heartbeat = self.heartbeat.take();
        self.heartbeat_stop.notify_one();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                drop(handle.spawn(async move {
                    if let Some(heartbeat) = heartbeat {
                        let _ = heartbeat.await;
                    }
                    let (lost, e_tag) = {
                        let state = state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        (state.lost, state.e_tag.clone())
                    };
                    if lost {
                        return;
                    }
                    if let Err(error) =
                        LockGuard::delete_lock(&s3_client, &bucket, &key, &e_tag, &token).await
                    {
                        warn!("failed to release lock {} on drop: {error}", key);
                    }
                }));
            }
            Err(_) => {
                warn!("no tokio runtime available to release lock {} on drop", key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::{
        config::{BehaviorVersion, Credentials, Region, retry::RetryConfig},
        primitives::SdkBody,
    };
    use aws_smithy_runtime_api::client::{
        http::{
            HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings,
            SharedHttpConnector,
        },
        orchestrator::{HttpRequest, HttpResponse},
        result::ConnectorError,
        runtime_components::RuntimeComponents,
    };
    use http::Response;
    use std::collections::VecDeque;

    const ETAG_1: &str = "\"etag-1\"";
    const ETAG_2: &str = "\"etag-2\"";

    #[derive(Clone, Debug)]
    struct FakeHttpClient {
        responses: Arc<Mutex<VecDeque<HttpResponse>>>,
        requests: Arc<Mutex<Vec<HttpRequest>>>,
    }

    impl FakeHttpClient {
        fn new(responses: Vec<HttpResponse>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl HttpConnector for FakeHttpClient {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request);
            let response = self
                .responses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front();
            HttpConnectorFuture::new(async move {
                response
                    .ok_or_else(|| ConnectorError::other("no fake response available".into(), None))
            })
        }
    }

    impl HttpClient for FakeHttpClient {
        fn http_connector(
            &self,
            _settings: &HttpConnectorSettings,
            _components: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    fn s3_response(status: u16, headers: &[(&str, &str)], body: &str) -> HttpResponse {
        let mut response = Response::builder().status(status);
        for (name, value) in headers {
            response = response.header(*name, *value);
        }
        response
            .body(SdkBody::from(body.to_owned()))
            .unwrap()
            .try_into()
            .unwrap()
    }

    fn put_response(e_tag: Option<&str>) -> HttpResponse {
        match e_tag {
            Some(e_tag) => s3_response(200, &[("etag", e_tag)], ""),
            None => s3_response(200, &[], ""),
        }
    }

    fn delete_response() -> HttpResponse {
        s3_response(204, &[], "")
    }

    fn precondition_failed_response() -> HttpResponse {
        s3_response(412, &[], "")
    }

    fn s3_client(responses: Vec<HttpResponse>) -> (S3Client, FakeHttpClient) {
        let http_client = FakeHttpClient::new(responses);
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "test-key",
                "test-secret",
                None,
                None,
                "test",
            ))
            .retry_config(RetryConfig::disabled())
            .http_client(http_client.clone())
            .build();
        (S3Client::from_conf(config), http_client)
    }

    fn bucket() -> Bucket {
        Bucket::new("test-bucket")
    }

    fn key() -> ObjectKey {
        ObjectKey::new("locks/market-data")
    }

    fn request_body(request: &HttpRequest) -> String {
        String::from_utf8(request.body().bytes().unwrap_or_default().to_vec()).unwrap()
    }

    fn requests(http_client: &FakeHttpClient) -> std::sync::MutexGuard<'_, Vec<HttpRequest>> {
        http_client
            .requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[tokio::test]
    async fn acquire_and_release_use_conditional_requests() {
        let (s3_client, http_client) =
            s3_client(vec![put_response(Some(ETAG_1)), delete_response()]);
        let mut guard = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        guard.release().await.unwrap();
        guard.release().await.unwrap();

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method(), "PUT");
        assert_eq!(requests[0].headers().get("if-none-match"), Some("*"));
        assert!(request_body(&requests[0]).starts_with("locked:"));
        assert_eq!(requests[1].method(), "DELETE");
        assert_eq!(requests[1].headers().get("if-match"), Some(ETAG_1));
    }

    #[tokio::test]
    async fn acquire_reports_contention_when_lock_is_held() {
        let (s3_client, http_client) = s3_client(vec![
            precondition_failed_response(),
            s3_response(404, &[], ""),
        ]);

        let error = LockGuard::acquire_until(&s3_client, &bucket(), &key(), Instant::now())
            .await
            .err()
            .expect("lock acquisition should fail");

        assert!(matches!(
            error,
            BpError::Data(DataError::LockContention { .. })
        ));
        let requests = requests(&http_client);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method(), "PUT");
        assert_eq!(requests[1].method(), "HEAD");
    }

    #[tokio::test]
    async fn stale_lock_is_removed_before_acquiring() {
        let stale_last_modified = (Utc::now() - chrono::Duration::hours(1))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let (s3_client, http_client) = s3_client(vec![
            precondition_failed_response(),
            s3_response(
                200,
                &[("etag", ETAG_1), ("last-modified", &stale_last_modified)],
                "",
            ),
            delete_response(),
            put_response(Some(ETAG_2)),
            delete_response(),
        ]);

        let mut guard = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .unwrap();
        guard.release().await.unwrap();

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0].method(), "PUT");
        assert_eq!(requests[1].method(), "HEAD");
        assert_eq!(requests[2].method(), "DELETE");
        assert_eq!(requests[2].headers().get("if-match"), Some(ETAG_1));
        assert_eq!(requests[3].method(), "PUT");
        assert_eq!(requests[4].method(), "DELETE");
        assert_eq!(requests[4].headers().get("if-match"), Some(ETAG_2));
    }

    #[tokio::test]
    async fn stale_detection_uses_s3_response_date() {
        let last_modified = (Utc::now() - chrono::Duration::minutes(20))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let response_date = SystemTime::now() - Duration::from_secs(10 * 60);
        let response_date = httpdate::fmt_http_date(response_date);
        let (s3_client, http_client) = s3_client(vec![s3_response(
            200,
            &[
                ("etag", ETAG_1),
                ("last-modified", &last_modified),
                ("date", &response_date),
            ],
            "",
        )]);

        let removed = LockGuard::take_over_stale(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        assert!(!removed);
        let requests = requests(&http_client);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method(), "HEAD");
    }

    #[tokio::test]
    async fn stale_detection_uses_s3_amz_date() {
        let last_modified = (Utc::now() - chrono::Duration::minutes(20))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let response_date = (Utc::now() - chrono::Duration::minutes(10))
            .format("%Y%m%dT%H%M%SZ")
            .to_string();
        let (s3_client, http_client) = s3_client(vec![s3_response(
            200,
            &[
                ("etag", ETAG_1),
                ("last-modified", &last_modified),
                ("x-amz-date", &response_date),
            ],
            "",
        )]);

        let removed = LockGuard::take_over_stale(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        assert!(!removed);
        let requests = requests(&http_client);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method(), "HEAD");
    }

    #[tokio::test]
    async fn stale_detection_applies_skew_margin_without_response_date() {
        let last_modified = (Utc::now() - chrono::Duration::minutes(20))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let (s3_client, http_client) = s3_client(vec![s3_response(
            200,
            &[("etag", ETAG_1), ("last-modified", &last_modified)],
            "",
        )]);

        let removed = LockGuard::take_over_stale(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        assert!(!removed);
        let requests = requests(&http_client);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method(), "HEAD");
    }

    #[tokio::test]
    async fn acquire_reads_etag_from_head_when_put_response_omits_it() {
        let (s3_client, http_client) = s3_client(vec![
            put_response(None),
            s3_response(200, &[("etag", ETAG_1)], ""),
            delete_response(),
        ]);

        let mut guard = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .unwrap();
        guard.release().await.unwrap();

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method(), "PUT");
        assert_eq!(requests[1].method(), "HEAD");
        assert_eq!(requests[2].method(), "DELETE");
        assert_eq!(requests[2].headers().get("if-match"), Some(ETAG_1));
    }

    #[tokio::test]
    async fn acquire_fails_when_lock_has_no_etag() {
        let (s3_client, http_client) = s3_client(vec![
            put_response(None),
            s3_response(200, &[], ""),
            s3_response(200, &[("etag", ETAG_1)], ""),
        ]);

        let error = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .err()
            .expect("lock acquisition should fail");

        assert!(matches!(
            error,
            BpError::Data(DataError::MissingLockETag { .. })
        ));
        let requests = requests(&http_client);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method(), "PUT");
        assert_eq!(requests[1].method(), "HEAD");
        assert_eq!(requests[2].method(), "GET");
    }

    #[tokio::test]
    async fn renew_uses_current_etag() {
        let (s3_client, http_client) = s3_client(vec![put_response(Some(ETAG_2))]);

        let e_tag = LockGuard::renew(&s3_client, &bucket(), &key(), ETAG_1, "locked:test")
            .await
            .unwrap();

        assert_eq!(e_tag.as_deref(), Some(ETAG_2));
        let requests = requests(&http_client);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method(), "PUT");
        assert_eq!(requests[0].headers().get("if-match"), Some(ETAG_1));
        assert!(request_body(&requests[0]).starts_with("locked:"));
    }

    #[tokio::test]
    async fn renew_returns_none_when_lock_was_taken_over() {
        let (s3_client, _http_client) = s3_client(vec![precondition_failed_response()]);

        let e_tag = LockGuard::renew(&s3_client, &bucket(), &key(), ETAG_1, "locked:test")
            .await
            .unwrap();

        assert_eq!(e_tag, None);
    }

    #[tokio::test]
    async fn release_does_not_delete_a_lost_lock() {
        let (s3_client, http_client) = s3_client(vec![]);
        let mut guard = LockGuard {
            s3_client,
            bucket: bucket(),
            key: key(),
            token: "locked:test".to_string(),
            state: Arc::new(Mutex::new(LockState {
                e_tag: ETAG_1.to_string(),
                released: false,
                lost: true,
            })),
            heartbeat: None,
            heartbeat_stop: Arc::new(Notify::new()),
            lost_notify: Arc::new(Notify::new()),
        };

        guard.release().await.unwrap();

        assert!(requests(&http_client).is_empty());
    }

    #[tokio::test]
    async fn release_refreshes_stale_etag_when_lock_is_still_owned() {
        let token = "locked:test".to_string();
        let (s3_client, http_client) = s3_client(vec![
            precondition_failed_response(),
            s3_response(200, &[("etag", ETAG_2)], &token),
            delete_response(),
        ]);
        let mut guard = LockGuard {
            s3_client,
            bucket: bucket(),
            key: key(),
            token,
            state: Arc::new(Mutex::new(LockState {
                e_tag: ETAG_1.to_string(),
                released: false,
                lost: false,
            })),
            heartbeat: None,
            heartbeat_stop: Arc::new(Notify::new()),
            lost_notify: Arc::new(Notify::new()),
        };

        guard.release().await.unwrap();

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method(), "DELETE");
        assert_eq!(requests[0].headers().get("if-match"), Some(ETAG_1));
        assert_eq!(requests[1].method(), "GET");
        assert_eq!(requests[2].method(), "DELETE");
        assert_eq!(requests[2].headers().get("if-match"), Some(ETAG_2));
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_renews_lock_and_release_uses_new_etag() {
        let (s3_client, http_client) = s3_client(vec![
            put_response(Some(ETAG_1)),
            put_response(Some(ETAG_2)),
            delete_response(),
        ]);
        let mut guard = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        tokio::task::yield_now().await;
        tokio::time::advance(RENEW_INTERVAL + Duration::from_secs(1)).await;
        for _ in 0..100 {
            if requests(&http_client).len() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        guard.release().await.unwrap();

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1].method(), "PUT");
        assert_eq!(requests[1].headers().get("if-match"), Some(ETAG_1));
        assert_eq!(requests[2].method(), "DELETE");
        assert_eq!(requests[2].headers().get("if-match"), Some(ETAG_2));
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_notifies_when_lock_is_lost() {
        let (s3_client, http_client) = s3_client(vec![
            put_response(Some(ETAG_1)),
            precondition_failed_response(),
        ]);
        let mut guard = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        tokio::task::yield_now().await;
        tokio::time::advance(RENEW_INTERVAL + Duration::from_secs(1)).await;
        tokio::time::timeout(Duration::from_secs(1), guard.wait_until_lost())
            .await
            .expect("lock loss should be reported");
        guard.release().await.unwrap();

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method(), "PUT");
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_recovers_after_a_transient_renewal_failure() {
        let (s3_client, http_client) = s3_client(vec![
            put_response(Some(ETAG_1)),
            s3_response(500, &[], ""),
            put_response(Some(ETAG_2)),
            delete_response(),
        ]);
        let mut guard = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        tokio::task::yield_now().await;
        tokio::time::advance(RENEW_INTERVAL + Duration::from_secs(1)).await;
        for _ in 0..100 {
            if requests(&http_client).len() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        tokio::time::advance(RENEW_INTERVAL + Duration::from_secs(1)).await;
        for _ in 0..100 {
            if requests(&http_client).len() >= 3 {
                break;
            }
            tokio::task::yield_now().await;
        }
        guard.release().await.unwrap();

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[1].method(), "PUT");
        assert_eq!(requests[1].headers().get("if-match"), Some(ETAG_1));
        assert_eq!(requests[2].method(), "PUT");
        assert_eq!(requests[2].headers().get("if-match"), Some(ETAG_1));
        assert_eq!(requests[3].method(), "DELETE");
        assert_eq!(requests[3].headers().get("if-match"), Some(ETAG_2));
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_marks_lock_lost_after_repeated_transient_renewal_failures() {
        let (s3_client, http_client) = s3_client(vec![
            put_response(Some(ETAG_1)),
            s3_response(500, &[], ""),
            s3_response(500, &[], ""),
        ]);
        let mut guard = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        tokio::task::yield_now().await;
        tokio::time::advance(RENEW_INTERVAL + Duration::from_secs(1)).await;
        for _ in 0..100 {
            if requests(&http_client).len() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        tokio::time::advance(RENEW_INTERVAL + Duration::from_secs(1)).await;
        tokio::time::timeout(Duration::from_secs(1), guard.wait_until_lost())
            .await
            .expect("repeated renewal failures should abandon the lock");
        guard.release().await.unwrap();

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1].method(), "PUT");
        assert_eq!(requests[2].method(), "PUT");
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_stops_before_a_delayed_renewal_can_expire_the_lease() {
        let (s3_client, http_client) =
            s3_client(vec![put_response(Some(ETAG_1)), s3_response(500, &[], "")]);
        let mut guard = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        tokio::task::yield_now().await;
        tokio::time::advance(RENEW_INTERVAL * 2 + RENEW_SAFETY_MARGIN + Duration::from_secs(1))
            .await;
        tokio::time::timeout(Duration::from_secs(1), guard.wait_until_lost())
            .await
            .expect("a renewal that would run past the lease should abandon the lock");
        guard.release().await.unwrap();

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method(), "PUT");
    }

    #[tokio::test]
    async fn dropping_guard_releases_lock() {
        let (s3_client, http_client) =
            s3_client(vec![put_response(Some(ETAG_1)), delete_response()]);
        let guard = LockGuard::acquire(&s3_client, &bucket(), &key())
            .await
            .unwrap();

        drop(guard);
        for _ in 0..100 {
            if requests(&http_client).len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let requests = requests(&http_client);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method(), "DELETE");
        assert_eq!(requests[1].headers().get("if-match"), Some(ETAG_1));
    }
}
