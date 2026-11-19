use crate::api::ApiRequest;
use crate::api::Problem;
use chrono::{DateTime, Duration, Utc};
use http_body_util::Full;
use hyper::{Response, body::Bytes};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const RETENTION: Duration = Duration::hours(24);

struct Record {
    body_hash: u64,
    expires_at: DateTime<Utc>,
    status: hyper::StatusCode,
    headers: hyper::HeaderMap,
    body: Bytes,
}

pub(crate) struct IdempotencyStore {
    records: Mutex<HashMap<String, Record>>,
}

impl IdempotencyStore {
    pub(crate) fn new() -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn apply(
        &self,
        request: &ApiRequest,
        owner: &str,
        endpoint: &str,
        create: impl FnOnce() -> Result<Response<Full<Bytes>>, Problem>,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let Some(key) = request.idempotency_key() else {
            return create();
        };
        let scoped = format!("{owner}:{endpoint}:{key}");
        let body_hash = Self::hash(request.body());
        let now = Utc::now();
        let mut records = self.records.lock();
        records.retain(|_, record| record.expires_at > now);
        if let Some(record) = records.get(&scoped) {
            if record.body_hash == body_hash {
                return Ok(Self::replay(record));
            }
            return Err(Problem::idempotency_conflict());
        }
        let response = create()?;
        let (parts, body) = response.into_parts();
        let bytes = body.into_inner().unwrap_or_default();
        records.insert(
            scoped,
            Record {
                body_hash,
                expires_at: now + RETENTION,
                status: parts.status,
                headers: parts.headers.clone(),
                body: bytes.clone(),
            },
        );
        Ok(Response::from_parts(parts, Full::new(bytes)))
    }

    fn replay(record: &Record) -> Response<Full<Bytes>> {
        let mut response = Response::builder()
            .status(record.status)
            .body(Full::new(record.body.clone()))
            .expect("replay response");
        *response.headers_mut() = record.headers.clone();
        response
    }

    fn hash(body: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        hasher.finish()
    }
}
