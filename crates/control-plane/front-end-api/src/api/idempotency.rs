use crate::api::ApiRequest;
use crate::api::Problem;
use http_body_util::Full;
use hyper::{Response, body::Bytes};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

struct Record {
    body_hash: u64,
    status: hyper::StatusCode,
    headers: hyper::HeaderMap,
    body: Bytes,
}

pub struct IdempotencyStore {
    records: Mutex<HashMap<String, Record>>,
}

// TODO: retain keys for 24 hours in a durable store and evict expired entries.

impl IdempotencyStore {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
        }
    }

    pub fn apply(
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
        let mut records = self.records.lock();
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
