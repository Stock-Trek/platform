use crate::api::Problem;
use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper::{
    HeaderMap, Method, Request,
    body::{Bytes, Incoming},
    header::CONTENT_LENGTH,
};
use serde::de::DeserializeOwned;
use std::collections::HashMap;

const MAX_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub struct ApiRequest {
    method: Method,
    path: String,
    segments: Vec<String>,
    query: HashMap<String, String>,
    headers: HeaderMap,
    body: Bytes,
}

impl ApiRequest {
    pub async fn parse(request: Request<Incoming>) -> Result<Self, Problem> {
        let (parts, body) = request.into_parts();
        let too_large = parts
            .headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|length| length > MAX_BODY_BYTES);
        if too_large {
            return Err(Problem::payload_too_large());
        }
        let collected = Limited::new(body, MAX_BODY_BYTES)
            .collect()
            .await
            .map_err(|error| {
                if error.downcast_ref::<LengthLimitError>().is_some() {
                    Problem::payload_too_large()
                } else {
                    Problem::validation_failed(error.to_string())
                }
            })?;
        Ok(Self::from_parts(
            parts.method,
            parts.uri,
            parts.headers,
            collected.to_bytes(),
        ))
    }

    pub fn from_parts(method: Method, uri: hyper::Uri, headers: HeaderMap, body: Bytes) -> Self {
        let path = uri.path().to_string();
        let mut segments: Vec<String> = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(str::to_string)
            .collect();
        let mut query = HashMap::new();
        for pair in uri.query().unwrap_or_default().split('&') {
            if let Some((name, value)) = pair.split_once('=') {
                query
                    .entry(name.to_string())
                    .or_insert_with(|| value.to_string());
            }
        }
        if segments.first().map(String::as_str) == Some("v1") {
            segments.remove(0);
        }
        Self {
            method,
            path,
            segments,
            query,
            headers,
            body,
        }
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn segments(&self) -> Vec<&str> {
        self.segments.iter().map(String::as_str).collect()
    }

    pub fn query(&self, name: &str) -> Option<&str> {
        self.query.get(name).map(String::as_str)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn bearer(&self) -> Result<&str, Problem> {
        let value = self
            .header("authorization")
            .ok_or_else(|| Problem::unauthorized("bearer token is required"))?;
        value
            .strip_prefix("Bearer ")
            .filter(|token| !token.trim().is_empty())
            .ok_or_else(|| Problem::unauthorized("authorization must use the Bearer scheme"))
    }

    pub fn idempotency_key(&self) -> Option<String> {
        self.header("idempotency-key")
            .filter(|key| !key.is_empty())
            .map(str::to_string)
    }

    pub fn if_match(&self) -> Result<Option<u64>, Problem> {
        let Some(value) = self.header("if-match") else {
            return Ok(None);
        };
        let version =
            value.trim().trim_matches('"').parse::<u64>().map_err(|_| {
                Problem::validation_failed("If-Match must be a quoted version number")
            })?;
        Ok(Some(version))
    }

    pub fn request_id(&self) -> Option<String> {
        self.header("x-request-id")
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    }

    pub fn origin(&self) -> Option<&str> {
        self.header("origin")
    }

    pub fn json<T: DeserializeOwned>(&self) -> Result<T, Problem> {
        if self.body.is_empty() {
            return Err(Problem::validation_failed(
                "a JSON request body is required",
            ));
        }
        serde_json::from_slice(&self.body)
            .map_err(|error| Problem::validation_failed(error.to_string()))
    }

    pub fn json_optional<T: DeserializeOwned>(&self) -> Result<Option<T>, Problem> {
        if self.body.is_empty() {
            return Ok(None);
        }
        self.json().map(Some)
    }

    pub fn require_json_content_type(&self) -> Result<(), Problem> {
        let content_type = self.header("content-type").unwrap_or_default();
        if content_type.starts_with("application/json")
            || content_type.starts_with("application/merge-patch+json")
        {
            Ok(())
        } else {
            Err(Problem::validation_failed(
                "requests must use a JSON content type",
            ))
        }
    }
}
