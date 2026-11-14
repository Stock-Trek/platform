use crate::{
    SigV4Registry, SigV4Request, VerifiedCaller,
    authorization::ParsedAuthorization,
    date::AmzDate,
    headers::{AMZ_CONTENT_SHA256, AMZ_DATE, AMZ_SECURITY_TOKEN, AUTHORIZATION, HOST},
};
use aws_sigv4::{
    http_request::{SignableBody, SignableRequest, SigningParams, sign},
    sign::v4,
};
use platform_errors::PlatformResult;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::SystemTime};
use subtle::ConstantTimeEq;
use time::OffsetDateTime;

pub struct SigV4Verifier {
    region: String,
    service: String,
    max_clock_skew_secs: i64,
}

impl SigV4Verifier {
    pub fn new(
        region: impl Into<String>,
        service: impl Into<String>,
        max_clock_skew_secs: i64,
    ) -> Self {
        Self {
            region: region.into(),
            service: service.into(),
            max_clock_skew_secs,
        }
    }

    pub fn verify(
        &self,
        registry: &SigV4Registry,
        request: &SigV4Request<'_>,
        now: SystemTime,
    ) -> PlatformResult<VerifiedCaller> {
        let headers = Self::normalize_headers(request.headers());
        let authorization = Self::header(&headers, AUTHORIZATION)?;
        let parsed = ParsedAuthorization::parse(authorization)?;
        if parsed.region != self.region || parsed.service != self.service {
            return Err(ParsedAuthorization::invalid(
                "credential scope does not match this service",
            ));
        }
        let (credentials, owner) = registry
            .caller(&parsed.access_key_id)
            .ok_or_else(|| ParsedAuthorization::invalid("unknown access key id"))?;
        let amz_date = Self::header(&headers, AMZ_DATE)?;
        let request_time = AmzDate::parse(amz_date)?;
        let (request_date, _) = AmzDate::format(request_time);
        if request_date != parsed.date {
            return Err(ParsedAuthorization::invalid(
                "credential date does not match x-amz-date",
            ));
        }
        let request_seconds = OffsetDateTime::from(request_time).unix_timestamp();
        let now_seconds = OffsetDateTime::from(now).unix_timestamp();
        if (now_seconds - request_seconds).abs() > self.max_clock_skew_secs {
            return Err(ParsedAuthorization::invalid(
                "request timestamp is outside the allowed clock skew",
            ));
        }
        if ![HOST, AMZ_DATE]
            .iter()
            .all(|header| parsed.signed_headers.iter().any(|name| name == header))
        {
            return Err(ParsedAuthorization::invalid(
                "host and x-amz-date must be signed",
            ));
        }
        Self::verify_session_token(&headers, credentials.session_token())?;
        let payload_hash = hex::encode(Sha256::digest(request.body()));
        if let Some(header) = headers.get(AMZ_CONTENT_SHA256)
            && header != &payload_hash
        {
            return Err(ParsedAuthorization::invalid(
                "x-amz-content-sha256 does not match the request body",
            ));
        }
        let selected = parsed
            .signed_headers
            .iter()
            .filter(|name| !matches!(name.as_str(), AUTHORIZATION | AMZ_DATE | AMZ_SECURITY_TOKEN))
            .filter_map(|name| {
                headers
                    .get(name)
                    .map(|value| (name.as_str(), value.as_str()))
            })
            .collect::<Vec<_>>();
        let identity = credentials.identity();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(request_time)
            .settings(Default::default())
            .build()
            .map_err(|error| ParsedAuthorization::invalid(error.to_string()))?;
        let signable = SignableRequest::new(
            request.method(),
            request.url(),
            selected.into_iter(),
            SignableBody::Bytes(request.body()),
        )
        .map_err(|error| ParsedAuthorization::invalid(error.to_string()))?;
        let output = sign(signable, &SigningParams::V4(params))
            .map_err(|error| ParsedAuthorization::invalid(error.to_string()))?;
        let expected = output
            .output()
            .headers()
            .find(|(name, _)| *name == AUTHORIZATION)
            .map(|(_, value)| value)
            .ok_or_else(|| ParsedAuthorization::invalid("signature was not produced"))?;
        let expected = ParsedAuthorization::parse(expected)?;
        if expected.signed_headers != parsed.signed_headers {
            return Err(ParsedAuthorization::invalid(
                "signed headers do not match the request",
            ));
        }
        Self::verify_signature(&parsed.signature, &expected.signature)?;
        Ok(VerifiedCaller::new(parsed.access_key_id, owner.to_string()))
    }

    fn normalize_headers(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        headers
            .iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
            .collect()
    }

    fn header<'a>(headers: &'a BTreeMap<String, String>, name: &str) -> PlatformResult<&'a str> {
        headers
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| ParsedAuthorization::invalid(format!("missing required header {name}")))
    }

    fn verify_session_token(
        headers: &BTreeMap<String, String>,
        expected: Option<&str>,
    ) -> PlatformResult<()> {
        match (headers.get(AMZ_SECURITY_TOKEN), expected) {
            (Some(actual), Some(expected))
                if actual.as_bytes().ct_eq(expected.as_bytes()).into() =>
            {
                Ok(())
            }
            (None, None) => Ok(()),
            _ => Err(ParsedAuthorization::invalid(
                "x-amz-security-token does not match the credential",
            )),
        }
    }

    fn verify_signature(actual: &str, expected: &str) -> PlatformResult<()> {
        let actual = hex::decode(actual)
            .map_err(|_| ParsedAuthorization::invalid("signature is not valid hex"))?;
        let expected = hex::decode(expected)
            .map_err(|_| ParsedAuthorization::invalid("expected signature is not valid hex"))?;
        if actual.len() != expected.len() || !bool::from(actual.ct_eq(&expected)) {
            return Err(ParsedAuthorization::invalid("signature does not match"));
        }
        Ok(())
    }
}
