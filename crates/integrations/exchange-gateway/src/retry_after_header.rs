use std::time::{Duration, SystemTime};

pub(crate) struct RetryAfter;

impl RetryAfter {
    pub(crate) fn from_headers(headers: &[(String, String)]) -> Option<Duration> {
        headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("Retry-After"))
            .and_then(|(_, value)| Self::parse_retry_after(value))
    }
    pub(crate) fn parse_retry_after(value: &str) -> Option<Duration> {
        let value = value.trim();
        if let Ok(seconds) = value.parse::<u64>() {
            return (seconds > 0).then(|| Duration::from_secs(seconds));
        }
        let retry_at = httpdate::parse_http_date(value).ok()?;
        retry_at
            .duration_since(SystemTime::now())
            .ok()
            .filter(|duration| !duration.is_zero())
    }
}
