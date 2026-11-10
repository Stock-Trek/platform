pub(crate) const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub(crate) const AMZ_CONTENT_SHA256: &str = "x-amz-content-sha256";
pub(crate) const AMZ_DATE: &str = "x-amz-date";
pub(crate) const AMZ_SECURITY_TOKEN: &str = "x-amz-security-token";
pub(crate) const AUTHORIZATION: &str = "authorization";
pub(crate) const HOST: &str = "host";

pub(crate) struct SigV4Headers;

impl SigV4Headers {
    pub(crate) fn generated(name: &str) -> bool {
        matches!(
            name,
            AUTHORIZATION | AMZ_DATE | AMZ_SECURITY_TOKEN | AMZ_CONTENT_SHA256
        )
    }
}
