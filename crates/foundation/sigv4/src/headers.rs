pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const AMZ_CONTENT_SHA256: &str = "x-amz-content-sha256";
pub const AMZ_DATE: &str = "x-amz-date";
pub const AMZ_SECURITY_TOKEN: &str = "x-amz-security-token";
pub const AUTHORIZATION: &str = "authorization";
pub const HOST: &str = "host";

pub struct SigV4Headers;

impl SigV4Headers {
    pub fn generated(name: &str) -> bool {
        matches!(
            name,
            AUTHORIZATION | AMZ_DATE | AMZ_SECURITY_TOKEN | AMZ_CONTENT_SHA256
        )
    }
}
