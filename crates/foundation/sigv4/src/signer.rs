use crate::{
    SigV4Credentials,
    headers::{HOST, SigV4Headers},
};
use aws_sigv4::{
    http_request::{SignableBody, SignableRequest, SigningParams, sign},
    sign::v4,
};
use platform_errors::{BuildError, BuildResult};
use std::{collections::BTreeMap, time::SystemTime};
use url::{Host, Url};

#[derive(Debug, Clone)]
pub struct SigV4Signer {
    credentials: SigV4Credentials,
    region: String,
    service: String,
}

impl SigV4Signer {
    pub fn new(
        credentials: SigV4Credentials,
        region: impl Into<String>,
        service: impl Into<String>,
    ) -> Self {
        Self {
            credentials,
            region: region.into(),
            service: service.into(),
        }
    }

    pub fn sign(
        &self,
        method: &str,
        url: &str,
        headers: &mut BTreeMap<String, String>,
        body: &[u8],
        now: SystemTime,
    ) -> BuildResult<()> {
        let url = Url::parse(url).map_err(|error| BuildError::config_invalid_source("url", error))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(BuildError::config_invalid(
                "url",
                "SigV4 signing requires an http or https URL",
            ));
        }
        let host = Self::host_header(&url)?;
        let normalized = std::mem::take(headers)
            .into_iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
            .filter(|(name, _)| !SigV4Headers::generated(name))
            .collect::<BTreeMap<_, _>>();
        headers.extend(normalized);
        headers.insert(HOST.to_string(), host);
        let identity = self.credentials.identity();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(now)
            .settings(Default::default())
            .build()
            .map_err(|error| BuildError::config_invalid_source("sigv4", error))?;
        let request = SignableRequest::new(
            method,
            url.as_str(),
            headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
            SignableBody::Bytes(body),
        )
        .map_err(BuildError::external)?;
        let output = sign(request, &SigningParams::V4(params)).map_err(BuildError::external)?;
        for (name, value) in output.output().headers() {
            headers.insert(name.to_string(), value.to_string());
        }
        Ok(())
    }

    fn host_header(url: &Url) -> BuildResult<String> {
        let host = match url.host() {
            Some(Host::Ipv6(address)) => format!("[{address}]"),
            Some(host) => host.to_string(),
            None => return Err(BuildError::config_invalid("url", "must contain a host")),
        };
        Ok(match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host,
        })
    }
}
