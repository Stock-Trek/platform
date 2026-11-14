use crate::{SigV4Registry, SigV4Request, SigV4Signer, VerifiedCaller, verifier::SigV4Verifier};
use platform_config::{EnvVars, SIGV4_CREDENTIALS, SIGV4_REGION, SIGV4_SERVICE};
use platform_errors::{PlatformError, PlatformResult};
use std::time::SystemTime;

const DEFAULT_MAX_CLOCK_SKEW_SECS: i64 = 300;

#[derive(Debug, Clone)]
pub struct SigV4Config {
    registry: SigV4Registry,
    region: String,
    service: String,
    max_clock_skew_secs: i64,
}

impl SigV4Config {
    pub fn new(
        registry: SigV4Registry,
        region: impl Into<String>,
        service: impl Into<String>,
    ) -> PlatformResult<Self> {
        Self::with_clock_skew(registry, region, service, DEFAULT_MAX_CLOCK_SKEW_SECS)
    }

    pub fn with_clock_skew(
        registry: SigV4Registry,
        region: impl Into<String>,
        service: impl Into<String>,
        max_clock_skew_secs: i64,
    ) -> PlatformResult<Self> {
        let region = region.into();
        let service = service.into();
        Self::validate_name(SIGV4_REGION, &region)?;
        Self::validate_name(SIGV4_SERVICE, &service)?;
        if max_clock_skew_secs < 0 {
            return Err(PlatformError::config_invalid(
                "sigv4_max_clock_skew_secs",
                "must not be negative",
            ));
        }
        Ok(Self {
            registry,
            region,
            service,
            max_clock_skew_secs,
        })
    }

    pub fn from_env() -> PlatformResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> PlatformResult<Self> {
        let registry = SigV4Registry::from_json(&vars.required(SIGV4_CREDENTIALS)?)?;
        Self::new(
            registry,
            vars.required(SIGV4_REGION)?,
            vars.required(SIGV4_SERVICE)?,
        )
    }

    pub fn signer_for_owner(&self, owner: &str) -> PlatformResult<SigV4Signer> {
        Ok(SigV4Signer::new(
            self.registry.credentials_for_owner(owner)?.clone(),
            self.region.clone(),
            self.service.clone(),
        ))
    }

    pub fn verify(
        &self,
        request: &SigV4Request<'_>,
        now: SystemTime,
    ) -> PlatformResult<VerifiedCaller> {
        SigV4Verifier::new(
            self.region.clone(),
            self.service.clone(),
            self.max_clock_skew_secs,
        )
        .verify(&self.registry, request, now)
    }

    pub fn registry(&self) -> &SigV4Registry {
        &self.registry
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    pub fn max_clock_skew_secs(&self) -> i64 {
        self.max_clock_skew_secs
    }

    fn validate_name(name: &'static str, value: &str) -> PlatformResult<()> {
        if value.is_empty() {
            return Err(PlatformError::config_invalid(name, "must not be empty"));
        }
        if value.trim() != value || value.chars().any(char::is_control) {
            return Err(PlatformError::config_invalid(
                name,
                "must not contain surrounding whitespace or control characters",
            ));
        }
        Ok(())
    }
}
