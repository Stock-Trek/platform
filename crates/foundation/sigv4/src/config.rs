use crate::{SigV4Registry, SigV4Request, SigV4Signer, VerifiedCaller, verifier::SigV4Verifier};
use platform_config::{
    env_names::{SIGV4_CREDENTIALS, SIGV4_REGION, SIGV4_SERVICE},
    env_vars::EnvVars,
};
use platform_errors::{BpError, BpResult};
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
    ) -> BpResult<Self> {
        Self::with_clock_skew(registry, region, service, DEFAULT_MAX_CLOCK_SKEW_SECS)
    }

    pub fn with_clock_skew(
        registry: SigV4Registry,
        region: impl Into<String>,
        service: impl Into<String>,
        max_clock_skew_secs: i64,
    ) -> BpResult<Self> {
        let region = region.into();
        let service = service.into();
        Self::validate_name(SIGV4_REGION, &region)?;
        Self::validate_name(SIGV4_SERVICE, &service)?;
        if max_clock_skew_secs < 0 {
            return Err(BpError::config_invalid(
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

    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let registry = SigV4Registry::from_json(&vars.required(SIGV4_CREDENTIALS)?)?;
        Self::new(
            registry,
            vars.required(SIGV4_REGION)?,
            vars.required(SIGV4_SERVICE)?,
        )
    }

    pub fn signer_for_owner(&self, owner: &str) -> BpResult<SigV4Signer> {
        Ok(SigV4Signer::new(
            self.registry.credentials_for_owner(owner)?.clone(),
            self.region.clone(),
            self.service.clone(),
        ))
    }

    pub fn verify(&self, request: &SigV4Request<'_>, now: SystemTime) -> BpResult<VerifiedCaller> {
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

    fn validate_name(name: &'static str, value: &str) -> BpResult<()> {
        if value.is_empty() {
            return Err(BpError::config_invalid(name, "must not be empty"));
        }
        if value.trim() != value || value.chars().any(char::is_control) {
            return Err(BpError::config_invalid(
                name,
                "must not contain surrounding whitespace or control characters",
            ));
        }
        Ok(())
    }
}
