use crate::{
    app_config::GitHubAppConfig,
    host_allow_list::{DEFAULT_HOST, HostAllowlist},
    installation::GitHubInstallation,
};
use jsonwebtoken::EncodingKey;
use lru::LruCache;
use octocrab::Octocrab;
use parking_lot::Mutex;
use platform_config::git::DirectoryLimits;
use platform_errors::{BpError, BpResult, GitHubError};
use platform_time::timeouts::Timeouts;
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

const ENTERPRISE_API_PATH: &str = "api/v3";
type InstallationKey = (String, u64);

#[derive(Clone)]
pub struct GitHubAuth {
    app_id: u64,
    key: EncodingKey,
    apps: Arc<Mutex<HashMap<String, Arc<Octocrab>>>>,
    installations: Arc<Mutex<LruCache<InstallationKey, GitHubInstallation>>>,
    allowed_hosts: HostAllowlist,
    timeouts: Timeouts,
    directory_limits: DirectoryLimits,
}

impl GitHubAuth {
    pub fn new(
        config: GitHubAppConfig,
        timeouts: Timeouts,
        allowed_hosts: HostAllowlist,
        max_cached_installations: NonZeroUsize,
    ) -> BpResult<Self> {
        Self::with_directory_limits(
            config,
            timeouts,
            allowed_hosts,
            max_cached_installations,
            DirectoryLimits::default(),
        )
    }

    pub fn with_directory_limits(
        config: GitHubAppConfig,
        timeouts: Timeouts,
        allowed_hosts: HostAllowlist,
        max_cached_installations: NonZeroUsize,
        directory_limits: DirectoryLimits,
    ) -> BpResult<Self> {
        let key = EncodingKey::from_rsa_pem(config.private_key.as_bytes())
            .map_err(|error| BpError::config_invalid_source("GitHub App private key", error))?;
        Ok(Self {
            app_id: config.app_id,
            key,
            apps: Arc::new(Mutex::new(HashMap::new())),
            installations: Arc::new(Mutex::new(LruCache::new(max_cached_installations))),
            allowed_hosts,
            timeouts,
            directory_limits,
        })
    }

    pub fn installation(&self, host: &str, installation_id: u64) -> BpResult<GitHubInstallation> {
        let host = self.allowed_hosts.validate(host)?;
        let key = (host.clone(), installation_id);
        let mut installations = self.installations.lock();
        if let Some(installation) = installations.get(&key) {
            return Ok(installation.clone());
        }
        let app = self.app(&host)?;
        let github = GitHubInstallation::with_limits(
            app.as_ref(),
            &host,
            installation_id,
            self.timeouts,
            self.directory_limits,
        )?;
        installations.put(key, github.clone());
        Ok(github)
    }

    /// Returns an authenticated GitHub App client for `host`, building and
    /// caching it on first use. The public GitHub host uses octocrab's default
    /// base URI; any other host is treated as GitHub Enterprise and routed
    /// through `https://<host>/api/v3`.
    fn app(&self, host: &str) -> BpResult<Arc<Octocrab>> {
        let mut apps = self.apps.lock();
        if let Some(app) = apps.get(host) {
            return Ok(Arc::clone(app));
        }
        let mut builder = Octocrab::builder().app(self.app_id.into(), self.key.clone());
        if host != DEFAULT_HOST {
            let base_uri = format!("https://{host}/{ENTERPRISE_API_PATH}");
            builder = builder.base_uri(base_uri).map_err(|error| {
                BpError::config_invalid_source(format!("GitHub Enterprise host {host}"), error)
            })?;
        }
        let app = Arc::new(
            builder
                .set_connect_timeout(self.timeouts.connect.into())
                .set_read_timeout(self.timeouts.operation.into())
                .build()
                .map_err(|error| BpError::GitHub(GitHubError::BuildClient(error)))?,
        );
        apps.insert(host.to_owned(), Arc::clone(&app));
        Ok(app)
    }
}
