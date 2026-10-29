use crate::{
    dto::{event_converter::EventConverter, github::github_event_type::GitHubEventType},
    event_response::EventResponse,
    infisical::secrets::Secrets,
};
use platform_errors::{BpError, BpResult};
use platform_github::host_allow_list::HostAllowlist;
use platform_metrics::event::EventMetrics;

const GITHUB_WEBHOOK_SECRET: &str = "GITHUB_WEBHOOK_SECRET";
pub const UNKNOWN_ACTION: &str = "unknown";
pub const NO_ACTION: &str = "none";

pub struct GitHubEventConverter {
    pub secret: String,
    pub allowed_hosts: HostAllowlist,
}

impl GitHubEventConverter {
    pub fn new(secrets: &Secrets, allowed_hosts: HostAllowlist) -> BpResult<Self> {
        let secret = secrets.get(GITHUB_WEBHOOK_SECRET)?;
        if secret.is_empty() {
            Err(BpError::config_empty(GITHUB_WEBHOOK_SECRET))?;
        }
        Ok(Self {
            secret,
            allowed_hosts,
        })
    }
}

impl EventConverter for GitHubEventConverter {
    fn event_response(&self, request: &lambda_http::Request) -> BpResult<EventResponse> {
        let Some(event_type_str) = request
            .headers()
            .get("x-github-event")
            .and_then(|v| v.to_str().ok())
        else {
            return Ok(EventResponse::WrongGitProvider);
        };
        if !Self::verify_signature(request, &self.secret) {
            return Ok(EventResponse::Unauthorized);
        }
        let delivery_id = Self::delivery_id(request);
        let event_type = GitHubEventType::from(event_type_str);
        tracing::info!(delivery_id = ?delivery_id, "event type: {event_type}");
        match event_type {
            GitHubEventType::Create => self.ref_response(request, delivery_id),
            GitHubEventType::Delete => {
                EventMetrics::record_ignored_event(event_type_str, NO_ACTION);
                Ok(EventResponse::Ignore)
            }
            GitHubEventType::Installation => self.installation_response(request, delivery_id),
            GitHubEventType::InstallationRepositories => {
                self.installation_repositories_response(request, delivery_id)
            }
            GitHubEventType::Other => {
                EventMetrics::record_ignored_event(event_type_str, UNKNOWN_ACTION);
                Ok(EventResponse::Ignore)
            }
            GitHubEventType::Ping => self.ping_response(),
            GitHubEventType::Push => self.push_response(request, delivery_id),
            GitHubEventType::Repository => self.repository_response(request, delivery_id),
        }
    }
}
