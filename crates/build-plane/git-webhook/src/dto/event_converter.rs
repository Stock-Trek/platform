use crate::{
    dto::github::github_event_converter::GitHubEventConverter, event_response::EventResponse,
};
use lambda_http::Request;
use platform_aws::SsmSecrets;
use platform_errors::BuildResult;
use platform_github::host_allowlist::HostAllowlist;

pub trait EventConverter: Send + Sync {
    fn event_response(&self, event: &Request) -> BuildResult<EventResponse>;
}

pub struct EventConverters;

impl EventConverters {
    pub fn all(
        secrets: &SsmSecrets,
        allowed_hosts: HostAllowlist,
    ) -> BuildResult<Vec<Box<dyn EventConverter>>> {
        Ok(vec![Box::new(GitHubEventConverter::new(
            secrets,
            allowed_hosts,
        )?)])
    }
}
