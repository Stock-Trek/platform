use crate::{
    dto::github::github_event_converter::GitHubEventConverter, event_response::EventResponse,
};
use lambda_http::Request;
use platform_aws::secrets::Secrets;
use platform_errors::BpResult;
use platform_github::host_allow_list::HostAllowlist;

pub trait EventConverter: Send + Sync {
    fn event_response(&self, event: &Request) -> BpResult<EventResponse>;
}

pub struct EventConverters;

impl EventConverters {
    pub fn all(
        secrets: &Secrets,
        allowed_hosts: HostAllowlist,
    ) -> BpResult<Vec<Box<dyn EventConverter>>> {
        Ok(vec![Box::new(GitHubEventConverter::new(
            secrets,
            allowed_hosts,
        )?)])
    }
}
