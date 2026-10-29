use super::{
    GitHubEventConverter,
    converter::{NO_ACTION, UNKNOWN_ACTION},
};
use crate::{
    dto::github::{
        github_event_installation::{GitHubEventInstallation, GitHubEventInstallationAction},
        github_event_installation_repositories::{
            GitHubEventInstallationRepositories, GitHubEventInstallationRepositoriesAction,
        },
        github_event_push::GitHubEventPush,
        github_event_ref::GitHubEventRef,
        github_event_repository::{GitHubEventRepository, GitHubEventRepositoryAction},
        github_types::GitHubRepositorySelection,
    },
    event_response::EventResponse,
};
use platform_errors::{BpError, BpResult, HttpError};
use platform_messages::sqs_message::SqsDetail;
use platform_metrics::event::EventMetrics;

impl GitHubEventConverter {
    pub fn installation_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventInstallation = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let installation_id = event.installation.id;
        let action = event.action;
        match action {
            GitHubEventInstallationAction::Created | GitHubEventInstallationAction::Unsuspend => {}
            _ => {
                EventMetrics::record_ignored_event("installation", &action.to_string());
                return Ok(EventResponse::Ignore);
            }
        }
        match event.installation.repository_selection {
            GitHubRepositorySelection::All => {
                let host = self.host(&event.installation)?;
                Self::accept(host, delivery_id, installation_id, SqsDetail::AllReposAdd)
            }
            GitHubRepositorySelection::Selected if !event.repositories.is_empty() => {
                let host = self.host(&event.installation)?;
                Self::accept(
                    host,
                    delivery_id,
                    installation_id,
                    SqsDetail::MultiRepoAdd {
                        repo_ids: Self::repo_ids(event.repositories),
                    },
                )
            }
            _ => {
                EventMetrics::record_ignored_event("installation", &action.to_string());
                Ok(EventResponse::Ignore)
            }
        }
    }

    pub fn installation_repositories_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventInstallationRepositories = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let action = event.action;
        match action {
            GitHubEventInstallationRepositoriesAction::Added => {}
            _ => {
                EventMetrics::record_ignored_event(
                    "installation_repositories",
                    &action.to_string(),
                );
                return Ok(EventResponse::Ignore);
            }
        }
        let repositories = event.repositories_added;
        if repositories.is_empty() {
            EventMetrics::record_ignored_event("installation_repositories", &action.to_string());
            return Ok(EventResponse::Ignore);
        }
        let host = self.host(&event.installation)?;
        let detail = SqsDetail::MultiRepoAdd {
            repo_ids: Self::repo_ids(repositories),
        };
        Self::accept(host, delivery_id, event.installation.id, detail)
    }

    pub fn repository_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventRepository = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let installation_id = event.installation.id;
        let repo_id = Self::repo_id(event.repository);
        let action = event.action;
        match action {
            GitHubEventRepositoryAction::Created => {
                let host = self.host(&event.installation)?;
                Self::accept(
                    host,
                    delivery_id,
                    installation_id,
                    SqsDetail::RepoAdd { repo_id },
                )
            }
            _ => {
                EventMetrics::record_ignored_event("repository", &action.to_string());
                Ok(EventResponse::Ignore)
            }
        }
    }

    pub fn ref_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventRef = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let Some(ref_type) = Self::ref_type(&event.ref_type) else {
            EventMetrics::record_ignored_event("create", &event.ref_type.to_string());
            return Ok(EventResponse::Ignore);
        };
        let host = self.host(&event.installation)?;
        let detail = SqsDetail::RefCreate {
            repo_id: Self::repo_id(event.repository),
            ref_name: Self::ref_name(&event.r#ref),
            ref_type,
        };
        Self::accept(host, delivery_id, event.installation.id, detail)
    }

    pub fn ping_response(&self) -> BpResult<EventResponse> {
        Ok(EventResponse::Pong)
    }

    pub fn push_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventPush = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let GitHubEventPush {
            after,
            created,
            deleted,
            installation,
            r#ref,
            repository,
            ..
        } = event;
        if created || deleted || Self::is_zero_sha(&after) {
            EventMetrics::record_ignored_event("push", NO_ACTION);
            return Ok(EventResponse::Ignore);
        }
        let Some((ref_name, ref_type)) = Self::commit_ref(&r#ref) else {
            EventMetrics::record_ignored_event("push", UNKNOWN_ACTION);
            return Ok(EventResponse::Ignore);
        };
        let host = self.host(&installation)?;
        let detail = SqsDetail::Commit {
            repo_id: Self::repo_id(repository),
            ref_name: ref_name.into(),
            ref_type,
            commit_hash: after,
        };
        Self::accept(host, delivery_id, installation.id, detail)
    }
}
