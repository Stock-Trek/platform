use crate::aws_ops::presigned_post::PresignedPost;
use platform_errors::BpResult;
use platform_messages::{
    build_request::{BuildRequest as BuildRequestPayload, BuildRequestSignedPost},
    sqs_message::{GitSource, SqsRefType},
};

pub struct BuildRequest {
    pub source: GitSource,
    pub repo_id: String,
    pub ref_name: String,
    pub ref_type: SqsRefType,
    pub commit_hash: Option<String>,
    pub signed_get_url: String,
    pub signed_post: PresignedPost,
}

impl BuildRequest {
    pub fn payload(&self) -> BpResult<Vec<u8>> {
        let payload = BuildRequestPayload {
            git_host: self.source.host().to_string(),
            git_provider: self.source.name().to_string(),
            repo_id: self.repo_id.clone(),
            ref_name: self.ref_name.clone(),
            ref_type: self.ref_type,
            commit_hash: self.commit_hash.clone(),
            signed_get_url: self.signed_get_url.clone(),
            signed_post: BuildRequestSignedPost {
                url: self.signed_post.url.clone(),
                fields: self.signed_post.fields.clone(),
                max_artifact_size_bytes: Some(self.signed_post.max_artifact_size_bytes()),
            },
        };
        Ok(serde_json::to_vec(&payload)?)
    }
}

#[cfg(test)]
mod tests;
