use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::commit_algorithm::{
    CommitRetry, ConditionalCommit, ConditionalCommitTarget, Evaluation,
};
use crate::iceberg::catalog::iceberg_catalog::snapshot::SnapshotCommit;
use crate::iceberg::{ArchiveIcebergTable, ArchiveRegistration, RegisterOutcome};
use async_trait::async_trait;
use iceberg::table::Table;
use platform_errors::{PlatformError, PlatformResult};

pub(super) struct IcebergCommitTarget<'a> {
    catalog: &'a IcebergCatalog,
    table: ArchiveIcebergTable,
}

impl<'a> IcebergCommitTarget<'a> {
    pub(super) fn new(catalog: &'a IcebergCatalog, table: ArchiveIcebergTable) -> Self {
        Self { catalog, table }
    }
}

#[async_trait]
impl ConditionalCommitTarget for IcebergCommitTarget<'_> {
    type State = Table;

    fn table(&self) -> ArchiveIcebergTable {
        self.table
    }

    async fn load(&self) -> PlatformResult<Self::State> {
        self.catalog.ensure_table(self.table).await
    }

    async fn evaluate(
        &self,
        state: &Self::State,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<Evaluation> {
        let live_outputs = self.catalog.registered_paths(state).await?;
        let claimed_sources = IcebergCatalog::source_files_from_metadata(state);
        Ok(Evaluation::resolve(
            registration,
            &claimed_sources,
            &live_outputs,
        ))
    }

    async fn submit(
        &self,
        state: &Self::State,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<()> {
        let properties =
            IcebergCatalog::snapshot_properties(registration.files(), registration.source_files())?;
        let commit = SnapshotCommit::build(state, registration.files(), properties).await?;
        let request = commit.to_request(&self.catalog.table_ident(self.table));
        self.catalog.send_commit(self.table, &request).await
    }
}

impl IcebergCatalog {
    pub async fn commit_files(
        &self,
        table: ArchiveIcebergTable,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<RegisterOutcome> {
        ConditionalCommit::new(
            IcebergCommitTarget::new(self, table),
            CommitRetry::default(),
        )
        .run(registration)
        .await
    }

    pub(super) async fn send_commit(
        &self,
        table: ArchiveIcebergTable,
        request: &serde_json::Value,
    ) -> PlatformResult<()> {
        let namespace = self.namespace.as_ref().join("\u{1f}");
        let prefix = self
            .prefix
            .as_ref()
            .map(|prefix| format!("/{prefix}"))
            .unwrap_or_default();
        let url = format!(
            "{}/v1{prefix}/namespaces/{namespace}/tables/{}",
            self.catalog_uri.trim_end_matches('/'),
            table.name()
        );
        let mut request_builder = reqwest::Client::new().post(url.clone()).json(request);
        if let Some(token) = &self.token {
            request_builder = request_builder.bearer_auth(token);
        }
        let response = request_builder.send().await.map_err(|error| {
            PlatformError::external(std::io::Error::other(format!(
                "conditional Iceberg commit to {url} failed: {error}"
            )))
        })?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status.is_success() {
            return Ok(());
        }
        Err(PlatformError::external(std::io::Error::other(format!(
            "conditional Iceberg commit rejected with {status}: {body}"
        ))))
    }
}
