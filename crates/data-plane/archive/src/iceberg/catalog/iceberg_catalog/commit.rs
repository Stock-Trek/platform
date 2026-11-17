use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::claim_index::ClaimIndex;
use crate::iceberg::catalog::iceberg_catalog::commit_algorithm::{
    CommitRetry, ConditionalCommit, ConditionalCommitTarget, Evaluation,
};
use crate::iceberg::catalog::iceberg_catalog::snapshot::SnapshotCommit;
use crate::iceberg::{ArchiveIcebergTable, ArchiveRegistration, CommitContext, RegisterOutcome};
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

    async fn load(&self, context: &CommitContext) -> PlatformResult<Self::State> {
        self.catalog
            .ensure_table_with_context(self.table, Some(context))
            .await
    }

    async fn evaluate(
        &self,
        state: &Self::State,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<Evaluation> {
        let sources = registration.source_files().unwrap_or_default();
        let outputs = registration
            .expected_outputs()
            .into_iter()
            .collect::<Vec<_>>();
        let claims = self.catalog.claim_matches(state, sources, &outputs).await?;
        Ok(Evaluation::resolve(
            registration,
            &claims.sources,
            &claims.outputs,
        ))
    }

    async fn submit(
        &self,
        state: &Self::State,
        registration: &ArchiveRegistration,
        context: &CommitContext,
    ) -> PlatformResult<()> {
        let baseline = if ClaimIndex::has_head(state) {
            None
        } else {
            Some(self.catalog.claim_state(state).await?)
        };
        let table_properties = ClaimIndex::append(state, registration, baseline).await?;
        let mut properties =
            IcebergCatalog::snapshot_properties(registration.files(), registration.source_files())?;
        if let Some(compaction_id) = registration.compaction_id() {
            properties.insert(
                crate::COMPACTION_ID_PROPERTY.to_string(),
                compaction_id.as_str(),
            );
        }
        let commit = SnapshotCommit::build_with_table_properties(
            state,
            registration.files(),
            properties,
            table_properties,
        )
        .await?;
        let request = commit.to_request(&self.catalog.table_ident(self.table));
        context.authorize().await?;
        self.catalog.send_commit(self.table, &request).await
    }
}

impl IcebergCatalog {
    pub(super) async fn commit_files(
        &self,
        table: ArchiveIcebergTable,
        registration: &ArchiveRegistration,
        context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        ConditionalCommit::new(
            IcebergCommitTarget::new(self, table),
            CommitRetry::default(),
        )
        .run(registration, context)
        .await
    }

    pub(super) async fn send_commit(
        &self,
        table: ArchiveIcebergTable,
        request: &serde_json::Value,
    ) -> PlatformResult<()> {
        self.send_table_commit(&self.table_ident(table), request)
            .await
    }

    pub(super) async fn send_table_commit(
        &self,
        ident: &iceberg::TableIdent,
        request: &serde_json::Value,
    ) -> PlatformResult<()> {
        let namespace = ident.namespace().as_ref().join("\u{1f}");
        let prefix = self
            .prefix
            .as_ref()
            .map(|prefix| format!("/{prefix}"))
            .unwrap_or_default();
        let url = format!(
            "{}/v1{prefix}/namespaces/{namespace}/tables/{}",
            self.catalog_uri.trim_end_matches('/'),
            ident.name()
        );
        let mut request_builder = self.client.post(url.clone()).json(request);
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
