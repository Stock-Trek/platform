use crate::iceberg::catalog::iceberg_catalog::claim_index::INDEX_HEAD_KEY;
use crate::iceberg::catalog::iceberg_catalog::retention::ClaimRetention;
use crate::iceberg::catalog::iceberg_catalog::snapshot_file::SnapshotFile;
use crate::iceberg::catalog::{IcebergDataFile, iceberg_catalog::IcebergCatalog};
use iceberg::{
    TableIdent, TableRequirement, TableUpdate,
    spec::{
        FormatVersion, MAIN_BRANCH, ManifestListWriter, ManifestWriterBuilder, Snapshot,
        SnapshotReference, SnapshotSummaryCollector, Summary,
    },
    table::Table,
};
use platform_errors::PlatformResult;
use serde_json::{Value, json};
use std::collections::HashMap;
use uuid::Uuid;

pub(super) struct SnapshotCommit {
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

impl SnapshotCommit {
    pub(super) async fn build_with_table_properties(
        table: &Table,
        files: &[IcebergDataFile],
        properties: HashMap<String, String>,
        table_properties: HashMap<String, String>,
    ) -> PlatformResult<Self> {
        let snapshot_id = SnapshotFile::snapshot_id(table);
        let sequence_number = table.metadata().next_sequence_number();
        let commit_uuid = Uuid::new_v4();
        let metadata_directory = table.metadata().metadata_location().map_err(|error| {
            IcebergCatalog::catalog_error("read Iceberg metadata directory", error)
        })?;
        let manifest_path = format!("{metadata_directory}/{commit_uuid}-m0.avro");
        let manifest_list_path =
            format!("{metadata_directory}/snap-{snapshot_id}-0-{commit_uuid}.avro");

        let mut manifests = Vec::new();
        if let Some(current) = table.metadata().current_snapshot() {
            manifests.extend(
                table
                    .manifest_list_reader(current)
                    .load()
                    .await
                    .map_err(|error| {
                        IcebergCatalog::catalog_error("read Iceberg manifests", error)
                    })?
                    .consume_entries(),
            );
        }

        let mut data_files = files
            .iter()
            .map(|file| SnapshotFile::data_file(table, file))
            .collect::<PlatformResult<Vec<_>>>()?;
        data_files.sort_by(|left, right| left.file_path().cmp(right.file_path()));

        if !data_files.is_empty() {
            let output = table
                .file_io()
                .new_output(manifest_path)
                .map_err(|error| IcebergCatalog::catalog_error("open Iceberg manifest", error))?;
            let builder = ManifestWriterBuilder::new(
                output,
                Some(snapshot_id),
                table.metadata().current_schema().clone(),
                table.metadata().default_partition_spec().as_ref().clone(),
            );
            let mut writer = match table.metadata().format_version() {
                FormatVersion::V1 => builder.build_v1(),
                FormatVersion::V2 => builder.build_v2_data(),
                FormatVersion::V3 => builder.build_v3_data(),
            };
            for file in &data_files {
                writer
                    .add_file(file.clone(), sequence_number)
                    .map_err(|error| {
                        IcebergCatalog::catalog_error("write Iceberg manifest entry", error)
                    })?;
            }
            manifests.push(
                writer.write_manifest_file().await.map_err(|error| {
                    IcebergCatalog::catalog_error("write Iceberg manifest", error)
                })?,
            );
        }

        let manifest_list_output = table
            .file_io()
            .new_output(manifest_list_path.clone())
            .map_err(|error| IcebergCatalog::catalog_error("open Iceberg manifest list", error))?;
        let manifest_list_writer = manifest_list_output
            .writer()
            .await
            .map_err(|error| IcebergCatalog::catalog_error("write Iceberg manifest list", error))?;
        let parent_snapshot_id = table.metadata().current_snapshot_id();
        let mut manifest_list_writer = match table.metadata().format_version() {
            FormatVersion::V1 => {
                ManifestListWriter::v1(manifest_list_writer, snapshot_id, parent_snapshot_id)
            }
            FormatVersion::V2 => ManifestListWriter::v2(
                manifest_list_writer,
                snapshot_id,
                parent_snapshot_id,
                sequence_number,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                manifest_list_writer,
                snapshot_id,
                parent_snapshot_id,
                sequence_number,
                Some(table.metadata().next_row_id()),
            ),
        };
        manifest_list_writer
            .add_manifests(manifests.into_iter())
            .map_err(|error| {
                IcebergCatalog::catalog_error("prepare Iceberg manifest list", error)
            })?;
        manifest_list_writer
            .close()
            .await
            .map_err(|error| IcebergCatalog::catalog_error("write Iceberg manifest list", error))?;

        let mut summary = SnapshotSummaryCollector::default();
        for (key, value) in &properties {
            summary.set(key, value);
        }
        for file in &data_files {
            summary.add_file(
                file,
                table.metadata().current_schema().clone(),
                table.metadata().default_partition_spec().clone(),
            );
        }
        let index_authoritative = table.metadata().properties().contains_key(INDEX_HEAD_KEY)
            || table_properties.contains_key(INDEX_HEAD_KEY);
        let snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(sequence_number)
            .with_summary(Summary {
                operation: iceberg::spec::Operation::Append,
                additional_properties: summary.build(),
            })
            .with_schema_id(table.metadata().current_schema_id())
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .build();

        let mut updates = vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    ClaimRetention::branch(index_authoritative),
                ),
            },
        ];
        if !table_properties.is_empty() {
            updates.push(TableUpdate::SetProperties {
                updates: table_properties,
            });
        }
        Ok(Self {
            requirements: vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: parent_snapshot_id,
                },
            ],
            updates,
        })
    }

    pub(super) fn to_request(&self, identifier: &TableIdent) -> Value {
        json!({
            "identifier": {
                "namespace": identifier.namespace().as_ref(),
                "name": identifier.name(),
            },
            "requirements": self.requirements,
            "updates": self.updates,
        })
    }
}
