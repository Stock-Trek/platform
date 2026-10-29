use super::{
    ArchiveIcebergTable, IcebergConfig, IcebergDataFile, metadata, schema::schema as table_schema,
};
use crate::{Bucket, ObjectKey, S3};
use async_trait::async_trait;
use bytes::Bytes;
use platform_errors::{BpError, BpResult};
use reqwest::{Client, Method, Response, StatusCode, header::CONTENT_TYPE};
use serde_json::{Value, json};
use std::{collections::HashSet, time::Duration};
use tokio::time::sleep;
use tracing::{info, warn};
use uuid::Uuid;

const MAX_COMMIT_ATTEMPTS: usize = 4;
const COMMIT_BACKOFF: Duration = Duration::from_millis(250);
const SOURCE_FILE_SUMMARY_PREFIX: &str = "source-file:";
const ARCHIVE_BATCH_SUMMARY_KEY: &str = "archive-batch";

#[async_trait]
pub trait ArchiveCatalog: Send + Sync {
    async fn register_files(
        &self,
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BpResult<()>;

    async fn registered_data_files(&self, table: ArchiveIcebergTable) -> BpResult<HashSet<String>>;

    async fn processed_source_files(&self, table: ArchiveIcebergTable)
    -> BpResult<HashSet<String>>;
}

pub struct IcebergCatalog {
    http: Client,
    base_url: String,
    token: Option<String>,
    warehouse: String,
    namespace: String,
    s3: aws_sdk_s3::Client,
    transfer: aws_sdk_s3_transfer_manager::Client,
}

impl IcebergCatalog {
    pub async fn connect(config: &IcebergConfig) -> BpResult<Self> {
        let http = Client::builder()
            .build()
            .map_err(|error| BpError::http_request("build Iceberg REST client", "", error))?;
        let base = config.catalog_uri.trim_end_matches('/');
        let base_url = if base.ends_with("/v1") {
            base.to_string()
        } else {
            format!("{base}/v1")
        };
        let (s3, transfer) = S3::clients().await;

        Ok(Self {
            http,
            base_url,
            token: config.token.clone(),
            warehouse: config.warehouse.trim_end_matches('/').to_string(),
            namespace: config.namespace.clone(),
            s3,
            transfer,
        })
    }

    fn tables_path(&self) -> String {
        format!(
            "/namespaces/{}/tables",
            urlencoding::encode(&self.namespace)
        )
    }

    fn table_path(&self, table: ArchiveIcebergTable) -> String {
        format!(
            "{}/{}",
            self.tables_path(),
            urlencoding::encode(table.name())
        )
    }

    fn namespace_path(&self) -> String {
        format!("/namespaces/{}", urlencoding::encode(&self.namespace))
    }

    fn table_location(&self, table: ArchiveIcebergTable) -> String {
        format!(
            "{}/{}/{}",
            self.warehouse,
            self.namespace.trim_matches('/'),
            table.name()
        )
    }

    async fn send(&self, method: Method, path: &str, body: Option<&Value>) -> BpResult<Response> {
        let target = format!("{}{}", self.base_url, path);
        let mut request = self.http.request(method, target.clone());
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            let body = serde_json::to_vec(body).map_err(BpError::external)?;
            request = request.header(CONTENT_TYPE, "application/json").body(body);
        }
        request
            .send()
            .await
            .map_err(|error| BpError::http_request("call Iceberg REST catalog", target, error))
    }

    async fn get_table(&self, table: ArchiveIcebergTable) -> BpResult<Option<TableState>> {
        let path = self.table_path(table);
        let response = self.send(Method::GET, &path, None).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        self.table_response("load Iceberg table", &path, response)
            .await
            .map(Some)
    }

    async fn ensure_table(&self, table: ArchiveIcebergTable) -> BpResult<TableState> {
        if let Some(state) = self.get_table(table).await? {
            return Ok(state);
        }

        self.ensure_namespace().await?;
        let location = self.table_location(table);
        let body = json!({
            "name": table.name(),
            "location": location,
            "schema": table_schema(table.kind())?,
            "partition-spec": {"spec-id": 0, "fields": []},
            "write-order": {"order-id": 0, "fields": []},
            "stage-create": false,
            "properties": {"format-version": "2"},
        });
        let path = self.tables_path();
        let response = self.send(Method::POST, &path, Some(&body)).await?;
        if response.status() == StatusCode::CONFLICT {
            return self.get_table(table).await?.ok_or_else(|| {
                BpError::external(std::io::Error::other(format!(
                    "Iceberg table {} was not found after a conflict",
                    table.name()
                )))
            });
        }
        self.table_response("create Iceberg table", &path, response)
            .await
    }

    async fn ensure_namespace(&self) -> BpResult<()> {
        let path = self.namespace_path();
        let response = self.send(Method::GET, &path, None).await?;
        if response.status().is_success() {
            return Ok(());
        }
        if response.status() != StatusCode::NOT_FOUND {
            return self
                .success("load Iceberg namespace", &path, response)
                .await;
        }

        let body = json!({
            "namespace": [self.namespace],
            "properties": {},
        });
        let path = "/namespaces";
        let response = self.send(Method::POST, path, Some(&body)).await?;
        if response.status().is_success() || response.status() == StatusCode::CONFLICT {
            return Ok(());
        }
        self.success("create Iceberg namespace", path, response)
            .await
    }

    async fn table_response(
        &self,
        operation: &'static str,
        target: &str,
        response: Response,
    ) -> BpResult<TableState> {
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|error| BpError::http_request(operation, target.to_string(), error))?;
        if !status.is_success() {
            return Err(BpError::http_rejected(
                operation,
                target.to_string(),
                status.as_u16(),
                text,
            ));
        }
        let value: Value = serde_json::from_str(&text).map_err(BpError::external)?;
        let metadata = match value.get("metadata") {
            Some(metadata) => metadata.clone(),
            None => {
                let location = value
                    .get("metadata-location")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        BpError::external(std::io::Error::other(
                            "Iceberg table response has no metadata or metadata-location",
                        ))
                    })?;
                let bytes = self.download(location).await?;
                serde_json::from_slice(&bytes).map_err(BpError::external)?
            }
        };

        Ok(TableState { metadata })
    }

    async fn success(
        &self,
        operation: &'static str,
        target: &str,
        response: Response,
    ) -> BpResult<()> {
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|error| BpError::http_request(operation, target.to_string(), error))?;
        if status.is_success() {
            Ok(())
        } else {
            Err(BpError::http_rejected(
                operation,
                target.to_string(),
                status.as_u16(),
                text,
            ))
        }
    }

    async fn commit(&self, table: ArchiveIcebergTable, body: &Value) -> BpResult<()> {
        let path = self.table_path(table);
        let response = self.send(Method::POST, &path, Some(body)).await?;
        self.success("commit Iceberg table", &path, response).await
    }

    async fn already_registered(
        &self,
        state: &TableState,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BpResult<bool> {
        if let Some(source_files) = source_files {
            return Ok(source_files.iter().all(|file| state.has_source_file(file)));
        }
        if files.is_empty() {
            return Ok(false);
        }
        Ok(state.has_summary_value(ARCHIVE_BATCH_SUMMARY_KEY, &batch_fingerprint(files)))
    }

    async fn commit_snapshot(
        &self,
        table: ArchiveIcebergTable,
        state: &TableState,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BpResult<()> {
        let metadata = &state.metadata;
        let location = metadata
            .get("location")
            .and_then(Value::as_str)
            .map(|location| location.trim_end_matches('/').to_string())
            .unwrap_or_else(|| self.table_location(table));

        let current_snapshot_id = state.current_snapshot_id();
        let sequence_number = state.last_sequence_number().saturating_add(1);
        let snapshot_id = new_snapshot_id();
        let uuid = Uuid::new_v4();

        // A snapshot's manifest list must carry forward every manifest that is
        // still live, then append this snapshot's new manifest.
        let mut manifests = self.current_manifests(state).await?;

        let manifest_key = format!("{location}/metadata/{uuid}-m0.avro");
        let manifest_bytes = metadata::manifest(snapshot_id, sequence_number, files)?;
        let manifest_length = i64::try_from(manifest_bytes.len()).map_err(BpError::external)?;
        self.upload(&manifest_key, manifest_bytes).await?;
        manifests.push(metadata::ManifestListEntry::added(
            manifest_key,
            manifest_length,
            snapshot_id,
            sequence_number,
            files,
        ));

        let manifest_list_key = format!("{location}/metadata/snap-{snapshot_id}-1-{uuid}.avro");
        let manifest_list_bytes = metadata::manifest_list(&manifests)?;
        self.upload(&manifest_list_key, manifest_list_bytes).await?;

        let added_files = i64::try_from(files.len()).unwrap_or(i64::MAX);
        let added_rows = files
            .iter()
            .fold(0_i64, |total, file| total.saturating_add(file.record_count));
        let added_size = files.iter().fold(0_i64, |total, file| {
            total.saturating_add(file.file_size_in_bytes)
        });
        let total_files = state
            .summary_i64("total-data-files")
            .saturating_add(added_files);
        let total_rows = state
            .summary_i64("total-records")
            .saturating_add(added_rows);
        let total_size = state
            .summary_i64("total-files-size")
            .saturating_add(added_size);
        let mut summary = json!({
            "operation": "append",
            "added-data-files": added_files.to_string(),
            "added-records": added_rows.to_string(),
            "added-files-size": added_size.to_string(),
            "total-data-files": total_files.to_string(),
            "total-records": total_rows.to_string(),
            "total-files-size": total_size.to_string(),
        });
        summary[ARCHIVE_BATCH_SUMMARY_KEY] = Value::String(batch_fingerprint(files));
        if let Some(source_files) = source_files {
            for file in source_files {
                summary[format!("{SOURCE_FILE_SUMMARY_PREFIX}{file}")] =
                    Value::String("1".to_string());
            }
        }

        let mut snapshot = json!({
            "snapshot-id": snapshot_id,
            "sequence-number": sequence_number,
            "timestamp-ms": chrono::Utc::now().timestamp_millis(),
            "manifest-list": manifest_list_key,
            "summary": summary,
            "schema-id": metadata.get("current-schema-id").and_then(Value::as_i64).unwrap_or(0),
        });
        if let Some(parent) = current_snapshot_id {
            snapshot["parent-snapshot-id"] = json!(parent);
        }

        let mut current_ref = json!({
            "type": "assert-ref-snapshot-id",
            "ref": "main",
        });
        if let Some(snapshot_id) = current_snapshot_id {
            current_ref["snapshot-id"] = json!(snapshot_id);
        }
        let mut requirements = vec![current_ref];
        if let Some(table_uuid) = metadata.get("table-uuid").and_then(Value::as_str) {
            requirements.push(json!({
                "type": "assert-table-uuid",
                "uuid": table_uuid,
            }));
        }
        let body = json!({
            "identifier": {"namespace": [self.namespace], "name": table.name()},
            "requirements": requirements,
            "updates": [
                {"action": "add-snapshot", "snapshot": snapshot},
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": snapshot_id,
                    "type": "branch",
                },
            ],
        });
        self.commit(table, &body).await
    }

    async fn current_manifests(
        &self,
        state: &TableState,
    ) -> BpResult<Vec<metadata::ManifestListEntry>> {
        let Some(snapshot) = state.current_snapshot() else {
            return Ok(Vec::new());
        };
        let Some(manifest_list) = snapshot.get("manifest-list").and_then(Value::as_str) else {
            return Ok(Vec::new());
        };

        let bytes = self.download(manifest_list).await?;
        metadata::read_manifest_list(&bytes)
    }

    async fn registered_paths(&self, state: &TableState) -> BpResult<HashSet<String>> {
        let manifests = self.current_manifests(state).await?;
        let mut paths = HashSet::new();
        for manifest in manifests {
            let bytes = self.download(&manifest.manifest_path).await?;
            paths.extend(metadata::read_data_file_paths(&bytes)?);
        }
        Ok(paths)
    }

    async fn upload(&self, uri: &str, bytes: Vec<u8>) -> BpResult<()> {
        let (bucket, key) = parse_s3_uri(uri)?;
        S3::upload_bytes(&self.transfer, &bucket, &key, Bytes::from(bytes)).await
    }

    async fn download(&self, uri: &str) -> BpResult<Bytes> {
        let (bucket, key) = parse_s3_uri(uri)?;
        S3::download_bytes(&self.s3, &bucket, &key).await
    }
}

#[async_trait]
impl ArchiveCatalog for IcebergCatalog {
    async fn register_files(
        &self,
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BpResult<()> {
        let mut last_error = None;
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            let state = match self.ensure_table(table).await {
                Ok(state) => state,
                Err(error) => {
                    last_error = Some(error);
                    if attempt < MAX_COMMIT_ATTEMPTS {
                        sleep(COMMIT_BACKOFF * attempt as u32).await;
                    }
                    continue;
                }
            };

            match self.already_registered(&state, files, source_files).await {
                Ok(true) => {
                    info!(table = table.name(), "Iceberg commit was already applied");
                    return Ok(());
                }
                Ok(false) => {}
                Err(error) => {
                    last_error = Some(error);
                    if attempt < MAX_COMMIT_ATTEMPTS {
                        sleep(COMMIT_BACKOFF * attempt as u32).await;
                    }
                    continue;
                }
            }

            match self
                .commit_snapshot(table, &state, files, source_files)
                .await
            {
                Ok(()) => {
                    info!(
                        table = table.name(),
                        files = files.len(),
                        "registered archive files in Iceberg"
                    );
                    return Ok(());
                }
                Err(error) => {
                    warn!(
                        %error,
                        table = table.name(),
                        attempt,
                        "Iceberg commit failed"
                    );
                    last_error = Some(error);
                    if attempt < MAX_COMMIT_ATTEMPTS {
                        sleep(COMMIT_BACKOFF * attempt as u32).await;
                    }
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| BpError::external(std::io::Error::other("Iceberg commit failed"))))
    }

    async fn registered_data_files(&self, table: ArchiveIcebergTable) -> BpResult<HashSet<String>> {
        match self.get_table(table).await? {
            Some(state) => self.registered_paths(&state).await,
            None => Ok(HashSet::new()),
        }
    }

    async fn processed_source_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> BpResult<HashSet<String>> {
        let Some(state) = self.get_table(table).await? else {
            return Ok(HashSet::new());
        };
        Ok(state.source_files())
    }
}

struct TableState {
    metadata: Value,
}

impl TableState {
    fn current_snapshot_id(&self) -> Option<i64> {
        self.metadata.get("current-snapshot-id")?.as_i64()
    }

    fn last_sequence_number(&self) -> i64 {
        self.metadata
            .get("last-sequence-number")
            .and_then(Value::as_i64)
            .unwrap_or(0)
    }

    fn current_snapshot(&self) -> Option<&Value> {
        let current = self.current_snapshot_id()?;
        self.metadata
            .get("snapshots")?
            .as_array()?
            .iter()
            .find(|snapshot| snapshot.get("snapshot-id").and_then(Value::as_i64) == Some(current))
    }

    fn summary_i64(&self, key: &str) -> i64 {
        self.current_snapshot()
            .and_then(|snapshot| snapshot.get("summary"))
            .and_then(|summary| summary.get(key))
            .and_then(Value::as_str)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    fn has_source_file(&self, file: &str) -> bool {
        self.has_summary_key(&format!("{SOURCE_FILE_SUMMARY_PREFIX}{file}"))
    }

    fn has_summary_key(&self, key: &str) -> bool {
        self.metadata
            .get("snapshots")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|snapshot| snapshot.get("summary"))
            .any(|summary| summary.get(key).is_some())
    }

    fn has_summary_value(&self, key: &str, value: &str) -> bool {
        self.metadata
            .get("snapshots")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|snapshot| snapshot.get("summary"))
            .filter_map(|summary| summary.get(key))
            .filter_map(Value::as_str)
            .any(|candidate| candidate == value)
    }

    fn source_files(&self) -> HashSet<String> {
        self.metadata
            .get("snapshots")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|snapshot| snapshot.get("summary"))
            .filter_map(Value::as_object)
            .flat_map(|summary| summary.keys())
            .filter_map(|key| key.strip_prefix(SOURCE_FILE_SUMMARY_PREFIX))
            .map(str::to_string)
            .collect()
    }
}

fn batch_fingerprint(files: &[IcebergDataFile]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for file in files {
        for byte in file.file_path.as_bytes().iter().chain(&[0]) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("{hash:016x}")
}

fn new_snapshot_id() -> i64 {
    let value = (Uuid::new_v4().as_u128() & i64::MAX as u128) as i64;
    value.max(1)
}

fn parse_s3_uri(uri: &str) -> BpResult<(Bucket, ObjectKey)> {
    let path = uri.strip_prefix("s3://").ok_or_else(|| {
        BpError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Iceberg location {uri} is not an s3:// URI"),
        ))
    })?;
    let (bucket, key) = path.split_once('/').ok_or_else(|| {
        BpError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Iceberg location {uri} has no object key"),
        ))
    })?;
    if bucket.is_empty() || key.is_empty() {
        return Err(BpError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Iceberg location {uri} is incomplete"),
        )));
    }
    Ok((Bucket::new(bucket), ObjectKey::new(key)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_s3_metadata_locations() {
        let (bucket, key) =
            parse_s3_uri("s3://archive/warehouse/market_data/raw_events/metadata/a.avro").unwrap();
        assert_eq!(bucket.as_str(), "archive");
        assert_eq!(
            key.as_str(),
            "warehouse/market_data/raw_events/metadata/a.avro"
        );
        assert!(parse_s3_uri("file:///tmp/metadata.json").is_err());
    }

    #[test]
    fn state_reads_snapshot_summaries() {
        let state = TableState {
            metadata: json!({
                "current-snapshot-id": 9,
                "last-sequence-number": 4,
                "snapshots": [
                    {
                        "snapshot-id": 9,
                        "summary": {
                            "total-data-files": "3",
                            "total-records": "30",
                            "source-file:raw/events/year=2024/month=01/day=01/hour=00/a.parquet": "1"
                        }
                    }
                ]
            }),
        };

        assert_eq!(state.current_snapshot_id(), Some(9));
        assert_eq!(state.last_sequence_number(), 4);
        assert_eq!(state.summary_i64("total-data-files"), 3);
        assert!(state.has_source_file("raw/events/year=2024/month=01/day=01/hour=00/a.parquet"));
    }
}
