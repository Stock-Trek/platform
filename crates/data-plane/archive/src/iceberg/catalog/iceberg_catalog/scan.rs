use super::IcebergCatalog;
use crate::iceberg::catalog::IcebergDataFile;
use futures::TryStreamExt;
use iceberg::table::Table;
use platform_errors::BpResult;
use std::collections::HashSet;

impl IcebergCatalog {
    pub(super) async fn registered_paths(&self, table: &Table) -> BpResult<HashSet<String>> {
        let scan = table
            .scan()
            .build()
            .map_err(|error| Self::catalog_error("build Iceberg table scan", error))?;
        let mut tasks = scan
            .plan_files()
            .await
            .map_err(|error| Self::catalog_error("plan Iceberg table scan", error))?;
        let mut paths = HashSet::new();
        while let Some(task) = tasks
            .try_next()
            .await
            .map_err(|error| Self::catalog_error("read Iceberg table scan", error))?
        {
            paths.insert(task.data_file_path().to_string());
        }
        Ok(paths)
    }

    pub(super) async fn data_files_from_table(
        &self,
        table: &Table,
    ) -> BpResult<Vec<IcebergDataFile>> {
        let scan = table
            .scan()
            .build()
            .map_err(|error| Self::catalog_error("build Iceberg table scan", error))?;
        let mut tasks = scan
            .plan_files()
            .await
            .map_err(|error| Self::catalog_error("plan Iceberg table scan", error))?;
        let mut files = Vec::new();
        while let Some(task) = tasks
            .try_next()
            .await
            .map_err(|error| Self::catalog_error("read Iceberg table scan", error))?
        {
            files.push(IcebergDataFile::new(
                task.data_file_path().to_string(),
                i64::try_from(task.file_size_in_bytes).unwrap_or(i64::MAX),
                i64::try_from(task.record_count.unwrap_or_default()).unwrap_or(i64::MAX),
            ));
        }
        files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
        Ok(files)
    }
}
