# Archive retention and Iceberg maintenance options

Status: investigation only.
No implementation changes, per issue #711.

## Question

How should raw archive objects, compacted data files, Iceberg snapshots, and Iceberg metadata be expired without losing the compacted/raw handoff or reprocessing data?

## Current behaviour

Raw ingestion and compaction are append-only.
The archiver writes content-addressed Parquet objects under the raw prefix and registers every object in the `raw/events` or `raw/candles` Iceberg table.

The compactor lists raw objects in S3, keeps those registered in the raw table and absent from the processed set, writes compacted files, and appends them to the `compacted/events` or `compacted/candles` table.
Raw objects remain in place.

The processed set is reconstructed from every retained compacted snapshot.
Each compaction commit writes one `source-file:<uri>` property per raw source.
`IcebergCatalog::source_files_from_metadata` unions all snapshot properties, and `ArchiveHandoff::discover` subtracts that union from the raw table to find the tail.

No component expires snapshots, deletes orphaned files, expires raw objects, rewrites compacted files, or removes processed entries from the raw table.
Storage, snapshots, manifests, and metadata grow with total history.

## Constraints

`expire_snapshots` in Iceberg 0.11 removes snapshots from metadata only; physical cleanup needs a separate orphan-removal pass.

Expiring compacted snapshots would remove their `source-file:` properties, so the compactor would reprocess raw objects and recovery would replay both layers.
Expiring raw snapshots does not remove processed data-file entries from the raw table's current snapshot, so raw metadata is not bounded by expiry alone.
Compacted files are only appended, so expiry cannot delete files still referenced by the current snapshot; bounding compacted storage also needs rewriting or merging.

The design must preserve these invariants:

- A raw object is fully represented by compacted files or in the raw tail, never both and never neither.
- Processed state survives snapshot expiry, and a crash between compacted-file and processed-state publication is recoverable without data loss.
- Raw retention is configurable, and deleting raw objects leaves no dangling raw table, compactor, recovery, or rebuilder references.
- Compacted files merge into bounded-size outputs, with old files deleted only after the replacement commit is durable.
- Maintenance has explicit retention and grace periods, runs separately from ingestion and compaction, is observable, and supports a dry run.
- Recovery states which layer is authoritative after raw expiry.
  Compacted files become the long-term source once raw objects are removed.

## Options

### Option 1: Scheduled expiry and lifecycle only

Add an external maintenance job that calls `expire_snapshots`, removes unreferenced files, applies an S3 lifecycle rule to raw and compacted prefixes, and rewrites compacted files.

This is the smallest implementation, but it is not safe on the current handoff.
Expiring compacted snapshots loses the processed set immediately, and an age-based lifecycle can delete raw objects that compaction has not processed.
It also cannot delete processed raw table entries or merge compacted files.
This option only becomes viable after processed state is decoupled from snapshots.

### Option 2: Durable compaction ledger plus separate maintenance

Add an authoritative processed-source ledger stored independently of compacted snapshot history.
A dedicated Iceberg table holds one row per compaction batch: compaction id, archive kind, source-manifest URI, source count, and processed timestamp.
The manifest is an immutable Parquet file listing the batch's raw object URIs.
The ledger's current snapshot is the processed set; its own old snapshots can be expired and its rows compacted by hour.

Compaction publishes in a crash-safe order:

1. Write deterministic compacted output objects.
2. Append the compacted data files and a single compaction-id property to the compacted table.
3. Append the batch row and source manifest to the ledger.

On retry, an existing compaction-id means the compacted files are already published, so only the ledger step is repeated.
A ledger entry is never written before its compacted files are visible.
Source manifests keep the per-file mapping out of snapshot summaries and out of every commit and scan.

After a grace period, maintenance deletes processed raw objects, removes their entries from the raw table with a rewrite, and expires old raw snapshots.
A periodic rebuild from the unprocessed tail can replace in-place entry removal.

Maintenance has four independent stages: merge compacted files into target-size outputs; expire old snapshots on all three tables; delete unreferenced data, manifest, and metadata files older than a grace period; and delete raw objects whose ledger entry is older than the configured retention, with an S3 lifecycle backstop.

This keeps the two-layer design, allows incremental delivery, and scales processed state by compaction batches rather than by every raw file in every snapshot.
The costs are the extra ledger table, cross-table ordering, and a raw-table rewrite or rebuild to reclaim raw table metadata.

### Option 3: Make current table state authoritative

Rewrite the archive in place so one snapshot adds compacted files and removes the raw files they replace.
The current snapshot becomes the live archive, and raw/compacted becomes a file-layout property.
Standard Iceberg rewrite, snapshot expiry, and orphan removal then apply directly.

A split-table variant keeps both tables but removes source files from the raw table after the compacted commit.
A handoff record is still needed between the commits, so this usually becomes Option 2 plus a raw-table rewrite.

The single-table form has the strongest atomicity and smallest live metadata, but requires a larger refactor of compaction, schemas, recovery, and rebuilder layer selection.
Iceberg 0.11 has no high-level rewrite action, so it needs a custom transaction/manifest writer or an external maintenance engine.

### Option 4: Object-store lifecycle without Iceberg maintenance

Use S3 lifecycle rules to expire raw objects by age or tag, and optionally move old compacted objects to colder storage.
This bounds object storage cost without touching Iceberg metadata, but not snapshots, manifests, metadata, or compacted file counts.
Age-based deletion can remove unprocessed raw data, and deleting a raw object does not remove its live raw table entry.
This is acceptable only as a backstop for an explicitly processed set and a lag monitor.

### Option 5: Periodic re-baseline and table swap

Periodically rebuild the compacted tables from retained history, validate row counts and handoff invariants, swap the catalog to the new table generation, and delete the old generation after a grace period.
A variant can retain only the compacted layer after raw expiry.

This bounds all metadata and file counts regardless of incremental commits, but is expensive, needs temporary duplicate storage, and requires a catalog swap or new table names.
It complements routine maintenance rather than replacing it.

## Recommended direction

Use Option 2 first.
Replace snapshot-summary processed markers with a durable compaction ledger, make retries idempotent by compaction id, and only then enable snapshot expiry and orphan removal.
Add raw retention after the ledger is authoritative, with a grace period longer than the maximum expected compaction lag and monitoring of the unprocessed raw backlog.
Follow with compacted-file rewriting so each partition has a bounded number of target-size files.

Add Option 5 as a scheduled re-baseline for disaster recovery and for reclaiming metadata that incremental maintenance cannot reduce.
Option 3 is cleaner long-term if the archive will retain years of history and the team is willing to replace the two-table handoff; it should be chosen before building ledger-specific tooling if a single-table rewrite is acceptable.

## Actionable plan for Option 2

1. Define the ledger schema, compaction id, source-manifest format, and raw and snapshot retention settings.
2. Publish compacted files first, then the ledger row, and make retries detect an existing compaction id.
3. Read the ledger's current snapshot in the compactor and recovery handoff instead of all compacted snapshot summaries.
4. Add a maintenance runner with a lease, dry-run mode, metrics, and the four stages above.
   Keep it out of ingestion and compaction.
5. Add raw-table entry removal or periodic rebuild, then enable raw object deletion and an S3 lifecycle backstop.
6. Add compacted-file rewriting and test that replacement, expiry, and orphan removal preserve exactly one copy of every archived record.
7. Test crash points, expired snapshots, orphan grace periods, concurrent maintenance, and recovery after raw objects are gone.

## Acceptance criteria for closing the issue

- Compacted snapshots can be expired without changing the processed set.
- Raw objects are deleted only after their compacted representation and ledger entry are durable and their retention period has elapsed.
- The raw table no longer retains entries for deleted processed objects.
- Compacted files per partition remain within a configured count or size target.
- Snapshot, metadata-log, manifest, and orphan-file counts remain bounded under continuous ingestion and compaction, and recovery still produces the same records as before maintenance.

## Open questions

1. What raw retention is required for audit, replay, or exchange obligations, and is colder object storage acceptable before deletion?
2. How long must compacted history and snapshots remain for time travel, and can recovery rely on compacted files alone?
3. Should the archive keep a separate raw layer after compaction, or can current table state become the only source of truth?
4. Is an external Iceberg maintenance engine available for rewrite and orphan removal, or must those operations be implemented in the Rust service?
5. Does the catalog support table renames or aliases for a re-baseline swap, and what validation is required before the old generation is deleted?
