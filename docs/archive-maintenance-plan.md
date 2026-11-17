# Archive retention and Iceberg maintenance plan

Status: implementation plan.
This document defines the target design, delivery sequence, acceptance criteria, and settled decisions for archive maintenance.

## Objective

Define how raw archive objects, compacted data files, Iceberg snapshots, and Iceberg metadata are expired without losing the compacted/raw handoff or reprocessing data.
The plan bounds storage, snapshots, manifests, and metadata while preserving exactly-once handoff and recovery.

## Existing architecture

Raw ingestion and compaction are append-only.
The archiver writes content-addressed Parquet objects under the raw prefix and registers every object in the `raw/events` or `raw/candles` Iceberg table.

The compactor lists raw objects in S3, keeps those registered in the raw table and absent from the processed set, writes compacted files, and appends them to the `compacted/events` or `compacted/candles` table.
Raw objects remain in place.

The processed set is reconstructed from every retained compacted snapshot.
Each compaction commit writes one `source-file:<uri>` property per raw source, so snapshot metadata grows with every compacted raw file.
Snapshot summary properties also have size limits, which makes them a poor store for large URI sets even before expiry is considered.
`IcebergCatalog::source_files_from_metadata` unions all snapshot properties, and `ArchiveHandoff::discover` subtracts that union from the raw table to find the tail.

No component expires snapshots, deletes orphaned files, expires raw objects, rewrites compacted files, or removes processed entries from the raw table.
Storage, snapshots, manifests, and metadata grow with total history.

## Constraints and invariants

Snapshot expiry removes snapshots from metadata only.
As of late 2026, iceberg-rust provides the metadata-only `ExpireSnapshotsAction`, while physical file cleanup and high-level rewrite actions are still maturing or are follow-ups.
Physical cleanup therefore still needs a separate orphan-removal pass, and the design keeps emerging actions behind interfaces instead of inventing ad-hoc manifest surgery.

Expiring compacted snapshots would remove their `source-file:` properties, so the compactor would reprocess raw objects and recovery would replay both layers.
Expiring raw snapshots does not remove processed data-file entries from the raw table's current snapshot, so raw metadata is not bounded by expiry alone.
Compacted files are only appended, so expiry cannot delete files still referenced by the current snapshot.
Bounding compacted storage also needs rewriting or merging.

Raw-table cleanup is a first-class part of retention, not a footnote to object deletion.
After processed entries are identified by the ledger, they still have to be removed from the raw table by a rewrite or a delete-and-commit, or the raw table must be rebuilt from the unprocessed tail.
That work can generate metadata churn and needs its own metrics, limits, and back-pressure.

The design must preserve these invariants:

- A raw object is fully represented by compacted files or in the raw tail, never both and never neither.
- Processed state survives snapshot expiry, and a crash between compacted-file and ledger publication is recoverable without data loss.
- Raw retention is configurable, and deleting raw objects leaves no dangling raw table, compactor, recovery, or rebuilder references.
- Compacted files merge into bounded-size outputs, with old files deleted only after the replacement commit is durable.
- Maintenance has explicit retention and grace periods, runs separately from ingestion and compaction, is observable, and supports a dry run.
- Maintenance stages have a defined order, a fencing lease, and partition-level coordination so maintenance cannot race compaction on the same partitions.
- Recovery states and records which layer is authoritative after raw expiry.
- Destructive stages, retention floors, and recovery choices fail closed and are visible in metrics and logs.

## Design: durable compaction ledger plus separate maintenance

The implementation adds an authoritative processed-source ledger stored independently of compacted snapshot history.
A dedicated Iceberg table holds one row per compaction batch with the compaction id, archive kind, source-manifest URI, source count, and processed timestamp.
The source manifest is an immutable Parquet file listing the batch's raw object URIs.
Keeping the per-file mapping in manifests avoids snapshot summary size limits and keeps it out of every commit and scan.
The ledger's current snapshot is the processed set. Its old snapshots can be expired after retention, and its rows are compacted by archive kind and hour.

### Ledger and source-manifest lifecycle

Ledger rows are partitioned by archive kind and hour, matching the archive kind they track so that raw deletion and ledger retention can be coordinated at partition level. The compacted-rewrite stage also runs on ledger partitions, with explicit targets such as at most 100 data files and 512 MiB per partition and a cap of one rewrite commit per partition per maintenance cycle.

Source manifests are immutable data files of the ledger table, not side artefacts. Ledger retention applies to them. Orphan cleanup must never delete a manifest while any ledger row that references it is live or inside the ledger retention window, and a manifest is only eligible for deletion after all referencing rows and snapshots have expired.

Compaction ids are UUIDv7 values generated once per batch and recorded in the ledger row, the compacted table property, and the source-manifest metadata. The compacted-table property is informational only; the ledger remains the sole authority. This makes retries idempotent and lets auditors correlate the manifest, ledger row, and compacted commit.

### Publication order

The ledger is the single source of truth for what has been processed.
Compaction publishes in a crash-safe order:

1. Write deterministic compacted output objects.
2. Append the compacted data files and a single informational compaction-id property to the compacted table.
3. Write the immutable source manifest, then append the ledger row that references it.

On retry, an existing compaction id means the compacted files are already published, so only the ledger step is repeated.
A ledger entry is never written before its compacted files are visible.
Compaction ids make retries idempotent without relying on snapshot summaries or batch fingerprint reconstruction.
The compaction-id property on the compacted table must not be used to calculate the processed set.

After the grace period, maintenance deletes processed raw objects and removes their entries from the raw table with a rewrite, or replaces in-place removal with a periodic rebuild from the unprocessed tail.
Raw-table cleanup is a first-class part of the raw-deletion stage, with its own metrics, throughput limits, and back-pressure.
It is not allowed to lag silently behind raw object deletion.

### Maintenance runner

Maintenance is a dedicated runner with four stages that can be scheduled independently:

1. Expire old snapshots on the raw, compacted, and ledger tables.
2. Delete unreferenced data, manifest, and metadata files older than the orphan grace period, including manifests only after their ledger rows and snapshots have expired.
3. Rewrite compacted files into target-size outputs when a compacted or ledger partition exceeds its configured file count or size target.
4. Remove processed entries from the raw table, then delete raw objects whose ledger entry and source manifest are older than the configured retention, with an S3 lifecycle backstop.

The enforced order is expire, orphan cleanup, compacted rewrite, then raw deletion.
Stages can be scheduled independently, but the runner never advances a later stage while an earlier one is blocked.
The runner holds a fencing lease so two maintenance instances cannot act at once.
Compaction and the rewrite stage use partition-level locks or leases, so concurrent compaction and maintenance cannot race on the same partition.
The lease and concurrent-maintenance tests are required, not optional hardening.

The orphan grace period has a hard floor of 7 days, is configurable upward, and must always exceed the longest expected write, compaction, or maintenance lease. A dry run lists candidate files and their age; deletion fails closed if any candidate is younger than the floor or referenced by a retained snapshot.

Stage 4 never advances while any earlier stage or raw-table cleanup is blocked. It deletes a raw object only when the compacted files are durable, its ledger row and manifest are older than raw retention, and its raw-table entry is gone. A batch that is published but cannot be cleaned up because of a permanent maintenance failure raises an alert, applies back-pressure to the affected partition, and requires operator action rather than silently retrying forever.

### Safety, visibility, and edge cases

Every stage emits a lag metric and an alert threshold. The critical metric is the age of the oldest processed ledger entry whose raw objects are still present in the raw table; it must trend down and alert if it exceeds one maintenance cycle or fails to fall over two consecutive cycles. Other required metrics are unreferenced files older than the grace floor, compacted and ledger files per partition against target, metadata-log and manifest growth, lease age, stage duration and retries, and recovery layer selections. A blocked earlier stage stops later stages and raises an alert; silent lag is a failure.

S3 lifecycle rules are a pure backstop. Raw objects have no age-based expiration; only the runner deletes them after the handoff, raw-table, and retention conditions are met. The baseline policy transitions eligible raw objects to Intelligent-Tiering after 30 days, aborts incomplete multipart uploads after 7 days, and applies no expiration to compacted, manifest, or metadata prefixes. If a backstop expiration is enabled, it is at least 30 days longer than the runner's raw-deletion window, is monitored, and is never the primary deletion mechanism.

Recovery records the selected layer in structured logs and a metric, including the ledger watermark and raw expiry state. After raw expiry, a missing or unreadable raw object must not trigger a silent raw fallback; recovery fails closed and alerts instead.

If a source manifest is missing or corrupt, quarantine the batch, keep its raw objects and ledger rows, and mark it for investigation. If raw objects are still live, re-compact the batch under a new compaction id and reconcile the ledger. If raw objects have expired, fail closed and rebuild from the compacted layer plus retained ledger state.

Schema and partition evolution are handled at batch boundaries. The source-manifest schema and ledger partition spec are versioned and pinned when a batch is created. Compaction, rewrite, and recovery read the spec recorded for that batch and never rewrite across incompatible specs. A partition-spec change pauses maintenance and requires a re-baseline before old raw objects are expired.

This keeps the two-layer design, allows incremental delivery, and scales processed state by compaction batches rather than by every raw file in every snapshot.
The costs are the extra ledger table, cross-table ordering, and the raw-table rewrite or rebuild needed to reclaim raw table metadata.
It also leaves a clear migration path to making current table state authoritative: the ledger can be retired once the current snapshot of a single table becomes authoritative.

## Delivery approach

Implement the durable compaction ledger plus separate maintenance design as the near-term path.
This gives safe snapshot expiry, bounded metadata, and configurable raw retention without a complete rewrite of the archive model or a custom high-level rewrite engine.

Keep the custom surface area small and intentional.
The custom pieces are the ledger, source manifests, and compaction-ID idempotency.
The maintenance runner orchestrates the four stages but prefers calling into iceberg-rust actions, or thin wrappers around emerging rewrite helpers, rather than reimplementing manifest surgery or file planning.

Make the ledger the single source of truth for what has been processed.
After raw expiry, recovery treats the compacted layer plus the ledger as the sole source of truth.
Recovery must not silently fall back to raw objects, and it should record and expose which layer it selected.
Keep recovery logic able to treat the current snapshot of a future single table as authoritative once raw objects are gone.
Avoid baking dual-table assumptions too deeply into schemas, metrics, or operational tooling.

## Implementation plan

1. Define the ledger schema, UUIDv7 compaction ids, versioned source-manifest format, partition targets, and raw, snapshot, and orphan-retention settings with their hard floors.
2. Publish compacted files first, then the ledger row, and make retries detect an existing compaction id.
3. Read the ledger's current snapshot in the compactor and recovery handoff instead of all compacted snapshot summaries.
4. Make the ledger the only processed-set authority, with an observable authoritative-layer decision after raw expiry.
5. Add a maintenance runner with a fencing lease, dry-run mode, stage-lag metrics, alerts, an S3 lifecycle policy that is only a backstop, stage ordering, and the four stages above.
   Keep it out of ingestion and compaction.
6. Add partition-level coordination between compaction and compacted-file rewriting.
   Prove with concurrency tests that maintenance and compaction cannot race on the same partition.
7. Add raw-table entry removal or periodic rebuild as a first-class part of raw cleanup with its own metrics, limits, alerting, and back-pressure, including permanent-failure handling.
   Only then enable raw object deletion and the S3 lifecycle backstop.
8. Add compacted-file rewriting for compacted and ledger partitions and test that replacement, expiry, and orphan removal preserve exactly one copy of every archived record.
9. Test crash points, expired snapshots, orphan grace floors, lease expiry, concurrent maintenance, manifest corruption or loss, schema and partition evolution, and recovery after raw objects are gone.
   Recovery tests must confirm that the compacted layer and ledger are authoritative, that the selected layer is logged and metered, and that no raw fallback occurs.

## Acceptance criteria

- Compacted snapshots can be expired without changing the processed set.
- Raw objects are deleted only after their compacted representation and ledger entry are durable, their raw-table entries are handled, and their retention period has elapsed.
- The raw table no longer retains entries for deleted processed objects.
- Raw-table cleanup has metrics, throughput limits, and back-pressure, and cannot fall silently behind raw object deletion.
- Compacted files per partition remain within a configured count or size target.
- Snapshot, metadata-log, manifest, and orphan-file counts remain bounded under continuous ingestion and compaction.
- Maintenance has a fencing lease and partition-level coordination with compaction, demonstrated by concurrent-maintenance tests.
- Orphan cleanup respects the 7-day floor and never deletes a source manifest referenced by a live or retained ledger row.
- Ledger partitions meet the same file-count and size targets as compacted partitions.
- Stage lag is metered and alerted, and a blocked stage stops later stages, including after a permanent raw-cleanup failure.
- Recovery after raw expiry uses the compacted layer plus the ledger as the sole authority, records that choice in logs and metrics, and still produces the same records as before maintenance.
- Schema and partition evolution are pinned per batch and covered by recovery tests.

## Decisions

Each decision is deliberately conservative because the archive is a primary data asset.

1. **Raw retention.** Retain raw objects for at least 90 days after their compacted representation and ledger entry are durable, and use 365 days as the default until audit, replay, and exchange obligations are confirmed. Make retention configurable per archive kind, and reject values below 90 days. Colder storage is acceptable after 30 days as a storage-class transition, such as S3 Intelligent-Tiering or Glacier Instant Retrieval, provided objects remain readable and retrievable. Do not attach an age-based expiration to raw objects; only the maintenance runner may delete them after the handoff, raw-table, and retention conditions are all satisfied.

2. **Compacted history and recovery.** Keep at least 10 compacted snapshots and 30 days of snapshot history as a hard floor, and use 90 days as the default when time-travel requirements are not yet confirmed. Never expire a snapshot or file that is still needed by an in-flight compaction or maintenance batch. Recovery cannot rely on compacted files alone while a raw tail exists. It must use compacted files plus the authoritative processed-source state, and after raw expiry it must record that it used the compacted layer plus the ledger rather than falling back to raw objects.

3. **Layer model.** Keep the separate raw layer during the initial implementation and for the full raw-retention period. Do not make the current table the only source of truth until the single-table design in Future work is implemented and proven to preserve the exactly-once handoff through crashes, snapshot expiry, orphan cleanup, and re-baseline. Recovery must keep using the dual-layer handoff until that cutover is complete.

4. **Upstream actions.** Use upstream actions for snapshot expiry, orphan-file cleanup, metadata cleanup, and compacted-file rewriting whenever the pinned iceberg-rust version exposes an action with the required fencing, dry-run, and durability semantics. Fall back to in-process code only for the ledger and source-manifest work, raw-table entry removal or rebuild, and any rewrite not yet covered by an upstream transactional action. Keep those fallbacks behind thin interfaces, test them against the real REST catalog and S3, and do not perform ad hoc manifest surgery.

5. **Re-baseline swap.** Require an atomic catalog rename or a stable alias for the preferred swap. If the catalog cannot provide one, use a new table generation with a controlled dual-read cutover. Before deleting the old generation, validate per-partition row counts and checksums, the exactly-one-copy handoff invariant, successful recovery from the new generation, and that every reader and writer uses it. Keep the old generation read-only for a rollback window of at least 30 days, and never delete it in the same run that performs the swap.

6. **Raw-table cleanup.** Default to periodic rebuilds from the unprocessed tail because they are bounded, restartable, and easier to validate against the ledger. Use in-place rewrites only for small, explicitly bounded partitions where a rebuild is too expensive, and require a dry run plus back-pressure. Budget at most one raw-table rewrite commit per partition per maintenance cycle, cap total maintenance commits and bytes per hour, and pause with an alert before metadata growth exceeds a configured weekly threshold, for example 10 percent, or before the metadata-log or manifest count reaches its alert limit.

7. **Orphan grace and S3 lifecycle.** Use 7 days as a hard floor, 14 days as the default, and always keep the grace period longer than the longest expected write, compaction, or maintenance lease. Dry runs must show candidate age. Baseline S3 policy: no expiration on raw, compacted, manifest, or metadata prefixes; transition eligible raw objects to Intelligent-Tiering after 30 days; abort incomplete multipart uploads after 7 days. If a backstop expiration is enabled, set it at least 30 days beyond the runner's raw-deletion window, monitor it, and never rely on it as the primary deletion mechanism.

8. **Failure handling.** For a lost or corrupt source manifest, quarantine the batch, keep its raw objects and ledger rows, and mark it for investigation. If raw objects are still live, re-compact under a new compaction id and reconcile the ledger. If they have expired, fail closed and rebuild from the compacted layer plus retained ledger state. Pin the manifest schema and ledger partition spec per batch, never rewrite across incompatible specs, and pause maintenance for a re-baseline after a partition-spec change. Permanent raw-cleanup failures alert and apply back-pressure instead of silently retrying.

## Future work

These changes are not part of the initial implementation.
Revisit them once ledger-based maintenance has been running long enough to prove the handoff invariants under expiry, orphan cleanup, and recovery.

### Make current table state authoritative

Rewrite the archive so one snapshot adds compacted files and removes the raw files they replace.
The current snapshot becomes the live archive, and raw versus compacted becomes a file-layout property rather than a cross-table handoff.
Standard Iceberg rewrite, snapshot expiry, and orphan removal then apply directly.

A split-table variant keeps both tables but removes source files from the raw table in the same transaction family as the compacted commit.
It still needs a handoff record between the commits, so it usually becomes the ledger design plus a raw-table rewrite.

The single-table form has the strongest atomicity and the smallest live metadata surface.
If the archive is expected to keep years of history, investing in this design earlier can be cheaper than maintaining ledger-specific tooling forever.
The cost is a larger refactor of compaction, schemas, recovery, and rebuilder layer selection.
Current iceberg-rust lacks a high-level single-table rewrite action, so it needs a custom transaction or manifest writer, or an external maintenance engine until upstream helpers cover the operation.

### Periodic re-baseline and table swap

Periodically rebuild the compacted tables from retained history, validate row counts and handoff invariants, swap the catalog to the new table generation, and delete the old generation after a grace period.
A variant can retain only the compacted layer after raw expiry.

This bounds all metadata and file counts regardless of incremental commits, but it is expensive, needs temporary duplicate storage, and requires a catalog swap or new table names.
It complements routine maintenance rather than replacing it.
