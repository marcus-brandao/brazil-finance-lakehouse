# 0003. Bronze: rewritten landing files come back as new rows

- Status: accepted
- Date: 2026-11-22

## Context

The connector rewrites the same file for a calendar year whenever a run touches that year ([ADR-0002](0002-sgs-windows-and-watermark.md)). Auto Loader identifies a file by its path, so with the default settings a rewritten file is skipped and bronze keeps the old values. Checked against the Databricks docs on 2026-11-22:

- With `cloudFiles.allowOverwrites` set to `true`, Auto Loader also compares the file's last modified time, and "is guaranteed to process the latest version of the file". It reads the whole file again and cannot guarantee which intermediate version it processes. The docs recommend immutable files and leave duplicate handling to the user.
- In file notification mode a file can be ingested twice with this option, because the event time and the modification time can differ. Directory listing, the default, does not have that problem.
- Serverless compute accepts only `Trigger.AvailableNow()` and the deprecated `Trigger.Once()`. The default trigger fails with `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.
- With a schema provided, the schema evolution mode is `none` and data is rescued only if `rescuedDataColumn` is set.

Bronze is append-only and keeps the raw payload untouched. Dedup and typing happen in silver.

## Decision

- `cloudFiles.allowOverwrites=true` and directory listing. A rewritten year file is read again and appended as a new row. The old row stays.
- Each row keeps `_metadata.file_path` as `source_file_path` and `_metadata.file_modification_time` as `source_file_modification_time`, next to `ingested_at` and `run_id` from the envelope. Silver picks the newest row per file (or per series and date) with these columns.
- The envelope schema is given to the reader, with every field a string except `series_code`. `payload` is read as the JSON text of the array, so bronze does not depend on what BACEN returns inside it. `rescuedDataColumn` is set to `_rescued_data`, so a new envelope field lands there instead of being dropped.
- The job runs with `trigger(availableNow=True)` and the checkpoint in the landing Volume.

### Alternatives considered

- A new file per run (the `run_id` in the path): bronze then needs no overwrite option, but the same window would no longer map to the same path, which breaks the idempotent connector rule, and the Volume would grow with every run.
- Replacing rows in bronze with a merge: it removes history and makes bronze depend on silver rules.

## Consequences

- Each rewrite adds a copy of the whole year, so `sgs_raw` grows with every incremental run. The data is small. If it stops being small, a retention step can drop old rows per file.
- Silver has to dedup. A reconciliation compares the records in a file with the newest bronze row for it ([bronze.md](../bronze.md)).
- If Auto Loader sees a file twice with the same modification time, the table gets two identical rows. Silver dedup handles that too.
- Not confirmed yet: that an overwrite through the Files API changes the modification time Auto Loader sees, and that Auto Loader runs on Free Edition serverless with a checkpoint in a Volume. The manual run in [bronze.md](../bronze.md) checks both. If the modification time does not change, this decision does not work and needs another look.
