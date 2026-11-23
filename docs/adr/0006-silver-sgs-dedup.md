# 0006. Silver SGS: newest bronze row per file, replaced by file

- Status: accepted
- Date: 2026-11-23

## Context

Bronze keeps one row per read of a landing file, and a rewritten year file comes back as a new row ([ADR-0003](0003-bronze-overwritten-files.md)). ADR-0002 and ADR-0003 say silver keeps the row with the latest `ingested_at` per series and date. Checked in `bronze.sgs_raw` on 2026-11-23:

- A date appears in one year file only (no date in more than one file path, none in the wrong year).
- Series 432 has 16 rows dated after today, up to 2026-12-09.
- `valor` always parses as a number with a point as decimal mark. Series 11 and 12 have up to 6 decimals and one integer digit, 432 and 433 have 2 decimals and up to 2 integer digits, and 433 has negative values.

## Decision

- `silver.sgs_series` is built from the newest bronze row of each `source_file_path` (latest `source_file_modification_time`, then `ingested_at`), with the payload exploded to one row per date. This is the grain of the reconciliation in [bronze.md](../bronze.md), and because a date lives in one file only, (series, date) stays unique.
- The model is incremental with `delete+insert` and `unique_key='source_file_path'`. A run reads only bronze rows with a `source_file_modification_time` later than the maximum in silver, and replaces all silver rows of the files it read. A date that BACEN removes in a revision leaves silver, which a merge on (series, date) would not do.
- `valor` is `decimal(18,6)`, enough for all four series. A value that does not fit fails the run instead of becoming null.
- Series 432 keeps its future-dated rows. They are the published Selic target. A flag set at build time would go stale in an incremental model, so consumers filter on `date <= current_date()` when they need observed values.

## Consequences

- The incremental filter assumes Auto Loader appends files in modification-time order. A file that lands in bronze with an older modification time than the silver maximum is skipped until a full refresh.
- A rewritten file with an empty payload would not delete its old rows. The connector writes no file for an empty window, so this does not happen today.
- On Databricks the delete and insert rewrite the table files that hold the replaced rows. The table is small, so this is cheap.
