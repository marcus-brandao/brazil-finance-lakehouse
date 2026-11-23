# Bronze: SGS

`databricks/bronze_sgs.py` reads the landing files of BACEN SGS with Auto Loader and appends them to `bronze.sgs_raw`. Each row is one file read: the envelope fields, the raw payload as a JSON string, the file path and the file modification time. Typing and dedup happen in silver. See [ADR-0003](adr/0003-bronze-overwritten-files.md) for how a rewritten file is handled.

## Deploy

`create_bronze_job.py` uploads the script to your workspace folder and creates the job `bronze_sgs`, or updates it if it exists. It does not start a run. It uses the same `.env` as the other scripts (see [setup.md](setup.md)).

```bash
set -a && source .env && set +a
uv run python databricks/create_bronze_job.py
```

The job has one serverless Python script task with `--catalog <DATABRICKS_CATALOG>` as its parameter. The stream uses `trigger(availableNow=True)`, the only incremental trigger besides the deprecated `once` that serverless accepts. The checkpoint is in the landing Volume at `_checkpoints/bronze_sgs_raw`.

## Run

In the workspace, open Jobs & Pipelines, open `bronze_sgs` and choose Run now. A run reads the files that are new or changed since the last run and stops.

## Reconciliation

Compares the number of records in each landing file with the number of records in the newest bronze row for the same file. Run it in the SQL editor, with your catalog in place of `workspace`. Add a filter on `series_code` and `window_start` to check one window.

```sql
WITH files AS (
  SELECT
    series_code,
    window_start,
    json_array_length(payload) AS records_in_file
  FROM read_files(
    '/Volumes/workspace/bronze/landing/sgs/',
    format => 'json',
    schema => 'series_code BIGINT, window_start STRING, payload STRING'
  )
),
newest AS (
  SELECT
    series_code,
    window_start,
    json_array_length(payload) AS records_in_table
  FROM workspace.bronze.sgs_raw
  QUALIFY row_number() OVER (
    PARTITION BY source_file_path
    ORDER BY source_file_modification_time DESC
  ) = 1
)
SELECT
  f.series_code,
  f.window_start,
  f.records_in_file,
  n.records_in_table,
  f.records_in_file = n.records_in_table AS matches
FROM files f
LEFT JOIN newest n USING (series_code, window_start)
ORDER BY f.series_code, f.window_start;
```

Every row should have `matches = true`. A null in `records_in_table` means the file was never read. To see how often each file was read:

```sql
SELECT source_file_path, count(*) AS times_read
FROM workspace.bronze.sgs_raw
GROUP BY source_file_path
ORDER BY source_file_path;
```

## Manual run in the workspace

CI cannot reach Databricks, so the checks below run by hand after a change to the job. Record the date, the counts and the result under "Verified".

1. Run the job. Run the reconciliation query. All rows match.
2. Run the job again without changing any file. `count(*)` on `bronze.sgs_raw` stays the same.
3. Upload one year file again with `uv run python -m ingestion.upload push`, then run the job. The count goes up by one row for each file that was rewritten, and the new row has a later `source_file_modification_time`.
4. `DESCRIBE TABLE EXTENDED bronze.sgs_raw` shows the table comment and a comment on every column.

## Verified

Not run yet.
