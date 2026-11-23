# Bronze

`databricks/bronze.py --source <name>` reads the landing files of one source with Auto Loader and appends them to `bronze.<name>_raw`. The sources are `sgs` (BACEN SGS series), `ptax` (USD closing rate from the Olinda API) and `cvm_inf_diario` (CVM daily fund report). `sgs_raw` and `ptax_raw` have the same layout, except that `ptax_raw` has no `series_code`. Each of their rows is one file read: the envelope fields, the raw payload as a JSON string, the file path and the file modification time. `cvm_inf_diario_raw` has one row per CSV line instead, with one string column per CSV column name across the three CVM layouts, plus the file path and modification time ([ADR-0008](adr/0008-cvm-inf-diario-landing.md)). Typing and dedup happen in silver. See [ADR-0003](adr/0003-bronze-overwritten-files.md) for how a rewritten file is handled.

## Deploy

`create_bronze_job.py` uploads the script to your workspace folder and creates the job `bronze`, or updates it if it exists. It does not start a run. It uses the same `.env` as the other scripts (see [setup.md](setup.md)).

```bash
set -a && source .env && set +a
uv run python databricks/create_bronze_job.py
```

The job has three serverless Python script tasks, `bronze_sgs`, `bronze_ptax` and `bronze_cvm_inf_diario`, with no dependency between them. Each gets `--catalog <DATABRICKS_CATALOG>` and its `--source`. The Jobs API refuses a serverless task without an environment, so the job sets environment version 6, the newest on the Databricks release notes page when this was written, and no libraries. The stream uses `trigger(availableNow=True)`, the only incremental trigger besides the deprecated `once` that serverless accepts. The checkpoints are in the landing Volume at `_checkpoints/bronze_<source>_raw`. The SGS one is the path the job used before it was renamed from `bronze_sgs`, so SGS files are not read again.

The job used to be called `bronze_sgs` and ran `bronze_sgs.py`. Delete that job and the old script in your workspace folder once the new job has run.

## Run

In the workspace, open Jobs & Pipelines, open `bronze` and choose Run now. A run reads the files that are new or changed since the last run and stops.

## Reconciliation

SGS. Compares the number of records in each landing file with the number of records in the newest bronze row for the same file. Run it in the SQL editor, with your catalog in place of `workspace`. Add a filter on `series_code` and `window_start` to check one window.

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

PTAX. The same comparison, per year file.

```sql
WITH files AS (
  SELECT
    window_start,
    json_array_length(payload) AS records_in_file
  FROM read_files(
    '/Volumes/workspace/bronze/landing/ptax/',
    format => 'json',
    schema => 'window_start STRING, payload STRING'
  )
),
newest AS (
  SELECT
    window_start,
    json_array_length(payload) AS records_in_table
  FROM workspace.bronze.ptax_raw
  QUALIFY row_number() OVER (
    PARTITION BY source_file_path
    ORDER BY source_file_modification_time DESC
  ) = 1
)
SELECT
  f.window_start,
  f.records_in_file,
  n.records_in_table,
  f.records_in_file = n.records_in_table AS matches
FROM files f
LEFT JOIN newest n USING (window_start)
ORDER BY f.window_start;
```

An empty year writes no file, so a year with no rows does not show up here. Check the connector log for those.

CVM. Counts the CSV lines in each month file and the bronze rows from the newest read of the same file. `read_files` is only used for counting here, so it needs no schema. It reports the path with a `dbfs:` prefix and Auto Loader without one, so the two sides are joined on the month.

```sql
WITH files AS (
  SELECT
    regexp_extract(_metadata.file_path, 'month=([0-9-]+)', 1) AS month,
    count(*) AS rows_in_file
  FROM read_files(
    '/Volumes/workspace/bronze/landing/cvm_inf_diario/',
    format => 'csv',
    header => true,
    sep => ';'
  )
  GROUP BY ALL
),
newest AS (
  SELECT source_file_path, max(source_file_modification_time) AS modified
  FROM workspace.bronze.cvm_inf_diario_raw
  GROUP BY ALL
),
in_table AS (
  SELECT
    regexp_extract(b.source_file_path, 'month=([0-9-]+)', 1) AS month,
    count(*) AS rows_in_table
  FROM workspace.bronze.cvm_inf_diario_raw b
  JOIN newest n
    ON b.source_file_path = n.source_file_path
   AND b.source_file_modification_time = n.modified
  GROUP BY ALL
)
SELECT
  f.month,
  f.rows_in_file,
  t.rows_in_table,
  f.rows_in_file = t.rows_in_table AS matches
FROM files f
LEFT JOIN in_table t USING (month)
ORDER BY f.month;
```

To check that each layout filled its own columns, count the fund columns per month. Up to 2023-11 only `CNPJ_FUNDO` is set, from 2023-12 only `CNPJ_FUNDO_CLASSE`.

```sql
SELECT
  regexp_extract(source_file_path, 'month=([0-9-]+)', 1) AS month,
  count(CNPJ_FUNDO) AS cnpj_fundo,
  count(CNPJ_FUNDO_CLASSE) AS cnpj_fundo_classe,
  count(_rescued_data) AS rescued
FROM workspace.bronze.cvm_inf_diario_raw
GROUP BY ALL
ORDER BY month;
```

## Manual run in the workspace

CI cannot reach Databricks, so the checks below run by hand after a change to the job. Record the date, the counts and the result under "Verified".

1. Run the job. Run the reconciliation queries. All rows match.
2. Run the job again without changing any file. `count(*)` on `bronze.sgs_raw` and `bronze.ptax_raw` stays the same.
3. Upload one year file again with `uv run python -m ingestion.upload push`, then run the job. The count goes up by one row for each file that was rewritten, and the new row has a later `source_file_modification_time`.
4. `DESCRIBE TABLE EXTENDED bronze.sgs_raw` and `bronze.ptax_raw` show the table comment and a comment on every column.

## Verified

Run by hand in a Free Edition workspace on 2026-11-22 and 2026-11-23 (UTC), with the four series for 2025 in the landing Volume.

- First run: the reconciliation query returned `matches = true` for series 11, 12, 432 and 433 (252, 252, 365 and 12 records).
- After a second run with no file changed, each file had been read once.
- Series 432 was uploaded again with `ingestion.upload push`. The next run added one row for `series_code=432/year=2025.json` with a later `source_file_modification_time`, and left the other three files alone. The table then had 5 rows.
- `DESCRIBE TABLE EXTENDED bronze.sgs_raw` showed the table comment and a comment on all 10 columns.

Full history, checked on 2026-11-23 after run 45113207172 of the ingest workflow backfilled from 1995-01-01. The reconciliation query matched for all 124 files, and `bronze.sgs_raw` had 138 rows (the earlier test runs account for the rest).

| Series | Files | First year | Records |
|---|---|---|---|
| 11 | 32 | 1995 | 8003 |
| 12 | 32 | 1995 | 8003 |
| 432 | 28 | 1999 | 10142 |
| 433 | 32 | 1995 | 382 |

SGS returned "Value(s) not found" for series 432 from 1995 to 1998, so those windows wrote no file.

### PTAX

Run from a laptop on 2026-11-23 (UTC) against the Free Edition workspace, with the same commands the workflow runs. The state files were pulled first, and the SGS state was not touched.

- `ingestion.ptax backfill --from 1984-01-01` wrote 43 year files (1984 to 2026) and `_state/ptax.json` with `{"usd": "2026-11-19"}`. The files hold 10514 records, the same count the API returned for the whole range in one request.
- After `ingestion.upload push` and the first run of the `bronze` job, `bronze.ptax_raw` had 43 rows for 43 files, each read once, and `_rescued_data` was empty. The PTAX reconciliation matched for all 43 files, 10514 records in the files and in the table. The 2025 payload keeps the rates as JSON numbers (`"cotacaoCompra":6.208`).
- `ingestion.ptax incremental` with a fresh pull fetched one window (2026), wrote `ptax/year=2026.json` and left the watermark at 2026-11-19. After the next job run the table had 44 rows. The extra row is for `year=2026.json`, now read twice, with a later `source_file_modification_time` on the second row. No other file was read again. `bronze.sgs_raw` stayed at 140 rows for 124 files.
- `DESCRIBE TABLE EXTENDED bronze.ptax_raw` showed the table comment and a comment on all 9 columns.
- `dbt source freshness` passed for `ptax_raw` and `sgs_raw`, and the two `not_null` tests on `ptax_raw` passed.

### CVM

Run on 2026-11-23 (UTC) against the Free Edition workspace.

- Before any code, a test job loaded one file of each layout (200501, 202311 and 202611) with the schema from `SOURCES` and showed Auto Loader matches CSV columns by header name. A fourth file with an extra column put it in `_rescued_data` with the file path. The test files and tables were deleted afterwards.
- From a laptop, `ingestion.cvm backfill --from 2023-01-01` took 34 s and wrote 47 month files (2023-01 to 2026-11), 2,338,382,514 bytes and 24,788,055 CSV lines, and `_state/cvm.json` with `{"inf_diario": "2026-11"}`. `ingestion.upload push` took 4 min 13 s. The first run of the `bronze` job took 48 s.
- `bronze.cvm_inf_diario_raw` then had 24,788,055 rows for 47 files, each read once. The CVM reconciliation matched for all 47 months. 2023-01 to 2023-11 filled `TP_FUNDO` and `CNPJ_FUNDO` only, 2023-12 to 2026-11 filled `TP_FUNDO_CLASSE` and `CNPJ_FUNDO_CLASSE` only, and `_rescued_data` was empty. The table was 470 MB in 25 files (`DESCRIBE DETAIL`), about a fifth of the CSV size.
- Run 45265550213 of the ingest workflow (incremental, `source` cvm, on this branch) refetched 2026-10 and 2026-11. After its bronze run those two months had been read twice, and the table had 25,632,412 rows. The reconciliation still matched for the newest read of every month. `bronze.sgs_raw` stayed at 144 rows for 124 files and `bronze.ptax_raw` at 45 rows for 43 files, so their tasks read nothing.
- `information_schema.columns` shows a comment on all 15 columns of `cvm_inf_diario_raw`, and `DESCRIBE DETAIL` shows the table comment.
- `dbt source freshness` passed for all three sources, and the two `not_null` tests on `cvm_inf_diario_raw` passed.
