# 0005. dbt runs outside Databricks against the Free Edition warehouse

- Status: accepted
- Date: 2026-11-23

## Context

M2 builds silver and gold with dbt. dbt needs a SQL endpoint, and something has to run it. Facts checked on 2026-11-23:

- The [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) page (updated 2026-11-16) gives one SQL warehouse, 2X-Small, and only serverless compute. It does not say whether clients outside the workspace (JDBC, ODBC, dbt) can connect. The [dbt Core page](https://docs.databricks.com/aws/en/partners/prep/dbt) does not mention Free Edition.
- Tested from a laptop: `databricks-sql-connector`, the driver dbt-databricks uses, connected to the workspace's "Serverless Starter Warehouse" with a personal access token. The warehouse was stopped and started on the connection. Its auto stop is 10 minutes.
- `dbt debug` and `dbt build` passed against the same warehouse with dbt-core 1.12.7 and dbt-databricks 1.13.1.
- No numeric compute quota is published. Going over it shuts compute down for the rest of the day (ADR-0001).
- Every dbt-databricks release from 1.11 on caps `databricks-sdk` below 0.158. The project had `databricks-sdk>=0.169.0`, which nothing in the code needed.

## Decision

- dbt runs outside Databricks, on GitHub Actions and from a laptop, against the Starter Warehouse with a personal access token. This is the same setup as ingestion, so the secrets and the runner already exist.
- On a schedule, dbt runs once a day after the ingest workflow. Pull requests run it too, once #46 adds that. A run starts the warehouse if it is stopped, and the warehouse stops again after 10 idle minutes.
- dbt lives in its own `uv` group (`dbt`), so the ingest workflow does not install it. The `databricks-sdk` floor drops to 0.100 so it resolves with dbt-databricks. The lock picks 0.157.0, and the tests pass on it.
- Source freshness for `bronze.sgs_raw` uses `source_file_modification_time`, because every envelope field is a string. Ingest rewrites the current-year files every day, so a healthy table has a file under a day old. It warns after 26 hours and errors after 48.

## Consequences

- `make dbt-build` needs `DATABRICKS_HTTP_PATH` as well as the host and token.
- A scheduled dbt run that starts while the warehouse is over quota fails for the day. The next day's run picks up from bronze, since silver is rebuilt from it.
- `databricks-sdk` cannot move past what dbt-databricks allows. Upgrading the SDK waits for a dbt-databricks release with a higher cap.
- A dbt task inside a Databricks job was not tested on Free Edition. Revisit if running on Actions becomes a problem.
