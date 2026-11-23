# Tech debt

Known shortcuts that do not justify an issue yet. Add a line when you take one, delete it when it is fixed.

- `databricks/setup_workspace.py` prints with `print`, while `ingestion/http.py` uses `logging`. Switch when a second script needs logging.
- `silver.sgs_series` filters bronze on `source_file_modification_time` greater than the silver maximum, so a bronze row that arrives with an older timestamp is skipped until `dbt run --full-refresh`.
- The workspace still has the job `bronze_sgs` and the script `bronze_sgs.py` from before #42. Delete both by hand after the first run of the `bronze` job on the schedule.
- A run for one source (`source=ptax`, `sgs` or `cvm`, or the weekly CVM cron) still starts all three bronze tasks. Split the job run per source if the extra task time matters.
- `ingestion/cvm.py` only reads the monthly files from 2021-01. Earlier months are in yearly zips under `DADOS/HIST`, and 2000 is one yearly CSV. Add a HIST reader if silver needs history before 2021.
- `bronze.cvm_inf_diario_raw` gets two months of rows again on every CVM run, about a million rows a week. Add a retention step that drops older copies of a month if the table gets too big for Free Edition.
- CVM revisions to months older than M-1 are missed once a month leaves the lookback (ADR-0008). Backfill the last 12 months now and then if silver needs them.
