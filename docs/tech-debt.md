# Tech debt

Known shortcuts that do not justify an issue yet. Add a line when you take one, delete it when it is fixed.

- `databricks/setup_workspace.py` prints with `print`, while `ingestion/http.py` uses `logging`. Switch when a second script needs logging.
- `silver.sgs_series` filters bronze on `source_file_modification_time` greater than the silver maximum, so a bronze row that arrives with an older timestamp is skipped until `dbt run --full-refresh`.
- The workspace still has the job `bronze_sgs` and the script `bronze_sgs.py` from before #42. Delete both by hand after the first run of the `bronze` job on the schedule.
- `ingestion/ptax.py` has its own watermark read and write, next to the SGS ones in `ingestion/sgs.py`. Merge them if a third connector needs the same thing.
- A run with `source=ptax` or `source=sgs` still starts both bronze tasks. Split the job run per source if the extra task time matters.
