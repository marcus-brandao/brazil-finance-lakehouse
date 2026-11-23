# Tech debt

Known shortcuts that do not justify an issue yet. Add a line when you take one, delete it when it is fixed.

- `databricks/setup_workspace.py` prints with `print`, while `ingestion/http.py` uses `logging`. Switch when a second script needs logging.
- `silver.sgs_series` filters bronze on `source_file_modification_time` greater than the silver maximum, so a bronze row that arrives with an older timestamp is skipped until `dbt run --full-refresh`.
