# Tech debt

Known shortcuts that do not justify an issue yet. Add a line when you take one, delete it when it is fixed.

- `databricks/setup_workspace.py` prints with `print`, while `ingestion/http.py` uses `logging`. Switch when a second script needs logging.
- CI reports coverage but enforces no threshold. Add one once the connectors and the bronze job settle.
