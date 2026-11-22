# Tech debt

Known shortcuts that do not justify an issue yet. Add a line when you take one, delete it when it is fixed.

- `databricks/setup_workspace.py` prints with `print`, while `ingestion/http.py` uses `logging`. Switch when a second script needs logging.
- `HttpClient.get` treats any status below 400 as success, so a 3xx would be returned to the caller (httpx does not follow redirects by default). Check the real SGS endpoint in #11 and decide whether to follow redirects or raise.
