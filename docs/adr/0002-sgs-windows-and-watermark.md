# 0002. SGS connector: calendar-year windows, per-series watermark, 404 and 304 handling

- Status: accepted
- Date: 2026-11-22

## Context

The BACEN SGS API returns JSON for a date range. Checked against https://api.bcb.gov.br on 2026-11-22:

- A window over 10 years returns 406. The cap applies to series of daily periodicity.
- A window with no data returns 404 with the body `{"erro":{"statusCode":404,"detail":"...SGSNegocioException: Value(s) not found"}}`, not an empty list.
- An unknown series code also returns 404, with the message "A serie nao existe!". The status code cannot tell the two cases apart.
- `http://` answers 307 to `https://` with an empty body.
- Series 432 (Selic target) has a row for every calendar day, weekends included, and already has rows for dates after today.
- Values are strings (`"valor": "0.050788"`). Typing happens in silver.
- On 2026-11-22 the JSON request for all of 2026 returned 304 with an empty body for series 432 and 1, although the request had no conditional headers (#32). Series 11, 12 and 433 returned 200 for the same window, and 432 and 1 returned 200 for 2024 and 2025. Moving either date by one day, or asking for each half of the year, returned 200. Reordering the parameters, encoding the slashes, adding a cache-busting parameter, changing User-Agent or Accept, and sending `Cache-Control: no-cache` or a non-matching `If-None-Match` still got 304, so a cache keyed on the URL does not explain it. The 304 has the same weak ETag as the 200 body for the same rows, `cache-control: public, max-age=900` and no Last-Modified. The BCB docs say nothing about 304, ETag or caching. The cause is unknown.

Landing files have to be deterministic, because a re-run must overwrite the same file (issue #1).

## Decision

- One window per calendar year, always January 1 to December 31, whatever range was asked for. A range is split into the years it touches. The path is `sgs/series_code=<code>/year=<yyyy>.json`, so any run that touches a year rewrites that year's file in full. This is well inside the 10-year cap.
- The watermark is the latest row date at or before today, stored per series in `_state/sgs.json` as `{"<code>": "<yyyy-mm-dd>"}`. Rows after today are ignored because of series 432. A backfill never moves a watermark back.
- `incremental` starts at the watermark minus `lookback_days` (set per series in `ingestion/config/sgs.toml`) and runs through today. It refetches whole years, so a partial window never overwrites a full year file. A series with no watermark fails with a message to run `backfill` first.
- A 404 whose body contains "Value(s) not found" means no data in the window: no file is written and the run continues. Any other 404 (unknown series) and any other `HttpError` raises. The connector reads `HttpError.body` to decide.
- `HttpClient` treats any status of 300 or more as an error and does not follow redirects, so a 3xx raises instead of returning an empty body as data.
- A 304 on a year window is retried once as two requests, January 1 to June 30 and July 1 to December 31, and the two row lists are joined in date order. The file, its `window_start` and `window_end`, the path and the watermark are the same as for a single request. A half that answers "Value(s) not found" counts as empty. A 304 or any other error on a half raises, so a 304 never turns into an empty window.
- The connector writes to a local directory (`out/` by default) laid out as the landing Volume. The uploader (#12) moves it to the Volume and reads and writes the state file there.
- Config is TOML, read with the standard library `tomllib`. No new dependency.

### Where the state lives

The state file is `_state/sgs.json` on the landing Volume, next to the files it describes.

- The uploader pushes it last, so a failed upload never moves the watermark past data that did not land. That order only protects anything if the state and the data go to the same place through the same tool.
- Actions already has a token that can write to the Volume, so nothing new needs setting up.
- A rebuilt workspace starts with no files and no state, so the two stay consistent.

Alternatives considered:

- GitHub Actions cache: the docs say entries not accessed for 7 days are removed, and the retention setting also defaults to 7 days. A week without a run would lose the watermarks. Artifacts last 90 days by default, but they belong to a run, and reading one needs an API download of the last run's artifact.
- A file committed to the repo: it needs write access, and `ingest.yml` has `contents: read`. It would also add a commit per day.
- A Delta table: reading and writing one row from Actions needs the SQL warehouse, which is one 2X-Small and counts against the quota.
- The newest date in bronze: it needs a query before every run, and bronze holds several copies of each rewritten file.

## Consequences

- Telling "no data" from "unknown series" depends on the text "Value(s) not found" in the error body. If BCB rewords it, empty windows fail the run instead of being skipped, and the error shows up in the first scheduled run.
- Each run rewrites a few whole-year files, which is little data. A year file can change after it was first written (revised values), so bronze keeps duplicates and silver keeps the row with the latest `ingested_at` per series and date.
- The lookback is in days for every series. Series 433 is monthly and uses 95 days, about three months, which covers its revisions without month arithmetic.
- Pull the state file before any run that writes state, backfills included. A backfill on an empty disk writes a state file with only its own series, and the upload would replace the full state on the Volume.
- If the state file is deleted, `incremental` fails with "run backfill" until a backfill rewrites it.
- When the 304 fallback runs, `payload` is the rows of two API responses joined in date order. For 432 and 1 in 2026 they equal the full-year rows (compared with the 200 answer for the same year without `formato`). If BCB starts answering 304 to half years as well, the run fails and needs another look.
