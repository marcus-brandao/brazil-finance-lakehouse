# 0002. SGS connector: calendar-year windows, per-series watermark, 404 handling

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

Landing files have to be deterministic, because a re-run must overwrite the same file (issue #1).

## Decision

- One window per calendar year, always January 1 to December 31, whatever range was asked for. A range is split into the years it touches. The path is `sgs/series_code=<code>/year=<yyyy>.json`, so any run that touches a year rewrites that year's file in full. This is well inside the 10-year cap.
- The watermark is the latest row date at or before today, stored per series in `_state/sgs.json` as `{"<code>": "<yyyy-mm-dd>"}`. Rows after today are ignored because of series 432. A backfill never moves a watermark back.
- `incremental` starts at the watermark minus `lookback_days` (set per series in `ingestion/config/sgs.toml`) and runs through today. It refetches whole years, so a partial window never overwrites a full year file. A series with no watermark fails with a message to run `backfill` first.
- A 404 whose body contains "Value(s) not found" means no data in the window: no file is written and the run continues. Any other 404 (unknown series) and any other `HttpError` raises. The connector reads `HttpError.body` to decide.
- `HttpClient` treats any status of 300 or more as an error and does not follow redirects, so a 3xx raises instead of returning an empty body as data.
- The connector writes to a local directory (`out/` by default) laid out as the landing Volume. The uploader (#12) moves it to the Volume and reads and writes the state file there.
- Config is TOML, read with the standard library `tomllib`. No new dependency.

## Consequences

- Telling "no data" from "unknown series" depends on the text "Value(s) not found" in the error body. If BCB rewords it, empty windows fail the run instead of being skipped, and the error shows up in the first scheduled run.
- Each run rewrites a few whole-year files, which is little data. A year file can change after it was first written (revised values), so bronze keeps duplicates and silver keeps the row with the latest `ingested_at` per series and date.
- The lookback is in days for every series. Series 433 is monthly and uses 95 days, about three months, which covers its revisions without month arithmetic.
- Pull the state file before any run that writes state, backfills included. A backfill on an empty disk writes a state file with only its own series, and the upload would replace the full state on the Volume.
