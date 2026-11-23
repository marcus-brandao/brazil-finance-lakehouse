# 0007. PTAX connector: calendar-year windows, own state file, one bronze script for both sources

- Status: accepted
- Date: 2026-11-23

## Context

The daily USD/BRL PTAX comes from the BACEN Olinda API, service PTAX v1. Checked against https://olinda.bcb.gov.br on 2026-11-23 (the `$metadata` document, the dataset page on dadosabertos.bcb.gov.br and live requests):

- `CotacaoDolarPeriodo(dataInicial, dataFinalCotacao)` returns the closing PTAX rows for a date range: `cotacaoCompra`, `cotacaoVenda` and `dataHoraCotacao`. The opening and intermediate bulletins come from `CotacaoMoedaPeriodo` with a `moeda` and a `tipoBoletim` field. The dataset page says the closing bulletin is the arithmetic mean of the day's bulletins.
- Dates are `MM-DD-YYYY` in single quotes, passed as `@dataInicial='01-31-2025'`. An ISO date (`2025-01-01`) returns 200 with no rows. A day-first date with a day over 12 returns 500 with `"Erro desconhecido"`.
- A weekend, a holiday (2025-12-25) and a reversed range all return 200 with `{"value": []}`. There is no 404 for "no data", unlike SGS.
- No range limit is documented, and none showed up. The whole history (1984-12-03 to 2026-11-19, 10514 rows, 43 calendar years) came back in one request in 0.9 s, without an `@odata.nextLink`.
- `http://` returns a 404 HTML page, not a redirect.
- Values are JSON numbers (`"cotacaoCompra": 6.208`), typed `Edm.Decimal` with scale 5 in `$metadata`. `dataHoraCotacao` is a string such as `2025-12-31 10:06:24.183`. Its time zone is not documented.
- Nine days have more than one closing row: 1984-12-03, 1984-12-05 (three rows), 1990-01-03, 1991-01-02, 1996-01-12, 1996-04-11, 1997-08-29, 2001-12-21 (two rows with the same timestamp) and 2025-04-23 (two rows 27 ms apart).
- 2026-11-20 is a holiday and has no row, and the 23rd was not published yet when this was written, so the newest row is 2026-11-19. The closing rows in 2025 and 2026 are stamped around 13:00.
- The Olinda docs say nothing about revisions of past closing rates.

## Decision

- One window per calendar year, always January 1 to December 31, as in [ADR-0002](0002-sgs-windows-and-watermark.md). The path is `ptax/year=<yyyy>.json`, so any run that touches a year rewrites that year's file in full. The API has no cap that forces this, but it keeps paths deterministic, files at about 250 rows, and the daily rewrite to the current year. A year with no rows writes no file.
- Only the closing PTAX is fetched. The intermediate bulletins would be a second source with its own path and table, added when something needs them.
- The envelope is the SGS one without `series_code`: `source` (`bcb_ptax`), `window_start`, `window_end`, `ingested_at`, `run_id` and `payload`. The payload is the `value` array as returned.
- The watermark is the latest row date at or before today, stored in its own file, `_state/ptax.json`, as `{"usd": "<yyyy-mm-dd>"}`. A backfill never moves it back. The separate file means a PTAX backfill cannot overwrite the SGS watermarks, which is the risk ADR-0002 describes for a backfill on an empty disk. The uploader pulls both state files and pushes both last.
- `incremental` starts at the watermark minus 10 days and runs through today, refetching whole years. The 10 days is a constant in `ingestion/ptax.py`, because there is one series and no config file. With no watermark it fails with a message to run `backfill`.
- A 200 with an empty `value` is no data. Any other status goes through `HttpClient` unchanged: 429 and 5xx are retried with backoff, everything else raises.
- `databricks/bronze.py` takes `--source sgs|ptax`, with the columns, table, landing path and checkpoint per source in one dict. The SGS checkpoint path is the same as before, so the SGS stream is not read again. The job is named `bronze` and has one task per source, with no dependency between them.
- PTAX joins the ingest workflow, now named `Ingest`, with a `source` input (`all`, `sgs`, `ptax`). Two workflows would need separate concurrency groups, and two runs pushing state at once is what the single group prevents. See the update in [ADR-0004](0004-ingest-workflow.md).

## Consequences

- A format mistake in the date gives an empty window, not an error. The unit test pins the exact query. The reconciliation in [bronze.md](../bronze.md) would not catch it, because an empty window writes no file.
- The connector parses the response and writes it again, so a number the API sends as `6.20860` is stored as `6.2086`. The value is the same.
- Every run refetches the current year, so a revised value in it is picked up on the next run. A revision to a past year is picked up only through the lookback around New Year, or by a backfill of that year.
- Bronze keeps the nine days with several closing rows as they came. Silver has to decide which row counts, and that is part of the PTAX silver model.
- Changing the job name from `bronze_sgs` to `bronze` leaves the old job and the old script in the workspace until they are deleted by hand.
- A run with `source=ptax` still starts both bronze tasks. The SGS one finds nothing new and stops.
