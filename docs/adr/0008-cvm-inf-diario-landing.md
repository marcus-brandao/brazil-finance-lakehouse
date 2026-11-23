# 0008. CVM daily fund report: monthly files, raw CSV in bronze, one month of lookback

- Status: accepted
- Date: 2026-11-23

## Context

The CVM daily fund report (informe diario) is published as zip files on the CVM open data portal, with no API that takes a date range. Checked against https://dados.cvm.gov.br/dataset/fi-doc-inf_diario, the directory listings under `dados/FI/DOC/INF_DIARIO/` and the files themselves on 2026-11-23:

- `DADOS/inf_diario_fi_<yyyymm>.zip` holds one CSV per month, from 202101 to the current month. A month that is not published yet returns 404 (`202612` on that day). The current month is partial: `202611` had dates from the 2nd to the 19th.
- `DADOS/HIST/inf_diario_fi_<yyyy>.zip` holds 2000 to 2020. Most yearly zips hold 12 monthly CSVs, but `2000` holds one yearly CSV, `inf_diario_fi_2000.csv`.
- The dataset page says the current month and the previous one are rewritten every day from Monday to Saturday, with resubmissions, and months M-2 to M-11 once a week. The listing agrees: on 2026-11-23 the last two months had that day's timestamp, and 202512 to 202609 were all rewritten on Saturday 2026-11-21. Older months stop changing. 202101 was last written in May 2022.
- The CSV has a header row, `;` as the separator, CRLF line ends and dates as `yyyy-mm-dd`. Every file sampled (200501, 202012, 202101, 202306, 202310, 202401, 202412, 202611) is plain ASCII.
- There are three layouts. The column names are in `docs/ingest.md`.
  - A: 8 columns starting with `CNPJ_FUNDO`. HIST 2005 to 2019.
  - B: A with `TP_FUNDO` in front. HIST 2000 to 2004 (rewritten in 2022), HIST 2020 and 202101 to 202311.
  - C: 10 columns, with `TP_FUNDO_CLASSE`, `CNPJ_FUNDO_CLASSE` and `ID_SUBCLASSE` in place of the two fund columns. From 202312, after CVM Resolution 175.
- A month from 2023 on is a 9 to 12 MB zip, 39 to 55 MB of CSV and 440 to 570 thousand rows. The 47 months from 2023-01 to 2026-11 are 2.3 GB of CSV and 24.8 million rows. The whole history is about 1.7 GB zipped, so roughly 8 GB of CSV.
- Spark's CSV docs say a given schema is matched to CSV columns by position. A test job on the Free Edition workspace with one file of each layout showed that Auto Loader, with a given schema, `header` set and `rescuedDataColumn`, matches columns by header name. Each layout filled its own columns and left the others null. A file with an extra column put it in `_rescued_data` with the file path.

## Decision

- One window per calendar month. The CSV inside the monthly zip lands byte for byte at `cvm_inf_diario/month=<yyyy-mm>.csv`, so the same month always writes the same path.
- Only the monthly files from 2021-01 are supported. The connector refuses an earlier month. HIST is left out because its zips are not all split by month and the backfill starts later anyway.
- Bronze gets the CSV with no JSON envelope. Wrapping 50 MB of CSV in one JSON string would make one huge row per file and push CSV parsing into silver SQL. This changes the bronze rule of ADR-0003 for this source: each bronze row is one CSV line, where SGS and PTAX keep one row per file. `source` and the window come from `source_file_path`. `ingested_at` and `run_id` are not kept.
- `bronze.cvm_inf_diario_raw` has the union of the three layouts, every column a string, plus `source_file_path`, `source_file_modification_time` and `_rescued_data`. Each source in `databricks/bronze.py` may give its own reader options. CVM reads `csv` with `header`, `sep=";"` and `encoding=ISO-8859-1`. CVM publishes its other CSVs in Latin-1, and ASCII reads the same either way. `allowOverwrites` stays on for all sources.
- The watermark is the newest month written, in `_state/cvm.json` as `{"inf_diario": "<yyyy-mm>"}`. A backfill never moves it back. `incremental` fetches from one month before the watermark through the current month.
- A 404 on the current month means it is not published yet, so the month is skipped and the watermark stays. A 404 on any other month fails the run.
- CVM joins the ingest workflow with its own weekly cron, Sundays at 10:47 UTC, after the Saturday rewrite. The steps check `github.event.schedule`: the daily cron runs SGS and PTAX, the weekly one runs CVM. One workflow keeps one concurrency group for the state files ([ADR-0004](0004-ingest-workflow.md)).
- The default backfill starts at 2023-01, which covers layouts B and C. A later backfill of 2021 and 2022 adds files at new paths and needs nothing else.

## Consequences

- Revisions to months M-2 to M-11 after a month has dropped out of the lookback are not picked up. A backfill of those months picks them up.
- Each run appends the rows of the two refetched months again, about a million rows a week. Silver keeps the newest file version per month. A retention step for bronze is in [tech-debt.md](../tech-debt.md).
- Silver has to merge the fund columns of the layouts (`CNPJ_FUNDO` and `CNPJ_FUNDO_CLASSE`, `TP_FUNDO` and `TP_FUNDO_CLASSE`).
- A new column from CVM lands in `_rescued_data` until it is added to `SOURCES`.
- The dbt freshness check on `cvm_inf_diario_raw` allows 8 days before a warning, since the source runs weekly.
- A CVM run still starts all three bronze tasks. SGS and PTAX find nothing new and stop.
