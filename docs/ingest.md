# Ingest workflow

`.github/workflows/ingest.yml` runs the whole pipeline: pull the state files from the landing Volume, fetch SGS and PTAX from BACEN and the daily fund report from CVM, push the files and the state, run the `bronze` job and wait for it. Why it is built this way is in [ADR-0004](adr/0004-ingest-workflow.md).

## Schedule

Every day at 09:17 UTC for all SGS series and PTAX, and every Sunday at 10:47 UTC for CVM. A scheduled run is always `incremental`. Each fetch step checks `github.event.schedule` to see which cron started the run ([ADR-0008](adr/0008-cvm-inf-diario-landing.md)).

## Run it by hand

Actions, Ingest, Run workflow.

| Input | Meaning |
|---|---|
| mode | `incremental` or `backfill` |
| from | Backfill start, `yyyy-mm-dd`. Required for `backfill`, rejected for `incremental` |
| to | Backfill end, `yyyy-mm-dd`. Defaults to today |
| source | `all`, `sgs`, `ptax` or `cvm` |
| series | One SGS series code from `ingestion/config/sgs.toml` (432, 11, 12, 433). Empty means all. Ignored for PTAX and CVM |

Invalid input stops the run at the fetch step, before anything is uploaded. A backfill for one series leaves the watermarks of the other series alone, and PTAX keeps its watermark in its own file. `from` and `to` go to every connector that runs, so backfill PTAX history with `source` set to `ptax` and `from` set to `1984-01-01`. CVM takes whole months from them and refuses anything before 2021-01.

The same thing locally, with the `.env` from [setup.md](setup.md):

```bash
uv run python -m ingestion.upload pull
uv run python -m ingestion.sgs --series 433 backfill --from 2024-01-01 --to 2024-12-31
uv run python -m ingestion.ptax backfill --from 2024-01-01 --to 2024-12-31
uv run python -m ingestion.cvm backfill --from 2024-01-01 --to 2024-12-31
uv run python -m ingestion.upload push
uv run python databricks/run_bronze_job.py
```

## CVM backfill window

The CVM history starts at 2023-01. Backfill it with `source` set to `cvm`, mode `backfill` and `from` set to `2023-01-01`. That is 47 months up to 2026-11, 2.3 GB of CSV and 24.8 million rows, and it covers the two layouts CVM has used since then. Months from 2021-01 can be added later with another backfill. Earlier months are not supported (see [tech-debt.md](tech-debt.md)).

CVM changed the CSV columns twice. Bronze has one column for every name below ([bronze.md](bronze.md)).

| Layout | Files | Columns |
|---|---|---|
| A | HIST 2005 to 2019 | `CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST` |
| B | HIST 2000 to 2004 and 2020, monthly 2021-01 to 2023-11 | `TP_FUNDO` and then the columns of A |
| C | monthly from 2023-12 | `TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;ID_SUBCLASSE;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST` |

## Setup

- Secret `DATABRICKS_TOKEN` and repository variable `DATABRICKS_HOST` (setup.md, step 5).
- Optional repository variable `DATABRICKS_CATALOG`. Without it the workflow uses `workspace`.
- The job `bronze` has to exist. Run `databricks/create_bronze_job.py` once (see [bronze.md](bronze.md)). The workflow fails with a message if it is missing.

## Reading a run

The step summary of every run has the link to the Databricks job run, also when the run failed. Any failed step, a bronze run that is not `SUCCESS` and a bronze run that takes more than 30 minutes turn the workflow red.

## Manual checks

1. Run with mode `backfill`, from `2024-01-01`, to `2024-12-31`, series `433`. The summary has a job run link and the run is green. The reconciliation query in bronze.md matches for series 433.
2. Run with series `99999`. The run fails at the fetch step and nothing is uploaded.
3. Run with `source` set to `ptax`, mode `backfill` and `from` set to `1984-01-01`. The run is green, `bronze.ptax_raw` has one row per year file, and the PTAX reconciliation in bronze.md matches.
4. Wait for the first scheduled run after the PTAX change and check that it is green and that `ptax/year=<current year>.json` was rewritten.
5. Run with `source` set to `cvm` and mode `incremental`. Fetch SGS and Fetch PTAX are skipped, Fetch CVM refetches the last two months, and the CVM reconciliation in bronze.md matches.
6. Wait for the first Sunday run and check that only Fetch CVM ran.

## Verified

Run on GitHub on 2026-11-23 (UTC).

- Check 1: run 45095844945 (backfill, 2024, series 433) was green. The step summary of a run shows "Bronze job run" with the link to the Databricks run, then "Bronze job succeeded".
- Check 2: run 45095977203 (series 99999) failed at the fetch step with "series 99999 is not in the config" and uploaded nothing.
- Run 45097032913 (incremental, all series) was green.
- Run 45113207172 backfilled all four series from 1995-01-01 in 2m56s and uploaded 124 year files plus the state file. The reconciliation query in bronze.md matched for every file.
- Check 3: no scheduled run yet. The first one is due at 09:17 UTC on 2026-11-23.

PTAX (#42), 2026-11-23.

- Before the merge, the same commands were run from a laptop with the workflow's `.env` values (`ingestion.upload pull`, `ingestion.ptax backfill --from 1984-01-01`, `ingestion.upload push`, `databricks/create_bronze_job.py`, `databricks/run_bronze_job.py`). The results are in the Verified section of [bronze.md](bronze.md).
- The 09:17 UTC schedule did not start a run that day. The workflow had no `schedule` runs at all, before or after the merge, and none was cancelled. The manual runs of the night before had finished long before 09:17, and `cancel-in-progress` is false. The cause is not known.
- Run 45209377853 (manual, `mode` incremental, `source` all, on `main` after the merge) was green in about 1.5 minutes. The steps Fetch SGS, Fetch PTAX, push and the `bronze` job all succeeded. Afterwards `bronze.ptax_raw` had 45 rows for 43 files, with `ptax/year=2026.json` read three times, and `bronze.sgs_raw` had 144 rows for 124 files (the four 2026 files read once more).
- Check 3 is done by that manual run. Check 4 (the first run with the `schedule` event) is still open.

CVM (#43), 2026-11-23.

- The backfill from 2023-01 ran from a laptop with the workflow's commands. The results are in the Verified section of [bronze.md](bronze.md).
- Check 5: run 45265550213 (manual, incremental, `source` cvm, on the branch before the merge) was green in 54 s. Fetch SGS and Fetch PTAX were skipped. Fetch CVM fetched `inf_diario_fi_202610.zip` and `inf_diario_fi_202611.zip`, the push uploaded the two month files and the three state files, and the `bronze` job succeeded.
- The workflow still has no `schedule` runs. The 09:17 UTC cron did not start a run on 2026-11-23 either, so check 4 and check 6 are open.
