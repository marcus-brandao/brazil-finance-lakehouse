# Ingest workflow

`.github/workflows/ingest.yml` runs the whole pipeline: pull the state files from the landing Volume, fetch SGS and PTAX from BACEN, push the files and the state, run the `bronze` job and wait for it. Why it is built this way is in [ADR-0004](adr/0004-ingest-workflow.md).

## Schedule

Every day at 09:17 UTC. A scheduled run is always `incremental`, for all SGS series and for PTAX.

## Run it by hand

Actions, Ingest, Run workflow.

| Input | Meaning |
|---|---|
| mode | `incremental` or `backfill` |
| from | Backfill start, `yyyy-mm-dd`. Required for `backfill`, rejected for `incremental` |
| to | Backfill end, `yyyy-mm-dd`. Defaults to today |
| source | `all`, `sgs` or `ptax` |
| series | One SGS series code from `ingestion/config/sgs.toml` (432, 11, 12, 433). Empty means all. Ignored for PTAX |

Invalid input stops the run at the fetch step, before anything is uploaded. A backfill for one series leaves the watermarks of the other series alone, and PTAX keeps its watermark in its own file. `from` and `to` go to both connectors, so backfill PTAX history with `source` set to `ptax` and `from` set to `1984-01-01`.

The same thing locally, with the `.env` from [setup.md](setup.md):

```bash
uv run python -m ingestion.upload pull
uv run python -m ingestion.sgs --series 433 backfill --from 2024-01-01 --to 2024-12-31
uv run python -m ingestion.ptax backfill --from 2024-01-01 --to 2024-12-31
uv run python -m ingestion.upload push
uv run python databricks/run_bronze_job.py
```

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

## Verified

Run on GitHub on 2026-11-23 (UTC).

- Check 1: run 45095844945 (backfill, 2024, series 433) was green. The step summary of a run shows "Bronze job run" with the link to the Databricks run, then "Bronze job succeeded".
- Check 2: run 45095977203 (series 99999) failed at the fetch step with "series 99999 is not in the config" and uploaded nothing.
- Run 45097032913 (incremental, all series) was green.
- Run 45113207172 backfilled all four series from 1995-01-01 in 2m56s and uploaded 124 year files plus the state file. The reconciliation query in bronze.md matched for every file.
- Check 3: no scheduled run yet. The first one is due at 09:17 UTC on 2026-11-23.

PTAX (#42), 2026-11-23. The workflow has not run on GitHub with the PTAX steps yet, because the change was not merged. The same commands were run from a laptop with the workflow's `.env` values (`ingestion.upload pull`, `ingestion.ptax backfill --from 1984-01-01`, `ingestion.upload push`, `databricks/create_bronze_job.py`, `databricks/run_bronze_job.py`). Checks 3 and 4 above are still open and belong to the first runs after the merge. Results are in the Verified section of [bronze.md](bronze.md).
