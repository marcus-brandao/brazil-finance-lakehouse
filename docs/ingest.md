# Ingest workflow

`.github/workflows/ingest.yml` runs the whole SGS pipeline: pull the state file from the landing Volume, fetch from BACEN, push the files and the state, run the `bronze_sgs` job and wait for it. Why it is built this way is in [ADR-0004](adr/0004-ingest-workflow.md).

## Schedule

Every day at 09:17 UTC. A scheduled run is always `incremental` for all series.

## Run it by hand

Actions, Ingest SGS, Run workflow.

| Input | Meaning |
|---|---|
| mode | `incremental` or `backfill` |
| from | Backfill start, `yyyy-mm-dd`. Required for `backfill`, rejected for `incremental` |
| to | Backfill end, `yyyy-mm-dd`. Defaults to today |
| series | One series code from `ingestion/config/sgs.toml` (432, 11, 12, 433). Empty means all |

Invalid input stops the run at the fetch step, before anything is uploaded. A backfill for one series leaves the watermarks of the other series alone.

The same thing locally, with the `.env` from [setup.md](setup.md):

```bash
uv run python -m ingestion.upload pull
uv run python -m ingestion.sgs --series 433 backfill --from 2024-01-01 --to 2024-12-31
uv run python -m ingestion.upload push
uv run python databricks/run_bronze_job.py
```

## Setup

- Secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` (setup.md, step 5).
- Optional repository variable `DATABRICKS_CATALOG`. Without it the workflow uses `workspace`.
- The job `bronze_sgs` has to exist. Run `databricks/create_bronze_job.py` once (see [bronze.md](bronze.md)). The workflow fails with a message if it is missing.

## Reading a run

The step summary of every run has the link to the Databricks job run, also when the run failed. Any failed step, a bronze run that is not `SUCCESS` and a bronze run that takes more than 30 minutes turn the workflow red.

## Manual checks

1. Run with mode `backfill`, from `2024-01-01`, to `2024-12-31`, series `433`. The summary has a job run link and the run is green. The reconciliation query in bronze.md matches for series 433.
2. Run with series `99999`. The run fails at the fetch step and nothing is uploaded.
3. Wait for the first scheduled run and check that it is green.

## Verified

Not run yet.
