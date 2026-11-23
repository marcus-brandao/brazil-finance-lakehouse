# 0004. Ingest workflow: queued runs, a job that must already exist

- Status: accepted
- Date: 2026-11-22

## Context

`ingest.yml` pulls the state file from the landing Volume, runs the SGS connector, pushes the files and the state, then runs the bronze job and waits for it. Facts checked on 2026-11-22:

- BCB's Plano Diretor da Taxa Selic (Resolução BCB 46) says the daily Selic is published by 19:00 Brasília time on each business day, and by 13:30 on the last business day of the year and on 24 December. Copom decisions (series 432) come out in the evening. BCB's pages for series 11 and 432 give no publication time, and the page for series 12 returned 404. How long SGS takes to show a published value is not documented, so it is not verified.
- GitHub documents that scheduled runs can be delayed under load, that the start of the hour is the busiest time, and that queued scheduled runs can be dropped.
- A cancelled run stops where it is. A run cancelled between `push` and the bronze job leaves the files on the Volume with no bronze run started. A run cancelled while waiting leaves the Databricks run going with nobody watching it.
- The bronze job has `max_concurrent_runs=1`, so a second `run_now` while one is active is skipped.

## Decision

- Cron `17 9 * * *` (09:17 UTC, 06:17 in Brasília). It is the morning after the 19:00 release and the evening Copom decision. The minute avoids the top of the hour. A late or dropped run is covered by the next one, because incremental starts from the watermark minus the lookback.
- One concurrency group, `ingest-sgs`, with `cancel-in-progress: false`. Runs queue and never overlap. GitHub keeps one pending run per group, so a newer pending run replaces an older one that has not started.
- The workflow does not create the bronze job. `databricks/run_bronze_job.py` looks it up by name and exits with a message pointing at `create_bronze_job.py` if it is missing. Creating it is a deploy step (it uploads the script and sets the catalog), and doing that on every scheduled run would hide a missing or edited job.
- The run URL is written to the step summary right after the run starts, before the wait. A run that is not `SUCCESS` (failed, timed out after 30 minutes, skipped) exits non-zero.
- `DATABRICKS_CATALOG` comes from a repository variable, with `workspace` as the fallback. `DATABRICKS_HOST` is a repository variable too: GitHub masks the value of a secret in the step summary, which turned the job run link into `***/?o=...` in the first run on 2026-11-23. Only `DATABRICKS_TOKEN` is a secret. Both are set on the ingest job only, and `ci.yml` has no Databricks credentials.
- The dispatch inputs reach the shell as environment variables and go into a bash array. The connector's argument parser validates them before any request or upload.

## Consequences

- A manual backfill queued behind a scheduled run waits for it, and a second dispatch queued behind the first replaces the pending one.
- If the workflow is cancelled by hand, check the Databricks run and the Volume before running again. Files without a bronze run are picked up by the next successful run, because Auto Loader reads what is new.
- The first run on a new workspace fails with "job bronze_sgs not found" until `create_bronze_job.py` has been run.
- Scheduled workflows in a public repo are turned off after 60 days without repository activity (GitHub docs). Any commit or a manual run restarts them.
