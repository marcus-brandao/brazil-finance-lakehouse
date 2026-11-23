# Setup

How to get from nothing to a Databricks workspace with the catalog, schemas and landing Volume this project expects.

## 1. Create the workspace

Sign up for [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition). It creates a workspace with serverless compute and default storage. Copy the workspace URL from the browser, for example `https://dbc-xxxx.cloud.databricks.com`.

## 2. Create an access token

In the workspace, open your user menu, then Settings, Developer, Access tokens, and generate a token. Copy it once, it is not shown again.

## 3. Configure local env

```bash
cp .env.example .env
```

Fill in `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in `.env`. The file is gitignored. `DATABRICKS_CATALOG` is optional and defaults to `workspace`, the catalog every Free Edition workspace starts with.

## 4. Create the objects

```bash
set -a && source .env && set +a
uv sync
uv run python databricks/setup_workspace.py
```

The script creates:

- the catalog named by `DATABRICKS_CATALOG`. On Free Edition the API refuses `CREATE CATALOG` ("Metastore storage root URL does not exist"), so a new catalog name falls back to `workspace`. To use your own catalog, create it in the Catalog UI with Default Storage and set the variable to its name.
- schemas `bronze`, `silver` and `gold`
- Volume `bronze.landing`, where the connectors upload raw files

Rerunning it leaves existing objects alone. The last line prints the catalog it used, for example `Ready: workspace.bronze.landing`. Use that name in later steps.

## 5. Add GitHub secrets

Ingestion runs on GitHub Actions, so the workflows need the same credentials. In the repo, open Settings, Secrets and variables, Actions. Add `DATABRICKS_TOKEN` as a secret. Add `DATABRICKS_HOST` as a repository variable under the Variables tab, because the workspace URL is not a credential and GitHub would hide it in the run summary if it were a secret. If you use a catalog other than `workspace`, add a variable `DATABRICKS_CATALOG` too.

## 6. Check dbt

dbt connects to the workspace's SQL warehouse. In the workspace, open SQL Warehouses, then the warehouse (Free Edition has one, "Serverless Starter Warehouse"), then Connection details, and copy the HTTP path into `DATABRICKS_HTTP_PATH` in `.env`.

```bash
set -a && source .env && set +a
uv sync --group dbt
uv run --group dbt dbt debug --project-dir dbt --profiles-dir dbt
make dbt-build
```

`dbt debug` ends with "All checks passed!". `make dbt-build` checks that `bronze.sgs_raw` was loaded in the last 48 hours, then runs the dbt tests. Run it after the first ingest, or the freshness check fails on an empty table.

## Rebuilding

The workspace can be deleted or lost to inactivity. Repeat steps 1 to 6 on a new one. Nothing else in the repo depends on the old workspace.
