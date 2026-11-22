# 0001. Platform: Databricks Free Edition, GitHub Actions, dbt, Evidence

- Status: accepted
- Date: 2026-11-22

## Context

This is a public portfolio project. It has to show lakehouse practice that carries over to real jobs: a catalog, Delta tables, incremental ingestion, transformations with tests, BI, CI/CD. The constraints are:

- $0 with no end date. The project must still run months from now, so time-limited trials don't work.
- Reproducible: anyone can clone the repo and run it on a free account.
- Non-commercial. This is a personal project, so a free tier that forbids commercial use is fine.

What Databricks Free Edition gives and limits (from the [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) page, checked 2026-11-22):

- Serverless compute only. No custom or classic clusters, and no GPU.
- Outbound internet access only to "a limited set of trusted domains". The list isn't published, so we assume BACEN and CVM are not reachable.
- One SQL warehouse, 2X-Small. At most 5 concurrent job tasks per account. One active Lakeflow pipeline per pipeline type.
- A fair-use policy: going over quota shuts compute down for the rest of the day, or in extreme cases the month. Data is kept. Accounts inactive for a long time may be deleted.
- There is no SLA or support.

## Decision

| Concern | Choice |
|---|---|
| Lakehouse platform | Databricks Free Edition: Unity Catalog, Delta, Auto Loader, serverless SQL |
| Ingestion and scheduling | Python connectors on GitHub Actions, which upload raw files to a Unity Catalog Volume |
| Transformations | dbt against the serverless SQL warehouse |
| BI | Evidence (static site, versioned in git) and Databricks AI/BI dashboards |
| CI/CD | GitHub Actions |

Ingestion runs outside Databricks because of the egress restriction. GitHub Actions has open internet access, free minutes for public repos, a cron scheduler and encrypted secrets, so it fetches from BACEN/CVM and pushes files in. Databricks takes over at the landing Volume.

### Alternatives considered

- Local-only stack (DuckDB + dbt-duckdb): free and simple, but it shows none of the platform side (catalog, governance, managed ingestion, warehouse) and has nothing to host. It stays an option for fast local dbt tests later.
- Microsoft Fabric trial: time-limited, so the project would stop working when the trial ends.
- Snowflake trial: time-limited and credit-limited, so it has the same problem. It also has no native equivalent of Delta + Auto Loader on files.
- Power BI: Desktop is Windows-only, and publishing dashboards publicly needs a paid licence or a tenant. Evidence builds to a static site anyone can open, and AI/BI dashboards ship with Free Edition.

## Consequences

- Two places run code: GitHub Actions (ingestion) and Databricks (bronze onward). They hand off at the landing Volume, so each side can be tested without the other.
- Connectors are plain Python with no Spark, so unit tests run locally with mocked HTTP.
- Compute is small (one 2X-Small warehouse, 5 concurrent tasks). Jobs must be incremental, and full-history backfills get split into windows. This suits the daily, low-volume BACEN/CVM data.
- Quota shutdowns and the risk of deletion after inactivity mean the workspace isn't durable storage. Everything (code, dbt models, dashboards, bundle config) lives in git and can be redeployed into a fresh workspace.
- Streaming (M5) is limited to what serverless and one pipeline per type allow. Revisit this ADR then.
- Confirmed in a live workspace (2026-11-22): personal access tokens work, and the Databricks SDK can create schemas and Volumes through the REST API. The API cannot create a catalog ("Metastore storage root URL does not exist"), so the project uses the default `workspace` catalog.
- File uploads to a Volume work: the Files API accepted overwrites with a personal access token, and a second upload of the same files left the same five paths (checked 2026-11-22 from a laptop, not yet from Actions, which #14 covers).
- Not yet confirmed: Auto Loader on serverless, Databricks Asset Bundles, and dbt against the Free Edition warehouse. The first M1 stories check these. If any is missing, this ADR gets updated.
