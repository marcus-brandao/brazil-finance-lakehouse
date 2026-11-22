# brazil-finance-lakehouse

Batch + streaming lakehouse on Brazilian public financial data (BACEN, CVM), built on Databricks Free Edition. A public portfolio project: the goal is to show modern data engineering practice, and the process (issues, ADRs, CI) matters as much as the code.

## Architecture

Planned design; see "What works today" for what exists.

```
BACEN SGS / CVM  ->  ingestion (GitHub Actions)  ->  landing Volume  ->  bronze  ->  silver  ->  gold  ->  dashboards
                     Python connectors, upload       raw files         Auto Loader   dbt        dbt      Evidence, AI/BI
```

- **Ingestion runs outside Databricks.** Free Edition has restricted egress, so Python connectors run on GitHub Actions and upload raw files to a Unity Catalog Volume. Connectors are idempotent: the same window always produces the same output path.
- **Bronze** is append-only. Auto Loader picks up new files and keeps the raw payload untouched in an envelope.
- **Silver and gold** are dbt models. Dedup and typing happen in silver.
- **Streaming** and **PII / identity resolution** (on synthetic, seeded data) come in later milestones.

A proper architecture diagram is tracked in #17.

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python 3.12, `uv` |
| Platform | Databricks Free Edition: Unity Catalog, Delta, Auto Loader |
| Transform | dbt |
| BI | Evidence, Databricks AI/BI dashboards |
| CI/CD | GitHub Actions |
| Quality | ruff, pytest, pre-commit |

## What works today

Repo scaffolding only: `uv` project, `ruff` lint and format, `pytest`, pre-commit hooks and a `Makefile`. No data pipeline yet; the first source (BACEN SGS) is milestone M1.

## Development

```bash
uv sync
uv run pre-commit install
make lint test
```

## Data sources

Data comes from open sources and is not stored in this repo.

- [BACEN SGS](https://dadosabertos.bcb.gov.br/) (Banco Central do Brasil, time-series system, open data portal)
- [CVM open data](https://dados.cvm.gov.br/) (Comissão de Valores Mobiliários)

Both are published under the Open Database License (ODbL); attribution is kept here.

## License

MIT
