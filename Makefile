.PHONY: lint test dbt-build

DBT = uv run --group dbt dbt
DBT_DIRS = --project-dir dbt --profiles-dir dbt

lint:
	uv run ruff check .
	uv run ruff format --check .

test:
	uv run pytest

# Needs the Databricks variables from .env in the environment.
dbt-build:
	$(DBT) source freshness $(DBT_DIRS)
	$(DBT) build $(DBT_DIRS)
