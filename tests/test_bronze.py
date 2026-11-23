import json
from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import bronze
import pytest

from ingestion import ptax, sgs

NOW = datetime(2026, 1, 1, tzinfo=UTC)
WINDOW = (date(2025, 1, 1), date(2025, 12, 31))


def written_envelope(tmp_path, source: str) -> dict:
    if source == "sgs":
        sgs.write_window(
            tmp_path, 432, WINDOW, [{"data": "01/01/2025", "valor": "0.05"}], "r", NOW
        )
        return json.loads((tmp_path / "sgs/series_code=432/year=2025.json").read_text())
    ptax.write_window(tmp_path, WINDOW, [{"cotacaoCompra": 6.2}], "r", NOW)
    return json.loads((tmp_path / "ptax/year=2025.json").read_text())


@pytest.mark.parametrize("source", ["sgs", "ptax"])
def test_envelope_columns_match_what_the_connector_writes(tmp_path, source):
    names = [name for name, _, _ in bronze.SOURCES[source]["envelope"]]
    assert names == list(written_envelope(tmp_path, source))


@pytest.mark.parametrize("source", ["sgs", "ptax"])
def test_every_column_has_a_comment(source):
    assert all(comment for _, _, comment in bronze.columns(source))
    sql = bronze.create_table_sql("workspace", source)
    assert sql.count("COMMENT '") == len(bronze.columns(source)) + 1
    assert f"workspace.bronze.{source}_raw" in sql


@pytest.mark.parametrize("source", ["sgs", "ptax"])
def test_select_matches_table_column_order(source):
    selected = [e.split(" AS ")[-1] for e in bronze.select_expressions(source)]
    assert selected == [name for name, _, _ in bronze.columns(source)]


def test_ptax_has_no_series_code_and_sgs_keeps_its_columns():
    ptax_names = [name for name, _, _ in bronze.columns("ptax")]
    sgs_names = [name for name, _, _ in bronze.columns("sgs")]
    assert "series_code" not in ptax_names
    assert sgs_names[:3] == ["source", "series_code", "window_start"]


def test_catalog_comes_from_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CATALOG", "lakehouse")
    assert bronze.default_catalog() == "lakehouse"
    monkeypatch.delenv("DATABRICKS_CATALOG")
    assert bronze.default_catalog() == "workspace"


@pytest.mark.parametrize("source", ["sgs", "ptax"])
def test_run_uses_overwrites_available_now_and_volume_paths(source):
    spark = MagicMock()
    bronze.run(spark, "lakehouse", source)

    spark.sql.assert_called_once()
    assert f"lakehouse.bronze.{source}_raw" in spark.sql.call_args.args[0]
    reader = spark.readStream.format.return_value
    reader.options.assert_called_once_with(**bronze.AUTO_LOADER_OPTIONS)
    assert bronze.AUTO_LOADER_OPTIONS["cloudFiles.allowOverwrites"] == "true"
    load = reader.options.return_value.schema.return_value.load
    load.assert_called_once_with(f"/Volumes/lakehouse/bronze/landing/{source}/")

    writer = load.return_value.selectExpr.return_value.writeStream
    # The sgs checkpoint path is unchanged from when the script was bronze_sgs.py.
    writer.option.assert_called_once_with(
        "checkpointLocation",
        f"/Volumes/lakehouse/bronze/landing/_checkpoints/bronze_{source}_raw",
    )
    trigger = writer.option.return_value.trigger
    trigger.assert_called_once_with(availableNow=True)
    trigger.return_value.toTable.assert_called_once_with(
        f"lakehouse.bronze.{source}_raw"
    )
