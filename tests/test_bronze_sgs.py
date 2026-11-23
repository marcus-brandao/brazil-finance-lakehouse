import json
from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import bronze_sgs as bronze

from ingestion import sgs


def written_envelope(tmp_path) -> dict:
    sgs.write_window(
        tmp_path,
        432,
        (date(2025, 1, 1), date(2025, 12, 31)),
        [{"data": "01/01/2025", "valor": "0.05"}],
        "run-1",
        datetime(2026, 1, 1, tzinfo=UTC),
    )
    return json.loads((tmp_path / "sgs/series_code=432/year=2025.json").read_text())


def test_envelope_columns_match_what_the_connector_writes(tmp_path):
    names = [name for name, _, _ in bronze.ENVELOPE_COLUMNS]
    assert names == list(written_envelope(tmp_path))


def test_every_column_has_a_comment():
    assert all(comment for _, _, comment in bronze.COLUMNS)
    sql = bronze.create_table_sql("workspace")
    assert sql.count("COMMENT '") == len(bronze.COLUMNS) + 1


def test_select_matches_table_column_order():
    selected = [e.split(" AS ")[-1] for e in bronze.select_expressions()]
    assert selected == [name for name, _, _ in bronze.COLUMNS]


def test_catalog_comes_from_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CATALOG", "lakehouse")
    assert bronze.default_catalog() == "lakehouse"
    monkeypatch.delenv("DATABRICKS_CATALOG")
    assert bronze.default_catalog() == "workspace"


def test_run_uses_overwrites_available_now_and_volume_paths():
    spark = MagicMock()
    bronze.run(spark, "lakehouse")

    spark.sql.assert_called_once()
    assert "lakehouse.bronze.sgs_raw" in spark.sql.call_args.args[0]
    reader = spark.readStream.format.return_value
    reader.options.assert_called_once_with(**bronze.AUTO_LOADER_OPTIONS)
    assert bronze.AUTO_LOADER_OPTIONS["cloudFiles.allowOverwrites"] == "true"
    load = reader.options.return_value.schema.return_value.load
    load.assert_called_once_with("/Volumes/lakehouse/bronze/landing/sgs/")

    writer = load.return_value.selectExpr.return_value.writeStream
    writer.option.assert_called_once_with(
        "checkpointLocation",
        "/Volumes/lakehouse/bronze/landing/_checkpoints/bronze_sgs_raw",
    )
    trigger = writer.option.return_value.trigger
    trigger.assert_called_once_with(availableNow=True)
    trigger.return_value.toTable.assert_called_once_with("lakehouse.bronze.sgs_raw")
