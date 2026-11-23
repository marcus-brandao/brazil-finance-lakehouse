# Bronze job: Auto Loader from the landing Volume into bronze.sgs_raw.
# Runs as a serverless Python script task, see create_bronze_job.py.

import argparse
import os

FALLBACK_CATALOG = "workspace"
TABLE = "{catalog}.bronze.sgs_raw"
SOURCE = "/Volumes/{catalog}/bronze/landing/sgs/"
CHECKPOINT = "/Volumes/{catalog}/bronze/landing/_checkpoints/bronze_sgs_raw"

# Same keys as the envelope that ingestion/sgs.py write_window() writes. Everything
# stays a string except the series code, typing happens in silver.
ENVELOPE_COLUMNS = [
    ("source", "STRING", "Data source name, always bcb_sgs."),
    ("series_code", "BIGINT", "BACEN SGS series code."),
    ("window_start", "STRING", "First day of the fetched window (yyyy-mm-dd)."),
    ("window_end", "STRING", "Last day of the fetched window (yyyy-mm-dd)."),
    ("ingested_at", "STRING", "UTC time the connector fetched the window (ISO 8601)."),
    ("run_id", "STRING", "Connector run that wrote the file."),
    (
        "payload",
        "STRING",
        "Raw JSON array exactly as BACEN returned it, values still strings.",
    ),
]
ADDED_COLUMNS = [
    ("source_file_path", "STRING", "Landing file this row was read from."),
    (
        "source_file_modification_time",
        "TIMESTAMP",
        "Last modified time of the landing file when Auto Loader read it.",
    ),
    (
        "_rescued_data",
        "STRING",
        "Fields that did not match the envelope schema, as JSON.",
    ),
]
COLUMNS = ENVELOPE_COLUMNS + ADDED_COLUMNS

TABLE_COMMENT = (
    "Raw BACEN SGS landing files, one row per file read. Append only: a file that "
    "is rewritten at the same path adds a new row, silver keeps the latest."
)

# allowOverwrites makes Auto Loader read a file again when its modification time
# changes. Without it a rewritten year file would be skipped. See ADR-0003.
AUTO_LOADER_OPTIONS = {
    "cloudFiles.format": "json",
    "cloudFiles.allowOverwrites": "true",
    "rescuedDataColumn": "_rescued_data",
}


def default_catalog() -> str:
    return os.environ.get("DATABRICKS_CATALOG", FALLBACK_CATALOG)


def envelope_schema() -> str:
    return ", ".join(f"{name} {kind}" for name, kind, _ in ENVELOPE_COLUMNS)


def create_table_sql(catalog: str) -> str:
    columns = ",\n  ".join(
        f"{name} {kind} COMMENT '{comment}'" for name, kind, comment in COLUMNS
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {TABLE.format(catalog=catalog)} (\n  {columns}\n)\n"
        f"COMMENT '{TABLE_COMMENT}'"
    )


def select_expressions() -> list[str]:
    # Explicit order, so the stream writes to the table columns it expects.
    names = [name for name, _, _ in ENVELOPE_COLUMNS]
    return names + [
        "_metadata.file_path AS source_file_path",
        "_metadata.file_modification_time AS source_file_modification_time",
        "_rescued_data",
    ]


def run(spark, catalog: str) -> None:
    spark.sql(create_table_sql(catalog))
    stream = (
        spark.readStream.format("cloudFiles")
        .options(**AUTO_LOADER_OPTIONS)
        .schema(envelope_schema())
        .load(SOURCE.format(catalog=catalog))
        .selectExpr(*select_expressions())
    )
    # Serverless only runs availableNow. The default trigger fails there.
    query = (
        stream.writeStream.option(
            "checkpointLocation", CHECKPOINT.format(catalog=catalog)
        )
        .trigger(availableNow=True)
        .toTable(TABLE.format(catalog=catalog))
    )
    query.awaitTermination()


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=default_catalog())
    run(SparkSession.builder.getOrCreate(), parser.parse_args().catalog)
