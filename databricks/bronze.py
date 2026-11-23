# Bronze job: Auto Loader from the landing Volume into bronze.<source>_raw, for the
# source named by --source. Runs as a serverless Python script task, see
# create_bronze_job.py.

import argparse
import os

FALLBACK_CATALOG = "workspace"
TABLE = "{catalog}.bronze.{source}_raw"
LANDING = "/Volumes/{catalog}/bronze/landing/{source}/"
CHECKPOINT = "/Volumes/{catalog}/bronze/landing/_checkpoints/bronze_{source}_raw"

# The envelope keys that ingestion/sgs.py and ingestion/ptax.py write_window()
# write. For CVM the "envelope" is the CSV header, see ADR-0008. Everything stays a string except the series code, typing happens in silver.
COMMON_ENVELOPE = [
    ("window_start", "STRING", "First day of the fetched window (yyyy-mm-dd)."),
    ("window_end", "STRING", "Last day of the fetched window (yyyy-mm-dd)."),
    ("ingested_at", "STRING", "UTC time the connector fetched the window (ISO 8601)."),
    ("run_id", "STRING", "Connector run that wrote the file."),
]
SOURCES = {
    "sgs": {
        "comment": (
            "Raw BACEN SGS landing files, one row per file read. Append only: a "
            "file that is rewritten at the same path adds a new row, silver keeps "
            "the latest."
        ),
        "envelope": [
            ("source", "STRING", "Data source name, always bcb_sgs."),
            ("series_code", "BIGINT", "BACEN SGS series code."),
            *COMMON_ENVELOPE,
            (
                "payload",
                "STRING",
                "Raw JSON array exactly as BACEN returned it, values still strings.",
            ),
        ],
    },
    "ptax": {
        "comment": (
            "Raw BACEN PTAX (USD closing rate) landing files, one row per file "
            "read. Append only: a file that is rewritten at the same path adds a "
            "new row, silver keeps the latest."
        ),
        "envelope": [
            ("source", "STRING", "Data source name, always bcb_ptax."),
            *COMMON_ENVELOPE,
            (
                "payload",
                "STRING",
                (
                    "Raw JSON array exactly as BACEN returned it, one object per "
                    "closing bulletin, rates as JSON numbers."
                ),
            ),
        ],
    },
    "cvm_inf_diario": {
        "comment": (
            "Raw CVM daily fund report (informe diario) CSV files, one row per CSV "
            "line, columns matched by header name. Each layout fills its own columns "
            "and leaves the others null. Append only: a month that is rewritten at "
            "the same path adds its rows again, silver keeps the latest."
        ),
        "reader": {
            "cloudFiles.format": "csv",
            "header": "true",
            "sep": ";",
            # Every file sampled was plain ASCII. CVM publishes its other CSVs in
            # Latin-1, so a non-ASCII byte would be read the same way.
            "encoding": "ISO-8859-1",
        },
        "envelope": [
            ("TP_FUNDO", "STRING", "Fund type, layouts up to 2023-11."),
            ("CNPJ_FUNDO", "STRING", "Fund CNPJ, layouts up to 2023-11."),
            ("TP_FUNDO_CLASSE", "STRING", "Fund or class type, from 2023-12."),
            ("CNPJ_FUNDO_CLASSE", "STRING", "Fund or class CNPJ, from 2023-12."),
            ("ID_SUBCLASSE", "STRING", "Subclass id, from 2023-12, often empty."),
            ("DT_COMPTC", "STRING", "Report date (yyyy-mm-dd)."),
            ("VL_TOTAL", "STRING", "Total portfolio value."),
            ("VL_QUOTA", "STRING", "Share value."),
            ("VL_PATRIM_LIQ", "STRING", "Net asset value."),
            ("CAPTC_DIA", "STRING", "Inflows of the day."),
            ("RESG_DIA", "STRING", "Redemptions of the day."),
            ("NR_COTST", "STRING", "Number of shareholders."),
        ],
    },
}
JSON_READER = {"cloudFiles.format": "json"}
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

# allowOverwrites makes Auto Loader read a file again when its modification time
# changes. Without it a rewritten year file would be skipped. See ADR-0003.
AUTO_LOADER_OPTIONS = {
    "cloudFiles.allowOverwrites": "true",
    "rescuedDataColumn": "_rescued_data",
}


def default_catalog() -> str:
    return os.environ.get("DATABRICKS_CATALOG", FALLBACK_CATALOG)


def columns(source: str) -> list[tuple[str, str, str]]:
    return SOURCES[source]["envelope"] + ADDED_COLUMNS


def reader_options(source: str) -> dict[str, str]:
    return {**SOURCES[source].get("reader", JSON_READER), **AUTO_LOADER_OPTIONS}


def envelope_schema(source: str) -> str:
    return ", ".join(f"{name} {kind}" for name, kind, _ in SOURCES[source]["envelope"])


def create_table_sql(catalog: str, source: str) -> str:
    column_sql = ",\n  ".join(
        f"{name} {kind} COMMENT '{comment}'" for name, kind, comment in columns(source)
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {TABLE.format(catalog=catalog, source=source)} "
        f"(\n  {column_sql}\n)\n"
        f"COMMENT '{SOURCES[source]['comment']}'"
    )


def select_expressions(source: str) -> list[str]:
    # Explicit order, so the stream writes to the table columns it expects.
    names = [name for name, _, _ in SOURCES[source]["envelope"]]
    return names + [
        "_metadata.file_path AS source_file_path",
        "_metadata.file_modification_time AS source_file_modification_time",
        "_rescued_data",
    ]


def run(spark, catalog: str, source: str) -> None:
    spark.sql(create_table_sql(catalog, source))
    stream = (
        spark.readStream.format("cloudFiles")
        .options(**reader_options(source))
        .schema(envelope_schema(source))
        .load(LANDING.format(catalog=catalog, source=source))
        .selectExpr(*select_expressions(source))
    )
    # Serverless only runs availableNow. The default trigger fails there.
    query = (
        stream.writeStream.option(
            "checkpointLocation", CHECKPOINT.format(catalog=catalog, source=source)
        )
        .trigger(availableNow=True)
        .toTable(TABLE.format(catalog=catalog, source=source))
    )
    query.awaitTermination()


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=default_catalog())
    parser.add_argument("--source", choices=sorted(SOURCES), required=True)
    args = parser.parse_args()
    run(SparkSession.builder.getOrCreate(), args.catalog, args.source)
