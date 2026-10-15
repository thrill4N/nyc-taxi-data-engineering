# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import re


if "year_month" not in dbutils.widgets.getAll():
    dbutils.widgets.text("year_month", "2024-01", "TLC data month (YYYY-MM)")
if "sample_mode" not in dbutils.widgets.getAll():
    dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "Use first 5,000 rows")

YEAR_MONTH = dbutils.widgets.get("year_month")
SAMPLE_MODE_VALUE = dbutils.widgets.get("sample_mode").lower()

if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", YEAR_MONTH):
    raise ValueError(f"year_month must use YYYY-MM format; received {YEAR_MONTH!r}")
if SAMPLE_MODE_VALUE not in {"true", "false"}:
    raise ValueError("sample_mode must be 'true' or 'false'")

SAMPLE_MODE = SAMPLE_MODE_VALUE == "true"
SAMPLE_LIMIT = 5000

# Change these names if your Free Edition workspace uses a different catalog.
CATALOG = "nyc_taxi"
SCHEMA = "pipeline"
VOLUME = "taxi_data"

for identifier in (CATALOG, SCHEMA, VOLUME):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid Unity Catalog identifier: {identifier!r}")

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`"
)

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_PATH = f"{VOLUME_PATH}/raw/yellow_tripdata_{YEAR_MONTH}.parquet"

TABLES = {
    "bronze": "bronze_yellow_tripdata",
    "quality": "bronze_quality_metrics",
    "silver": "silver_yellow_tripdata",
    "validation": "silver_schema_validation",
    "hourly": "gold_hourly_summary",
    "dow": "gold_day_of_week_summary",
    "routes": "gold_route_summary",
}


def table_name(key):
    """Return the configured table name, isolating sample runs from full runs."""
    suffix = "_sample" if SAMPLE_MODE else ""
    return f"`{CATALOG}`.`{SCHEMA}`.`{TABLES[key]}{suffix}`"


def write_month_partition(frame, destination):
    """Atomically replace one month so reruns are idempotent."""
    if "year_month" not in frame.columns:
        raise ValueError(f"{destination} must include a year_month column")

    try:
        spark.sql(f"DESCRIBE TABLE {destination}")
        _table_exists = True
    except Exception:
        _table_exists = False

    if not _table_exists:
        (
            frame.limit(0)
            .write.format("delta")
            .partitionBy("year_month")
            .saveAsTable(destination)
        )

    predicate = f"year_month = '{YEAR_MONTH}'"
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(destination)
    )


print(
    f"Databricks setup ready: month={YEAR_MONTH}, sample_mode={SAMPLE_MODE}, "
    f"catalog={CATALOG}.{SCHEMA}, volume={VOLUME_PATH}"
)