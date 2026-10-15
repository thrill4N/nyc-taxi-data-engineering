# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

import json
import os
import tempfile
import uuid

import requests
from pyspark.sql import functions as F


def _fs_path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def download_source():
    if _fs_path_exists(RAW_PATH):
        print(f"Source file already exists; reusing {RAW_PATH}")
        return

    url = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{YEAR_MONTH}.parquet"
    )
    print(f"Downloading {url}")
    local_path = None
    download_size = 0
    staged_path = f"{RAW_PATH}.{uuid.uuid4().hex}.tmp"
    try:
        with requests.get(url, stream=True, timeout=(20, 120)) as response:
            response.raise_for_status()
            with tempfile.NamedTemporaryFile(
                prefix="nyc_taxi_", suffix=".parquet", delete=False
            ) as tmp_file:
                local_path = tmp_file.name
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        tmp_file.write(chunk)

        if os.path.getsize(local_path) == 0:
            raise IOError(f"Downloaded an empty Parquet file from {url}")
        download_size = os.path.getsize(local_path)

        dbutils.fs.mkdirs(f"{VOLUME_PATH}/raw")
        with open(local_path, "rb") as src, open(staged_path, "wb") as dst:
            dst.write(src.read())
        if not dbutils.fs.mv(staged_path, RAW_PATH):
            raise IOError(f"Could not move downloaded data into {RAW_PATH}")
    except Exception:
        if _fs_path_exists(staged_path):
            dbutils.fs.rm(staged_path)
        raise
    finally:
        if local_path and os.path.exists(local_path):
            os.remove(local_path)

    print(f"Downloaded {download_size:,} bytes -> {RAW_PATH}")


download_source()

source_df = spark.read.parquet(RAW_PATH)
if SAMPLE_MODE:
    source_df = source_df.limit(SAMPLE_LIMIT)

source_columns = source_df.columns
source_df = source_df.withColumn("year_month", F.lit(YEAR_MONTH))
source_row_count = source_df.count()
if source_row_count == 0:
    raise ValueError(f"No taxi records were loaded for {YEAR_MONTH}")

print(f"Bronze input: {source_row_count:,} rows, {len(source_df.columns)} columns")
source_df.printSchema()

null_aliases = {
    column: f"_null_{index}"
    for index, column in enumerate(source_columns)
}
aggregations = [F.count(F.lit(1)).alias("_row_count")]
aggregations.extend(
    F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(alias)
    for column, alias in null_aliases.items()
)
aggregations.extend(
    [
        F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias("_negative_fares"),
        F.sum(F.when(F.col("passenger_count") == 0, 1).otherwise(0)).alias("_zero_passengers"),
        F.sum(
            F.when(
                (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0),
                1,
            ).otherwise(0)
        ).alias("_zero_distance_nonzero_fare"),
    ]
)
metrics = source_df.agg(*aggregations).first().asDict()

quality_report = {
    "year_month": YEAR_MONTH,
    "row_count": int(metrics["_row_count"]),
    "nulls": {
        column: int(metrics[alias] or 0)
        for column, alias in null_aliases.items()
    },
    "negative_fares": int(metrics["_negative_fares"] or 0),
    "zero_passengers": int(metrics["_zero_passengers"] or 0),
    "zero_distance_nonzero_fare": int(
        metrics["_zero_distance_nonzero_fare"] or 0
    ),
}

write_month_partition(source_df, table_name("bronze"))
quality_df = spark.createDataFrame(
    [(YEAR_MONTH, json.dumps(quality_report, sort_keys=True))],
    ["year_month", "report_json"],
).withColumn("recorded_at", F.current_timestamp())
write_month_partition(quality_df, table_name("quality"))

bronze_count = (
    spark.table(table_name("bronze"))
    .where(F.col("year_month") == YEAR_MONTH)
    .count()
)
if bronze_count != source_row_count:
    raise AssertionError(
        f"Bronze write changed row count: input={source_row_count}, "
        f"stored={bronze_count}"
    )

print(f"Bronze Delta table: {table_name('bronze')}")
print(f"Bronze quality report: {quality_report}")
print(f"Verified bronze row count: {bronze_count:,}")