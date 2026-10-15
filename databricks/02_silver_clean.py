# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %pip install pandera
# MAGIC
# MAGIC import json
# MAGIC from datetime import datetime
# MAGIC
# MAGIC import pandera.pyspark as pa
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType, TimestampType
# MAGIC
# MAGIC
# MAGIC taxi_schema = pa.DataFrameSchema(
# MAGIC     {
# MAGIC         "VendorID": pa.Column(IntegerType(), nullable=False),
# MAGIC         "tpep_pickup_datetime": pa.Column(TimestampType(), nullable=False),
# MAGIC         "tpep_dropoff_datetime": pa.Column(TimestampType(), nullable=False),
# MAGIC         "passenger_count": pa.Column(
# MAGIC             DoubleType(),
# MAGIC             pa.Check.ge(0, ignore_na=True),
# MAGIC             nullable=True,
# MAGIC         ),
# MAGIC         "trip_distance": pa.Column(
# MAGIC             DoubleType(),
# MAGIC             pa.Check.ge(0),
# MAGIC             nullable=False,
# MAGIC         ),
# MAGIC         "fare_amount": pa.Column(
# MAGIC             DoubleType(),
# MAGIC             pa.Check.in_range(-1000, 5000),
# MAGIC             nullable=False,
# MAGIC         ),
# MAGIC         "total_amount": pa.Column(DoubleType(), nullable=False),
# MAGIC         "PULocationID": pa.Column(
# MAGIC             IntegerType(),
# MAGIC             pa.Check.ge(1),
# MAGIC             nullable=False,
# MAGIC         ),
# MAGIC         "DOLocationID": pa.Column(
# MAGIC             IntegerType(),
# MAGIC             pa.Check.ge(1),
# MAGIC             nullable=False,
# MAGIC         ),
# MAGIC     },
# MAGIC     strict=False,
# MAGIC     coerce=False,
# MAGIC )
# MAGIC
# MAGIC
# MAGIC def smoke_test_pandera_spark():
# MAGIC     """Fail early if the installed Pandera PySpark backend is incompatible."""
# MAGIC     sample_schema = StructType(
# MAGIC         [
# MAGIC             StructField("VendorID", IntegerType(), False),
# MAGIC             StructField("tpep_pickup_datetime", TimestampType(), False),
# MAGIC             StructField("tpep_dropoff_datetime", TimestampType(), False),
# MAGIC             StructField("passenger_count", DoubleType(), True),
# MAGIC             StructField("trip_distance", DoubleType(), False),
# MAGIC             StructField("fare_amount", DoubleType(), False),
# MAGIC             StructField("total_amount", DoubleType(), False),
# MAGIC             StructField("PULocationID", IntegerType(), False),
# MAGIC             StructField("DOLocationID", IntegerType(), False),
# MAGIC         ]
# MAGIC     )
# MAGIC     valid_sample = spark.createDataFrame(
# MAGIC         [
# MAGIC             (
# MAGIC                 1,
# MAGIC                 datetime(2024, 1, 1, 8, 0),
# MAGIC                 datetime(2024, 1, 1, 8, 15),
# MAGIC                 1.0,
# MAGIC                 1.2,
# MAGIC                 10.0,
# MAGIC                 12.0,
# MAGIC                 100,
# MAGIC                 200,
# MAGIC             ),
# MAGIC             (
# MAGIC                 2,
# MAGIC                 datetime(2024, 1, 1, 9, 0),
# MAGIC                 datetime(2024, 1, 1, 9, 15),
# MAGIC                 None,
# MAGIC                 2.0,
# MAGIC                 15.0,
# MAGIC                 17.0,
# MAGIC                 101,
# MAGIC                 201,
# MAGIC             ),
# MAGIC         ],
# MAGIC         sample_schema,
# MAGIC     )
# MAGIC     taxi_schema.validate(valid_sample, lazy=True)
# MAGIC
# MAGIC     invalid_sample = valid_sample.withColumn(
# MAGIC         "trip_distance",
# MAGIC         F.when(F.col("VendorID") == 1, F.lit(-1.0)).otherwise(F.col("trip_distance")),
# MAGIC     )
# MAGIC     invalid_result = taxi_schema.validate(invalid_sample, lazy=True)
# MAGIC     if not invalid_result.pandera.errors:
# MAGIC         raise RuntimeError(
# MAGIC             "Pandera PySpark did not reject the intentionally invalid "
# MAGIC             "smoke-test row; verify the Databricks Runtime and Pandera version."
# MAGIC         )
# MAGIC
# MAGIC
# MAGIC def validation_report(frame):
# MAGIC     validated = taxi_schema.validate(frame, lazy=True)
# MAGIC     error_handler = validated.pandera.errors
# MAGIC     if not error_handler:
# MAGIC         return {
# MAGIC             "passed": True,
# MAGIC             "failure_count": 0,
# MAGIC             "failures": [],
# MAGIC         }
# MAGIC
# MAGIC     grouped_failures = {}
# MAGIC     for category in error_handler.values():
# MAGIC         for error_list in category.values():
# MAGIC             for error in error_list:
# MAGIC                 column = str(error["column"])
# MAGIC                 check = str(error["check"])
# MAGIC                 key = (column, check)
# MAGIC                 grouped_failures[key] = grouped_failures.get(key, 0) + 1
# MAGIC
# MAGIC     failures = [
# MAGIC         {
# MAGIC             "column": column,
# MAGIC             "check": check,
# MAGIC             "violation_count": count,
# MAGIC         }
# MAGIC         for (column, check), count in sorted(grouped_failures.items())
# MAGIC     ]
# MAGIC     return {
# MAGIC         "passed": not failures,
# MAGIC         "failure_count": sum(item["violation_count"] for item in failures),
# MAGIC         "failures": failures,
# MAGIC     }
# MAGIC
# MAGIC
# MAGIC smoke_test_pandera_spark()
# MAGIC print("Pandera PySpark compatibility smoke test passed.")
# MAGIC
# MAGIC bronze_df = (
# MAGIC     spark.table(table_name("bronze"))
# MAGIC     .where(F.col("year_month") == YEAR_MONTH)
# MAGIC )
# MAGIC bronze_count = bronze_df.count()
# MAGIC
# MAGIC # Validate the source contract before flags and dtype adjustments, matching
# MAGIC # the local pipeline's order and keeping validation independent of the flags.
# MAGIC report = validation_report(bronze_df)
# MAGIC report_df = spark.createDataFrame(
# MAGIC     [(YEAR_MONTH, json.dumps(report, sort_keys=True))],
# MAGIC     ["year_month", "report_json"],
# MAGIC ).withColumn("validated_at", F.current_timestamp())
# MAGIC write_month_partition(report_df, table_name("validation"))
# MAGIC if report["passed"]:
# MAGIC     print("Schema validation: PASSED")
# MAGIC else:
# MAGIC     print(
# MAGIC         f"Schema validation: {report['failure_count']} violations found; "
# MAGIC         "rows are preserved. See the silver validation Delta table."
# MAGIC     )
# MAGIC
# MAGIC cleaned_df = (
# MAGIC     bronze_df
# MAGIC     .withColumn(
# MAGIC         "flag_missing_optional_fields",
# MAGIC         F.col("passenger_count").isNull(),
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "flag_negative_fare",
# MAGIC         F.coalesce(F.col("fare_amount") < 0, F.lit(False)),
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "flag_zero_passenger",
# MAGIC         F.coalesce(F.col("passenger_count") == 0, F.lit(False)),
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "flag_zero_distance_nonzero_fare",
# MAGIC         F.coalesce(
# MAGIC             (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0),
# MAGIC             F.lit(False),
# MAGIC         ),
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "trip_duration_min",
# MAGIC         (
# MAGIC             F.unix_timestamp(F.col("tpep_dropoff_datetime"))
# MAGIC             - F.unix_timestamp(F.col("tpep_pickup_datetime"))
# MAGIC         )
# MAGIC         / F.lit(60.0),
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "flag_negative_or_zero_duration",
# MAGIC         F.coalesce(F.col("trip_duration_min") <= 0, F.lit(False)),
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC if "store_and_fwd_flag" in cleaned_df.columns:
# MAGIC     cleaned_df = cleaned_df.withColumn(
# MAGIC         "store_and_fwd_flag",
# MAGIC         F.coalesce(F.col("store_and_fwd_flag"), F.lit("N/A")),
# MAGIC     )
# MAGIC if "RatecodeID" in cleaned_df.columns:
# MAGIC     cleaned_df = cleaned_df.withColumn(
# MAGIC         "RatecodeID",
# MAGIC         F.col("RatecodeID").cast("int"),
# MAGIC     )
# MAGIC
# MAGIC write_month_partition(cleaned_df, table_name("silver"))
# MAGIC silver_count = (
# MAGIC     spark.table(table_name("silver"))
# MAGIC     .where(F.col("year_month") == YEAR_MONTH)
# MAGIC     .count()
# MAGIC )
# MAGIC if silver_count != bronze_count:
# MAGIC     raise AssertionError(
# MAGIC         f"Silver row count changed: bronze={bronze_count}, silver={silver_count}"
# MAGIC     )
# MAGIC
# MAGIC flag_columns = [
# MAGIC     column for column in cleaned_df.columns if column.startswith("flag_")
# MAGIC ]
# MAGIC flag_counts = cleaned_df.agg(
# MAGIC     *[F.sum(F.col(column).cast("long")).alias(column) for column in flag_columns]
# MAGIC ).first().asDict()
# MAGIC print(f"Silver Delta table: {table_name('silver')}")
# MAGIC print(f"Silver row count unchanged: {silver_count:,}")
# MAGIC print(f"Flag totals: {flag_counts}")