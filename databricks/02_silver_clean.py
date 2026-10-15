# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %pip install pandera  # install pandera for PySpark schema validation
# MAGIC
# MAGIC import json
# MAGIC from datetime import datetime
# MAGIC
# MAGIC import pandera.pyspark as pa  # Pandera PySpark backend for DataFrame schema validation
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType, TimestampType
# MAGIC
# MAGIC
# MAGIC # Define the expected schema contract for the bronze taxi data.
# MAGIC # strict=False: allow extra bronze columns to pass through untouched.
# MAGIC # coerce=False: do not attempt automatic type conversion.
# MAGIC taxi_schema = pa.DataFrameSchema(
# MAGIC     {
# MAGIC         "VendorID": pa.Column(IntegerType(), nullable=False),
# MAGIC         "tpep_pickup_datetime": pa.Column(TimestampType(), nullable=False),
# MAGIC         "tpep_dropoff_datetime": pa.Column(TimestampType(), nullable=False),
# MAGIC         "passenger_count": pa.Column(  # nullable: some trips have missing passenger counts
# MAGIC             DoubleType(),
# MAGIC             pa.Check.ge(0, ignore_na=True),  # non-negative; skip nulls during check
# MAGIC             nullable=True,
# MAGIC         ),
# MAGIC         "trip_distance": pa.Column(
# MAGIC             DoubleType(),
# MAGIC             pa.Check.ge(0),  # distance must be non-negative
# MAGIC             nullable=False,
# MAGIC         ),
# MAGIC         "fare_amount": pa.Column(
# MAGIC             DoubleType(),
# MAGIC             pa.Check.in_range(-1000, 5000),  # allow negatives (refunds) but cap extremes
# MAGIC             nullable=False,
# MAGIC         ),
# MAGIC         "total_amount": pa.Column(DoubleType(), nullable=False),
# MAGIC         "PULocationID": pa.Column(
# MAGIC             IntegerType(),
# MAGIC             pa.Check.ge(1),  # location IDs start at 1
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
# MAGIC     # build a minimal schema matching the validation contract for the test
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
# MAGIC     # create two valid rows: one with a passenger, one with null passenger_count
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
# MAGIC     # verify the schema accepts valid data without error
# MAGIC     taxi_schema.validate(valid_sample, lazy=True)
# MAGIC
# MAGIC     # inject an invalid trip_distance (-1.0) to confirm Pandera catches violations
# MAGIC     invalid_sample = valid_sample.withColumns(
# MAGIC         {"trip_distance": F.when(F.col("VendorID") == 1, F.lit(-1.0)).otherwise(F.col("trip_distance"))}
# MAGIC     )
# MAGIC     invalid_result = taxi_schema.validate(invalid_sample, lazy=True)
# MAGIC     if not invalid_result.pandera.errors:  # pandera should report errors for the invalid row
# MAGIC         raise RuntimeError(
# MAGIC             "Pandera PySpark did not reject the intentionally invalid "
# MAGIC             "smoke-test row; verify the Databricks Runtime and Pandera version."
# MAGIC         )
# MAGIC
# MAGIC
# MAGIC def validation_report(frame):
# MAGIC     """Run lazy schema validation and return a structured JSON-ready report."""
# MAGIC     validated = taxi_schema.validate(frame, lazy=True)  # collect all errors, don't fail fast
# MAGIC     error_handler = validated.pandera.errors  # nested defaultdict: category -> check -> error list
# MAGIC     if not error_handler:  # no errors means validation passed
# MAGIC         return {
# MAGIC             "passed": True,
# MAGIC             "failure_count": 0,
# MAGIC             "failures": [],
# MAGIC         }
# MAGIC
# MAGIC     # group errors by (column, check) pair and count violations
# MAGIC     grouped_failures = {}
# MAGIC     for category in error_handler.values():  # iterate over error categories
# MAGIC         for error_list in category.values():  # iterate over check groups
# MAGIC             for error in error_list:  # iterate over individual errors
# MAGIC                 column = str(error["column"])
# MAGIC                 check = str(error["check"])
# MAGIC                 key = (column, check)
# MAGIC                 grouped_failures[key] = grouped_failures.get(key, 0) + 1
# MAGIC
# MAGIC     # build a sorted list of failure summaries
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
# MAGIC # run the compatibility smoke test before touching real data
# MAGIC smoke_test_pandera_spark()
# MAGIC print("Pandera PySpark compatibility smoke test passed.")
# MAGIC
# MAGIC # load the bronze table for the target month
# MAGIC bronze_df = (
# MAGIC     spark.table(table_name("bronze"))
# MAGIC     .where(F.col("year_month") == YEAR_MONTH)
# MAGIC )
# MAGIC bronze_count = bronze_df.count()  # capture count for post-write verification
# MAGIC
# MAGIC # Validate the source contract before flags and dtype adjustments, matching
# MAGIC # the local pipeline's order and keeping validation independent of the flags.
# MAGIC report = validation_report(bronze_df)
# MAGIC # persist the validation report as a single-row JSON record with a timestamp
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
# MAGIC # Add quality flag columns and derive trip_duration_min in a single withColumns
# MAGIC # call to avoid building a deeply nested execution plan (SCPAP004 lint fix).
# MAGIC # F.coalesce(..., F.lit(False)) ensures null comparisons resolve to False, not null.
# MAGIC cleaned_df = bronze_df.withColumns(
# MAGIC     {
# MAGIC         # flag rows where optional passenger_count is missing
# MAGIC         "flag_missing_optional_fields": F.col("passenger_count").isNull(),
# MAGIC         # flag negative fare amounts (refunds or data errors)
# MAGIC         "flag_negative_fare": F.coalesce(F.col("fare_amount") < 0, F.lit(False)),
# MAGIC         # flag trips with zero passengers (missing passenger info)
# MAGIC         "flag_zero_passenger": F.coalesce(F.col("passenger_count") == 0, F.lit(False)),
# MAGIC         # flag trips charged despite zero distance
# MAGIC         "flag_zero_distance_nonzero_fare": F.coalesce(
# MAGIC             (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0),
# MAGIC             F.lit(False),
# MAGIC         ),
# MAGIC         # derive trip duration in minutes; unix_timestamp needed because bronze
# MAGIC         # timestamps are TIMESTAMP_NTZ (not castable to DOUBLE)
# MAGIC         "trip_duration_min": (
# MAGIC             F.unix_timestamp(F.col("tpep_dropoff_datetime"))
# MAGIC             - F.unix_timestamp(F.col("tpep_pickup_datetime"))
# MAGIC         )
# MAGIC         / F.lit(60.0),
# MAGIC     }
# MAGIC )
# MAGIC # flag_negative_or_zero_duration depends on trip_duration_min added above,
# MAGIC # so it must be a separate withColumn (withColumns expressions can't reference each other)
# MAGIC cleaned_df = cleaned_df.withColumn(
# MAGIC     "flag_negative_or_zero_duration",
# MAGIC     F.coalesce(F.col("trip_duration_min") <= 0, F.lit(False)),
# MAGIC )
# MAGIC
# MAGIC # fill nulls in store_and_fwd_flag with a sentinel value
# MAGIC if "store_and_fwd_flag" in cleaned_df.columns:
# MAGIC     cleaned_df = cleaned_df.withColumn(
# MAGIC         "store_and_fwd_flag",
# MAGIC         F.coalesce(F.col("store_and_fwd_flag"), F.lit("N/A")),
# MAGIC     )
# MAGIC # cast RatecodeID from long to int for consistency
# MAGIC if "RatecodeID" in cleaned_df.columns:
# MAGIC     cleaned_df = cleaned_df.withColumn(
# MAGIC         "RatecodeID",
# MAGIC         F.col("RatecodeID").cast("int"),
# MAGIC     )
# MAGIC
# MAGIC # write the silver Delta table partitioned by year_month
# MAGIC write_month_partition(cleaned_df, table_name("silver"))
# MAGIC # verify the silver write by re-reading and comparing row counts (no rows should be dropped)
# MAGIC silver_count = (
# MAGIC     spark.table(table_name("silver"))
# MAGIC     .where(F.col("year_month") == YEAR_MONTH)
# MAGIC     .count()
# MAGIC )
# MAGIC if silver_count != bronze_count:  # fail if any rows were silently dropped
# MAGIC     raise AssertionError(
# MAGIC         f"Silver row count changed: bronze={bronze_count}, silver={silver_count}"
# MAGIC     )
# MAGIC
# MAGIC # aggregate and print the count of each flag column for quick visibility
# MAGIC flag_columns = [
# MAGIC     column for column in cleaned_df.columns if column.startswith("flag_")
# MAGIC ]
# MAGIC flag_counts = cleaned_df.agg(
# MAGIC     *[F.sum(F.col(column).cast("long")).alias(column) for column in flag_columns]
# MAGIC ).first().asDict()
# MAGIC print(f"Silver Delta table: {table_name('silver')}")
# MAGIC print(f"Silver row count unchanged: {silver_count:,}")
# MAGIC print(f"Flag totals: {flag_counts}")