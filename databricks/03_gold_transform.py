# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

# load the silver Delta table for the target month
silver_df = (
    spark.table(table_name("silver"))
    .where(F.col("year_month") == YEAR_MONTH)
)

# filter out anomalous rows flagged in the silver layer, then derive
# time dimensions for hourly, day-of-week, and route aggregations
analysis_df = (
    silver_df
    .where(
        ~F.col("flag_negative_fare")  # exclude negative fare amounts
        & ~F.col("flag_negative_or_zero_duration")  # exclude invalid trip durations
        & ~F.col("flag_zero_passenger")  # exclude zero-passenger trips
    )
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))  # 0–23 hour of day
    .withColumn(
        "pickup_day_of_week",
        F.date_format("tpep_pickup_datetime", "EEEE"),  # full day name (e.g., "Monday")
    )
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))  # date portion only
)

# materialize counts to report how many rows were included vs excluded
analysis_count = analysis_df.count()
excluded_count = silver_df.count() - analysis_count
print(
    f"Analysis subset: {analysis_count:,} rows "
    f"({excluded_count:,} excluded; preserved in Silver)"
)

# aggregate by pickup hour: trip volume, avg fare, avg distance, total revenue
hourly = (
    analysis_df.groupBy("pickup_hour")
    .agg(
        F.count("VendorID").alias("trip_count"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_distance").alias("avg_distance"),
        F.sum("total_amount").alias("total_revenue"),
    )
    .withColumn("year_month", F.lit(YEAR_MONTH))  # tag with ingestion month
)

# aggregate by day of week: trip volume, avg fare, avg tip percentage
# avg_tip_pct guards against division by zero by skipping rows where fare_amount = 0
dow = (
    analysis_df.groupBy("pickup_day_of_week")
    .agg(
        F.count("VendorID").alias("trip_count"),
        F.avg("fare_amount").alias("avg_fare"),
        (
            F.avg(
                F.when(
                    F.col("fare_amount") != 0,
                    F.col("tip_amount") / F.col("fare_amount"),
                )
            )
            * F.lit(100.0)
        ).alias("avg_tip_pct"),
    )
    .withColumn("year_month", F.lit(YEAR_MONTH))
)

# aggregate by pickup/dropoff location pair: top 20 busiest routes
# ordered by trip count desc, then location IDs for deterministic tie-breaking
routes = (
    analysis_df.groupBy("PULocationID", "DOLocationID")
    .agg(F.count(F.lit(1)).alias("trip_count"))
    .orderBy(F.desc("trip_count"), "PULocationID", "DOLocationID")
    .limit(20)  # keep only the 20 busiest routes
    .withColumn("year_month", F.lit(YEAR_MONTH))
)

# write all three gold tables partitioned by year_month
write_month_partition(hourly, table_name("hourly"))
write_month_partition(dow, table_name("dow"))
write_month_partition(routes, table_name("routes"))

print(
    "Gold Delta tables written: "
    f"{table_name('hourly')}, {table_name('dow')}, {table_name('routes')}"
)