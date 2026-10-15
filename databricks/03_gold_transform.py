# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F


silver_df = (
    spark.table(table_name("silver"))
    .where(F.col("year_month") == YEAR_MONTH)
)
analysis_df = (
    silver_df
    .where(
        ~F.col("flag_negative_fare")
        & ~F.col("flag_negative_or_zero_duration")
        & ~F.col("flag_zero_passenger")
    )
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn(
        "pickup_day_of_week",
        F.date_format("tpep_pickup_datetime", "EEEE"),
    )
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
)

try:
    analysis_count = analysis_df.count()
    excluded_count = silver_df.count() - analysis_count
    print(
        f"Analysis subset: {analysis_count:,} rows "
        f"({excluded_count:,} excluded; preserved in Silver)"
    )

    hourly = (
        analysis_df.groupBy("pickup_hour")
        .agg(
            F.count("VendorID").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .withColumn("year_month", F.lit(YEAR_MONTH))
    )

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

    routes = (
        analysis_df.groupBy("PULocationID", "DOLocationID")
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .orderBy(F.desc("trip_count"), "PULocationID", "DOLocationID")
        .limit(20)
        .withColumn("year_month", F.lit(YEAR_MONTH))
    )

    write_month_partition(hourly, table_name("hourly"))
    write_month_partition(dow, table_name("dow"))
    write_month_partition(routes, table_name("routes"))

    print(
        "Gold Delta tables written: "
        f"{table_name('hourly')}, {table_name('dow')}, {table_name('routes')}"
    )
finally:
    pass