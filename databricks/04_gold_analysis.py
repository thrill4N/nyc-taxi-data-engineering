# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

# load the gold hourly summary table, filtered to the target month
hourly = (
    spark.table(table_name("hourly"))
    .where(F.col("year_month") == YEAR_MONTH)
    .orderBy("pickup_hour")  # chronological by hour of day (0–23)
)
# load the gold day-of-week summary table
# NOTE: ordered alphabetically by day name, not chronologically
dow = (
    spark.table(table_name("dow"))
    .where(F.col("year_month") == YEAR_MONTH)
    .orderBy("pickup_day_of_week")  # alphabetical: Friday, Monday, Saturday, ...
)
# load the gold route summary table, busiest routes first
routes = (
    spark.table(table_name("routes"))
    .where(F.col("year_month") == YEAR_MONTH)
    .orderBy(F.desc("trip_count"))  # descending trip count
)

# print header to contextualize the displayed tables
print(f"NYC Yellow Taxi analysis for {YEAR_MONTH}")
print(
    "Review the hourly, day-of-week, and route tables below before replacing "
    "the narrative placeholders with data-backed findings."
)

# display each table inline for visual inspection
display(hourly)  # full hourly breakdown (24 rows max)
display(dow)  # full day-of-week breakdown (7 rows)
display(routes.limit(10))  # top 10 busiest routes only, to keep output concise

# Keep narrative claims manual, as in the local analysis stage.
# The checklist below guides the analyst to fill in findings after reviewing the tables.
print(
    f"""
Narrative review checklist for {YEAR_MONTH}
- Peak trip volume occurs at [FILL IN after reviewing hourly output].
- Average fare [does/does not] track trip volume [REVIEW].
- [Day] sees the highest trip count; [Day] the lowest [REVIEW].
- Top 10 routes account for [CALCULATE after reviewing route output].
- Schema and quality reports are available in the corresponding Delta tables.
"""
)