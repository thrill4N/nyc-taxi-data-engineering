# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F


hourly = (
    spark.table(table_name("hourly"))
    .where(F.col("year_month") == YEAR_MONTH)
    .orderBy("pickup_hour")
)
dow = (
    spark.table(table_name("dow"))
    .where(F.col("year_month") == YEAR_MONTH)
    .orderBy("pickup_day_of_week")
)
routes = (
    spark.table(table_name("routes"))
    .where(F.col("year_month") == YEAR_MONTH)
    .orderBy(F.desc("trip_count"))
)

print(f"NYC Yellow Taxi analysis for {YEAR_MONTH}")
print(
    "Review the hourly, day-of-week, and route tables below before replacing "
    "the narrative placeholders with data-backed findings."
)
display(hourly)
display(dow)
display(routes.limit(10))

# Keep narrative claims manual, as in the local analysis stage.
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