# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
if "year_month" not in dbutils.widgets.getAll():
    dbutils.widgets.text("year_month", "2024-01", "TLC data month (YYYY-MM)")
if "sample_mode" not in dbutils.widgets.getAll():
    dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "Use first 5,000 rows")

year_month = dbutils.widgets.get("year_month")
sample_mode = dbutils.widgets.get("sample_mode").lower()

if sample_mode not in {"true", "false"}:
    raise ValueError("sample_mode must be 'true' or 'false'")

parameters = {
    "year_month": year_month,
    "sample_mode": sample_mode,
}

for notebook in (
    "./01_bronze_ingest",
    "./02_silver_clean",
    "./03_gold_transform",
    "./04_gold_analysis",
):
    print(f"Running {notebook} with {parameters}")
    dbutils.notebook.run(notebook, 0, parameters)

print("NYC Taxi Databricks pipeline completed successfully.")