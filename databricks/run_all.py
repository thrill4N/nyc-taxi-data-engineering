# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# create input widgets if not already present (avoids duplicates on re-runs)
if "year_month" not in dbutils.widgets.getAll():
    dbutils.widgets.text("year_month", "2024-01", "TLC data month (YYYY-MM)")
if "sample_mode" not in dbutils.widgets.getAll():
    dbutils.widgets.dropdown("sample_mode", "true", ["true", "false"], "Use first 5,000 rows")

# read widget values for the pipeline parameters
year_month = dbutils.widgets.get("year_month")
sample_mode = dbutils.widgets.get("sample_mode").lower()

# validate sample_mode before passing it to child notebooks
if sample_mode not in {"true", "false"}:
    raise ValueError("sample_mode must be 'true' or 'false'")

# assemble the parameter dict passed to every child notebook
parameters = {
    "year_month": year_month,
    "sample_mode": sample_mode,
}

# run the four pipeline notebooks in order; each receives the same widgets
# bronze -> silver -> gold -> analysis
for notebook in (
    "./01_bronze_ingest",   # download + raw load + quality profiling
    "./02_silver_clean",    # schema validation + data quality flags
    "./03_gold_transform",  # filter anomalies + aggregate to gold tables
    "./04_gold_analysis",   # display gold tables + narrative checklist
):
    print(f"Running {notebook} with {parameters}")
    dbutils.notebook.run(notebook, 0, parameters)  # 0 timeout = no timeout

print("NYC Taxi Databricks pipeline completed successfully.")