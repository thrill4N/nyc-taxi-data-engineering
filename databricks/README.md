# Databricks Free Edition pipeline

This is a separate PySpark implementation of the local Pandas pipeline. It
does not replace or modify `src/`. The stages use Unity Catalog managed Delta
tables and a Unity Catalog Volume for the downloaded TLC Parquet file:

| Existing local stage | Databricks layer | Artifact |
| --- | --- | --- |
| `src/ingest.py` | Bronze | `bronze_yellow_tripdata`, plus `bronze_quality_metrics` |
| `src/clean.py` | Silver | `silver_yellow_tripdata`, plus `silver_schema_validation` |
| `src/transform.py` | Gold | Hourly, day-of-week, and route Delta tables |
| `src/analysis.py` | Gold consumption | Notebook tables/charts and manual narrative prompts |

The PySpark stages preserve the local semantics: validation runs before flags,
Silver retains every input row, and only the explicitly defined analysis
subset excludes negative fares, zero/negative-duration trips, and zero
passengers. The selected month is replaced as one Delta partition on rerun;
other months are retained. Sample tables use `_sample` suffixes and never
overwrite full-run tables.

## Workspace setup

1. Create a Databricks Free Edition workspace and open its workspace UI.
2. Connect the GitHub repository in Databricks Git folders / Repos and check
   out the branch containing the `databricks/` directory.
3. Use a serverless notebook compute supported by the Free Edition workspace.
4. Confirm your user can create or use a Unity Catalog catalog, schema, and
   managed Volume. The defaults in `00_setup.py` are catalog `nyc_taxi`,
   schema `pipeline`, and Volume `taxi_data`. Edit those constants if you
   need to use a catalog provisioned for your workspace.
5. Open and run `00_setup.py` once. It creates the catalog, schema, and Volume
   idempotently. If workspace permissions disallow catalog creation, select a
   catalog you already have access to in `00_setup.py`.
6. Install Pandera on the notebook's serverless compute using the workspace's
   supported package/library mechanism (for example, `%pip install pandera`).
   Restart the Python process after installation, then run `02_silver_clean.py`.
   The notebook runs a valid/invalid synthetic compatibility check before
   processing data. It fails explicitly if the installed Pandera PySpark
   backend cannot validate the schema on that runtime.

No Databricks, Spark, or Pandera dependency is added to the local
`requirements.txt`; the local pipeline remains independently runnable.

## Run sample first, then the full month

1. Open `run_all.py` and leave `sample_mode` set to `true`. It defaults to
   `year_month=2024-01` and limits the Spark data to 5,000 rows. Run all cells.
   It downloads the source Parquet to the Volume if it is not already there,
   then creates sample-suffixed bronze, silver, and gold tables.
2. Inspect the displayed gold summaries and the JSON reports stored in the
   quality and validation Delta tables. Confirm the silver count equals the
   bronze count.
3. Only after the sample run succeeds, set `sample_mode` to `false` and run
   `run_all.py` again for the full month. The full-run tables do not share
   names with sample tables.
4. To run a single stage, open that notebook and set its `year_month` and
   `sample_mode` widgets consistently with the tables you want to read/write.

`run_all.py` calls the four stage notebooks sequentially and can itself be
used as the notebook task in a Databricks Workflow / Job. Keep the workspace's
serverless compute settings at their defaults and stop using the notebook
when the run is complete.

## Table names and checks

Full-run tables live in `{catalog}.{schema}`:

- `bronze_yellow_tripdata`
- `bronze_quality_metrics`
- `silver_yellow_tripdata`
- `silver_schema_validation`
- `gold_hourly_summary`
- `gold_day_of_week_summary`
- `gold_route_summary`

Sample-mode tables append `_sample` to each table name. Both quality reports
are JSON strings in their respective Delta tables (`report_json`) and follow
the local reports' `passed` / `failure_count` / `failures` shape where
applicable. The bronze report also contains `year_month`, `row_count`, null
counts, and the three ingestion scan counts.

Compare Databricks bronze and silver row counts with `row_count` in the local
`data/reports/quality_report_<month>.json`. Sample-mode counts are expected to
be 5,000 or fewer; compare full-month counts only after running with
`sample_mode=false`.

## Operational notes

- The TLC source is public and needs no credentials. Downloads are staged
  before being moved to the Volume so an interrupted download is not treated
  as a completed source file.
- The source file is reused on reruns. Delta writes replace only the selected
  `year_month` partition, making retries idempotent without duplicating rows.
- `spark.sql.session.timeZone` is set to UTC so date/hour derivations and
  timestamp differences are stable across compute sessions.
- Pandera compatibility is intentionally tested on the active Free Edition
  runtime; package availability and runtime behavior may change. No fallback
  silently bypasses validation.
- Analysis output uses notebook `display()` tables, which can be configured as
  charts in Databricks. Narrative findings remain placeholders until a person
  reviews the actual outputs.
