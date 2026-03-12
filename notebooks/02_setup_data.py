# Databricks notebook source
# MAGIC %md
# MAGIC # Commodities Market Data Pipeline - CSV Import
# MAGIC
# MAGIC This notebook loads the bronze, silver, and gold market data CSVs from the
# MAGIC **Commodities Market Data Pipeline/data** folder into Databricks tables under
# MAGIC the `hackathon.market_agents` schema.
# MAGIC
# MAGIC **What it creates:**
# MAGIC | Table | Source | Description |
# MAGIC |-------|--------|-------------|
# MAGIC | `bronze_market_data` | bronze_market_data.csv | Raw ingested market signal data |
# MAGIC | `silver_market_data` | silver_market_data.csv | Cleaned and enriched market data |
# MAGIC | `gold_market_summary` | gold_market_summary.csv | Aggregated market summary for analytics |
# MAGIC
# MAGIC **Prerequisites:** Run `01_setup_data` first to initialise the catalog and base Petrinex tables.
# MAGIC
# MAGIC **Runtime:** ~1 minute

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "hackathon", "Catalog Name")
dbutils.widgets.text("schema", "market_agents", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Target: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Schema if Not Exists

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Schema '{catalog}.{schema}' ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resolve CSV Paths
# MAGIC
# MAGIC Locates the Commodities Market Data Pipeline CSVs relative to this notebook's repo root.

# COMMAND ----------

import os

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + str(os.path.dirname(os.path.dirname(notebook_path)))
csv_dir = os.path.join(repo_root, "Commodities Market Data Pipeline", "data")

bronze_csv = os.path.join(csv_dir, "bronze_market_data.csv")
silver_csv = os.path.join(csv_dir, "silver_market_data.csv")
gold_csv   = os.path.join(csv_dir, "gold_market_summary.csv")

print(f"CSV directory : {csv_dir}")
print(f"Bronze CSV    : {bronze_csv}")
print(f"Silver CSV    : {silver_csv}")
print(f"Gold CSV      : {gold_csv}")

# Verify files exist before loading
for path in [bronze_csv, silver_csv, gold_csv]:
    if os.path.exists(path):
        print(f"  FOUND : {path}")
    else:
        raise FileNotFoundError(f"  MISSING: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Bronze Market Data

# COMMAND ----------

bronze_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_csv)
)

bronze_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_market_data")

bronze_count = spark.table(f"{catalog}.{schema}.bronze_market_data").count()
print(f"bronze_market_data: {bronze_count:,} rows")
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load Silver Market Data

# COMMAND ----------

silver_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(silver_csv)
)

silver_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_market_data")

silver_count = spark.table(f"{catalog}.{schema}.silver_market_data").count()
print(f"silver_market_data: {silver_count:,} rows")
silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load Gold Market Summary

# COMMAND ----------

gold_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(gold_csv)
)

gold_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_market_summary")

gold_count = spark.table(f"{catalog}.{schema}.gold_market_summary").count()
print(f"gold_market_summary: {gold_count:,} rows")
gold_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"{'='*60}")
print(f"  MARKET DATA SETUP COMPLETE: {catalog}.{schema}")
print(f"{'='*60}\n")

for t in ["bronze_market_data", "silver_market_data", "gold_market_summary"]:
    count = spark.table(f"{catalog}.{schema}.{t}").count()
    print(f"  {t:30s} {count:>10,} rows")

print(f"\n  All data loaded from Commodities Market Data Pipeline/data/")
print(f"{'='*60}")