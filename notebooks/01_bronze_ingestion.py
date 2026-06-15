# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Wikimedia Pageview Ingestion
# MAGIC
# MAGIC This notebook creates the Bronze layer for the Global Attention Analytics project.
# MAGIC
# MAGIC The Bronze layer is the raw ingestion layer of the Lakehouse pipeline. Hourly Wikimedia
# MAGIC pageview files are downloaded, decompressed, lightly structured, and stored as a Delta table.
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC - downloads hourly Wikimedia pageview files
# MAGIC - reads compressed `.gz` files
# MAGIC - keeps English Wikipedia desktop and mobile projects
# MAGIC - extracts the four raw fields from each line
# MAGIC - adds source metadata such as date, hour, and source URL
# MAGIC - writes the result to a Bronze Delta table
# MAGIC
# MAGIC ## Input Data
# MAGIC
# MAGIC The raw data comes from Wikimedia hourly pageview files.
# MAGIC
# MAGIC Each raw line contains four fields:
# MAGIC
# MAGIC - project
# MAGIC - page title
# MAGIC - views
# MAGIC - bytes sent
# MAGIC
# MAGIC The project currently focuses on:
# MAGIC
# MAGIC - `en` = English Wikipedia desktop
# MAGIC - `en.m` = English Wikipedia mobile
# MAGIC
# MAGIC ## Output Table
# MAGIC
# MAGIC `workspace.wiki_bronze.pageviews_raw`
# MAGIC
# MAGIC ## Why This Step Matters
# MAGIC
# MAGIC The Bronze layer keeps the data close to the original source while storing it in a structured
# MAGIC Delta table. This makes the pipeline reproducible and allows later notebooks to use the same
# MAGIC raw data without downloading it again.
# MAGIC
# MAGIC The Bronze table is intentionally not fully cleaned. Cleaning, normalization, and topic
# MAGIC categorization are handled in the Silver layer.
# MAGIC
# MAGIC ## Current Scope
# MAGIC
# MAGIC Each run loads the latest twelve completed Wikimedia hourly files after a three-hour safety
# MAGIC delay. The table is refreshed as a rolling twelve-hour snapshot, which keeps the project
# MAGIC efficient in Databricks Free Edition while supporting regularly updated analytics.

# COMMAND ----------

import gzip
import urllib.request
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark.sql("USE CATALOG workspace")

BRONZE_TABLE = "wiki_bronze.pageviews_raw"
TARGET_PROJECTS = {"en", "en.m"}
SAFETY_DELAY_HOURS = 3
HOURS_BACK = 12

PAGEVIEW_SCHEMA = StructType([
    StructField("project", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("bytes_sent", IntegerType(), True),
    StructField("source_date", StringType(), True),
    StructField("source_hour", IntegerType(), True),
    StructField("source_url", StringType(), True),
])

print("Using catalog: workspace")
print("Bronze table:", BRONZE_TABLE)

# COMMAND ----------

def load_pageview_hour(source_date: str, source_hour: int) -> DataFrame:
    """
    Download one hourly Wikimedia pageview file, retain English Wikipedia
    desktop and mobile rows, and return the result as a Spark DataFrame.
    """

    date_compact = source_date.replace("-", "")
    year_month = source_date[:7]
    year = source_date[:4]

    source_url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year_month}/pageviews-{date_compact}-{source_hour:02d}0000.gz"
    )

    print(f"Loading hour {source_hour:02d}: {source_url}")

    rows = []

    with urllib.request.urlopen(source_url, timeout=180) as response:
        with gzip.GzipFile(fileobj=response) as compressed_file:
            for raw_line in compressed_file:
                line = raw_line.decode("utf-8", errors="replace").strip()
                parts = line.split(" ")

                if len(parts) != 4:
                    continue

                project, page_title, views, bytes_sent = parts

                if project not in TARGET_PROJECTS:
                    continue

                rows.append((
                    project,
                    page_title,
                    int(views),
                    int(bytes_sent),
                    source_date,
                    source_hour,
                    source_url,
                ))

    print(f"Rows loaded for hour {source_hour:02d}: {len(rows)}")

    return spark.createDataFrame(rows, PAGEVIEW_SCHEMA)

# COMMAND ----------

# Load the latest twelve completed hourly files.
# A safety delay is used because the newest Wikimedia file may not be available immediately.

now_utc = datetime.now(timezone.utc)

end_time = (
    now_utc.replace(minute=0, second=0, microsecond=0)
    - timedelta(hours=SAFETY_DELAY_HOURS)
)

hours_to_load = sorted(
    end_time - timedelta(hours=offset)
    for offset in range(HOURS_BACK)
)

print("Loading dynamic 12-hour window:")
for timestamp in hours_to_load:
    print(timestamp.strftime("%Y-%m-%d %H:00 UTC"))

# The first hourly DataFrame replaces the previous rolling snapshot.
# The remaining hourly DataFrames are appended to complete the twelve-hour window.

for index, timestamp in enumerate(hours_to_load):
    source_date = timestamp.strftime("%Y-%m-%d")
    source_hour = timestamp.hour

    hour_df = load_pageview_hour(source_date, source_hour)
    write_mode = "overwrite" if index == 0 else "append"

    (
        hour_df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )

    print(
        f"Written {source_date} hour {source_hour:02d} "
        f"to {BRONZE_TABLE} with mode: {write_mode}"
    )

print("Dynamic 12-hour Bronze ingestion finished.")

# COMMAND ----------

# Validate the resulting rolling snapshot.

spark.sql(f"""
SELECT
    COUNT(*) AS rows,
    SUM(views) AS total_views,
    COUNT(DISTINCT CONCAT(source_date, '-', source_hour)) AS loaded_hour_slots,
    MIN(source_date) AS min_date,
    MAX(source_date) AS max_date,
    MIN(source_hour) AS min_hour,
    MAX(source_hour) AS max_hour
FROM {BRONZE_TABLE}
""").show()
