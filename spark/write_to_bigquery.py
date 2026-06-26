# spark/write_to_bigquery.py
# Build Day 3, Increment 3 — write all facts + dims to BigQuery.
# Builds FACT_EVENTS, FACT_COMMITS, and the 4 dims, then writes each to BigQuery,
# partitioned by date_key.

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, to_timestamp, to_date,
    year, month, dayofmonth, dayofweek, date_format
)
from schema import event_schema

# --- project / dataset / bucket constants (your real GCP setup) ---
GCP_PROJECT = "github-firehose-fumero"
BQ_DATASET = "github_firehose"
STAGING_BUCKET = "github-firehose-fumero-staging"
DATA_PATH = "/Users/felipefumero/projects/github-firehose-activity/data/2024-06-02-*.json.gz"

# --- Spark session WITH the BigQuery connector jar ---
# spark.jars.packages downloads the Spark-BigQuery connector at startup (first run = slow,
# it fetches the jar; cached after). This is what lets .write.format("bigquery") work.
spark = (
    SparkSession.builder
    .appName("gh-firehose-write-bq")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
    .config("spark.sql.catalogImplementation", "in-memory")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# READ + shared filtered events
# ============================================================
df = spark.read.schema(event_schema).json(DATA_PATH)
TARGET_TYPES = ["PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent"]
events = df.filter(col("type").isin(TARGET_TYPES))

# ============================================================
# FACT_EVENTS (event grain)
# ============================================================
fact_events = (
    events.select(
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        col("actor.id").alias("actor_id"),
        col("actor.login").alias("actor_login"),
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("repo_name"),
        col("org.id").alias("org_id"),
        col("org.login").alias("org_login"),
        col("payload.action").alias("action"),
        col("created_at"),
    )
    .withColumn("created_ts", to_timestamp(col("created_at")))
    .withColumn("date_key", date_format(col("created_ts"), "yyyyMMdd").cast("int"))
    .drop("created_at")
)

# ============================================================
# FACT_COMMITS (commit grain — explode)
# ============================================================
pushes = df.filter(col("type") == "PushEvent")
fact_commits = (
    pushes
    .withColumn("commit", explode(col("payload.commits")))
    .withColumn("created_ts", to_timestamp(col("created_at")))
    .withColumn("date_key", date_format(col("created_ts"), "yyyyMMdd").cast("int"))
    .select(
        col("id").alias("event_id"),
        col("actor.id").alias("actor_id"),
        col("repo.id").alias("repo_id"),
        col("date_key"),
        col("commit.sha").alias("commit_sha"),
        col("commit.message").alias("commit_message"),
    )
)

# ============================================================
# DIMENSIONS
# ============================================================
dim_actor = events.select(
    col("actor.id").alias("actor_id"),
    col("actor.login").alias("actor_login"),
).dropDuplicates(["actor_id"])

dim_org = events.select(
    col("org.id").alias("org_id"),
    col("org.login").alias("org_login"),
).where(col("org_id").isNotNull()).dropDuplicates(["org_id"])

dim_repo = events.select(
    col("repo.id").alias("repo_id"),
    col("repo.name").alias("repo_name"),
    split(col("repo.name"), "/").getItem(0).alias("repo_owner"),
).dropDuplicates(["repo_id"])

dim_time = (
    events
    .withColumn("created_ts", to_timestamp(col("created_at")))
    .withColumn("date_key", date_format(col("created_ts"), "yyyyMMdd").cast("int"))
    .select(
        col("date_key"),
        to_date(col("created_ts")).alias("date"),
        year(col("created_ts")).alias("year"),
        month(col("created_ts")).alias("month"),
        dayofmonth(col("created_ts")).alias("day"),
        dayofweek(col("created_ts")).alias("day_of_week"),
    )
    .dropDuplicates(["date_key"])
)

# ============================================================
# WRITE — helper to write one table to BigQuery
# ============================================================
def write_bq(dataframe, table_name, partition_field=None):
    # .format("bigquery") = the connector; table = project.dataset.table
    writer = (
        dataframe.write
        .format("bigquery")
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}")
        .option("writeMethod", "direct") 
        .mode("overwrite")   # clean re-runs while testing; Airflow will switch to append-by-date later
    )
    # partition the big facts by date_key so date-filtered queries scan less
    if partition_field:
        writer = writer.option("partitionField", partition_field) \
                       .option("partitionType", "DAY")
    writer.save()
    print(f"  ✓ wrote {table_name}")

print("=== writing to BigQuery ===")
# facts partitioned by date; dims small enough to leave unpartitioned
write_bq(fact_events,  "fact_events",  partition_field=None)   # date_key is int, see note below
write_bq(fact_commits, "fact_commits", partition_field=None)
write_bq(dim_actor,    "dim_actor")
write_bq(dim_org,      "dim_org")
write_bq(dim_repo,     "dim_repo")
write_bq(dim_time,     "dim_time")
print("=== done ===")

spark.stop()