# spark/transform_commits.py
# Build Day 3, Increment 1 — FACT_COMMITS at COMMIT grain (explode the commits array).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, date_format
from schema import event_schema

spark = (
    SparkSession.builder
    .appName("gh-firehose-transform-commits")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

path = "/Users/felipefumero/projects/github-firehose-activity/data/2024-06-02-*.json.gz"
df = spark.read.schema(event_schema).json(path)

# --- Filter early: commits only come from PushEvents, so narrow to those FIRST.
# Tighter than the events filter — for FACT_COMMITS we only care about pushes.
pushes = df.filter(col("type") == "PushEvent")

# --- THE EXPLODE: array -> rows. This is the GRAIN CHANGE.
# Before: one PushEvent row with payload.commits = [{sha,msg},{sha,msg},...]
# After:  one row PER commit, with the parent event's id/actor/repo copied onto each.
#
# explode() vs explode_outer():
#   explode()       -> DROPS rows where commits is null/empty (a push with no commits vanishes)
#   explode_outer() -> KEEPS them, emitting one row with a null commit
# We use explode() ON PURPOSE: a push with no commits contributes ZERO commit-grain rows,
# which is correct — FACT_COMMITS should only contain real commits. (Smoke test showed
# many pushes carry no commits array; we intentionally exclude those here.)
exploded = pushes.withColumn("commit", explode(col("payload.commits")))

# --- Flatten to the FACT_COMMITS columns (commit grain).
# Parent event fields (event_id, actor_id, repo_id, date_key) get copied onto every commit row;
# commit.sha / commit.message come from the exploded element.
fact_commits = (
    exploded
    .withColumn("created_ts", to_timestamp(col("created_at")))
    .withColumn("date_key", date_format(col("created_ts"), "yyyyMMdd").cast("int"))
    .select(
        col("id").alias("event_id"),          # FK back to the event
        col("actor.id").alias("actor_id"),    # FK
        col("repo.id").alias("repo_id"),      # FK
        col("date_key"),                       # FK
        col("commit.sha").alias("commit_sha"),       # from the exploded commit
        col("commit.message").alias("commit_message"),
    )
)

# --- Validate the grain change ---
# push event count vs commit row count: commits should be GREATER (multi-commit pushes
# fan out). Seeing commits > pushes is the explode/grain-change working.
print("=== push events (parent grain) ===")
print(pushes.count())

print("=== FACT_COMMITS rows (commit grain) ===")
print(fact_commits.count())

print("=== FACT_COMMITS schema ===")
fact_commits.printSchema()

print("=== FACT_COMMITS sample ===")
fact_commits.show(5, truncate=False)

spark.stop()