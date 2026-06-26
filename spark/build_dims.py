# spark/build_dims.py
# Build Day 3, Increment 2 — the 4 DIMENSION tables via dropDuplicates.
# Dims = deduplicated lookups: one row per UNIQUE key (one per actor, not per event).

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, to_timestamp, to_date,
    year, month, dayofmonth, hour, dayofweek, date_format
)
from schema import event_schema

spark = (
    SparkSession.builder
    .appName("gh-firehose-build-dims")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

path = "/Users/felipefumero/projects/github-firehose-activity/data/2024-06-02-*.json.gz"
df = spark.read.schema(event_schema).json(path)

# Filter to our 4 types (dims describe entities seen in our events).
TARGET_TYPES = ["PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent"]
events = df.filter(col("type").isin(TARGET_TYPES))

# ============================================================
# DIM_ACTOR — one row per unique actor.
# select the 2 cols, then dropDuplicates on the KEY (actor_id).
# dropDuplicates(["actor_id"]) = "keep one row per unique actor_id, other cols as-is."
# ============================================================
dim_actor = (
    events.select(
        col("actor.id").alias("actor_id"),
        col("actor.login").alias("actor_login"),
    )
    .dropDuplicates(["actor_id"])
)

# ============================================================
# DIM_ORG — one row per unique org. Same pattern.
# org is nullable, so we also drop rows where org_id is null (no point storing a null org).
# ============================================================
dim_org = (
    events.select(
        col("org.id").alias("org_id"),
        col("org.login").alias("org_login"),
    )
    .where(col("org_id").isNotNull())   # skip events that had no org
    .dropDuplicates(["org_id"])
)

# ============================================================
# DIM_REPO — one row per unique repo, PLUS a derived repo_owner.
# repo.name is "owner/repo" (e.g. "lost-kwt/PUBG-Mobile-Bypass-Source").
# split(name, "/") splits on the slash into an array; [0] is the owner part.
# This is a small derivation = a talking point (deriving dim attributes during the build).
# ============================================================
dim_repo = (
    events.select(
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("repo_name"),
        split(col("repo.name"), "/").getItem(0).alias("repo_owner"),  # "owner/repo" -> "owner"
    )
    .dropDuplicates(["repo_id"])
)

# ============================================================
# DIM_TIME — one row per unique date_key, with date parts broken out.
# Build from the timestamp: parse created_at -> ts, derive date_key, then extract
# year/month/day/hour/day_of_week. dropDuplicates on date_key.
# NOTE: hour varies WITHIN a day, so a single date_key can have many hours — for a
# DATE-grain dim we drop hour from the dedup key. (Keeping hour here is a design choice;
# we treat DIM_TIME at DATE grain, so hour is illustrative. We'll revisit if needed.)
# ============================================================
dim_time = (
    events
    .withColumn("created_ts", to_timestamp(col("created_at")))
    .withColumn("date_key", date_format(col("created_ts"), "yyyyMMdd").cast("int"))
    .select(
        col("date_key"),
        to_date(col("created_ts")).alias("date"),     # 2024-06-02
        year(col("created_ts")).alias("year"),
        month(col("created_ts")).alias("month"),
        dayofmonth(col("created_ts")).alias("day"),
        dayofweek(col("created_ts")).alias("day_of_week"),  # 1=Sunday ... 7=Saturday
    )
    .dropDuplicates(["date_key"])
)

# ============================================================
# VALIDATE — each dim's row count should be MUCH smaller than the fact (dedup worked),
# and printSchema/show confirm shape.
# ============================================================
for name, d in [("DIM_ACTOR", dim_actor), ("DIM_ORG", dim_org),
                ("DIM_REPO", dim_repo), ("DIM_TIME", dim_time)]:
    print(f"=== {name} count ===")
    print(d.count())
    d.show(5, truncate=False)

spark.stop()