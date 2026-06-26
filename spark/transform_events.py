# spark/transform_events.py
# Pipeline: read → filter early → flatten → parse timestamp + date_key.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format
from schema import event_schema   # our hand-written explicit schema (the contract)

# ============================================================
# SPARK SESSION — boots local Spark using all CPU cores ([*]).
# ============================================================
spark = (
    SparkSession.builder
    .appName("gh-firehose-transform-events")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")   # quiet the noisy INFO logs

# ============================================================
# READ — load one day with the EXPLICIT schema (no inference).
# .schema(event_schema) = use our contract; Spark decompresses .gz itself.
# ============================================================
path = "/Users/felipefumero/projects/github-firehose-activity/data/2024-06-02-*.json.gz"
df = spark.read.schema(event_schema).json(path)

# ============================================================
# INCREMENT 1 — FILTER EARLY (predicate pushdown)
# Keep only our 4 target types BEFORE any heavy work, so flatten/parse
# downstream only runs on rows we keep. isin() = SQL's IN.
# ============================================================
TARGET_TYPES = ["PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent"]
events = df.filter(col("type").isin(TARGET_TYPES))

# ============================================================
# INCREMENT 2 — FLATTEN NESTED STRUCTS
# Lift buried fields (actor.login, repo.name, ...) into flat top-level columns.
# col("a.b") reaches INTO the struct; .alias() renames it flat.
# .select() builds the EXACT final column set (drops leftover structs).
# created_at stays raw here — parsed in increment 3.
# ============================================================
fact_events = events.select(
    col("id").alias("event_id"),            # event-grain primary key
    col("type").alias("event_type"),
    col("actor.id").alias("actor_id"),
    col("actor.login").alias("actor_login"),
    col("repo.id").alias("repo_id"),
    col("repo.name").alias("repo_name"),
    col("org.id").alias("org_id"),          # nullable: null when event has no org
    col("org.login").alias("org_login"),
    col("payload.action").alias("action"),  # null on PushEvents (expected)
    col("created_at"),                       # raw string, parsed next
)

# ============================================================
# INCREMENT 3 — PARSE TIMESTAMP + DERIVE date_key
# created_at is a string ("2024-06-02T14:00:00Z"). Convert to a real
# timestamp (unlocks date math), then derive an integer date partition key.
# ============================================================
fact_events = fact_events \
    .withColumn(
        "created_ts",
        to_timestamp(col("created_at"))      # string -> timestamp; ISO format auto-detected
    ) \
    .withColumn(
        "date_key",
        date_format(col("created_ts"), "yyyyMMdd").cast("int")
        # date_format -> "20240602" (string); .cast("int") -> 20240602 (integer)
        # compact, sortable; DIM_TIME joins on it, BigQuery partitions on it
    )

# ============================================================
# INCREMENT 4 — FINALIZE FACT_EVENTS
# Drop the raw created_at string (created_ts replaces it), lock final
# column order to match the target schema, validate event grain holds.
# ============================================================
fact_events = fact_events.select(
    "event_id",
    "event_type",
    "actor_id",
    "actor_login",
    "repo_id",
    "repo_name",
    "org_id",
    "org_login",
    "action",
    "created_ts",   # the parsed timestamp (raw created_at dropped)
    "date_key",     # integer partition key
)

# --- Validate event GRAIN: count must still equal the filtered count (~3.02M).
# If this number CHANGED, rows fanned out somewhere = grain broke. It shouldn't
# have (no explode yet) — but we verify, not assume.
print("=== final FACT_EVENTS row count (expect ~3,021,593) ===")
print(fact_events.count())

print("=== final FACT_EVENTS schema ===")
fact_events.printSchema()

print("=== final FACT_EVENTS sample ===")
fact_events.show(5, truncate=False)



spark.stop()   # clean shutdown