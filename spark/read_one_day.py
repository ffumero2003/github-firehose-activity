# spark/read_one_day.py
# Goal: read ONE day of June 2024 with our explicit schema and prove it parses clean.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import the contract we hand-wrote. Both files live in spark/, so a plain import works
# WHEN you run from inside the spark/ folder. We'll handle that in the run command below.
from schema import event_schema

# --- 1. Start Spark ---
# SparkSession is the entry point — it boots the local Spark engine on your machine.
# .master("local[*]") = run locally using ALL your CPU cores ([*] = all available).
# .appName(...) is just a label you'd see in the Spark UI.
spark = (
    SparkSession.builder
    .appName("gh-firehose-read-one-day")
    .master("local[*]")
    .getOrCreate()
)

# Quieten Spark's noisy INFO logs so you can actually see your output.
spark.sparkContext.setLogLevel("WARN")

# --- 2. Read the data WITH our schema ---
# .schema(event_schema) is THE key line: it tells Spark "do not infer, use my contract."
# The *.json.gz glob picks up all 24 hourly files for June 2; Spark decompresses .gz itself.
# Use the FULL path — Spark does not expand the ~ shortcut.
path = "/Users/felipefumero/projects/github-firehose-activity/data/2024-06-02-*.json.gz"

df = spark.read.schema(event_schema).json(path)

# --- 3. Three checks that prove the contract held ---

# (a) Schema check: confirms the nested shape (actor/repo/org structs, payload.commits array)
#     matches what we declared. This is the structure, printed as a tree.
print("=== printSchema ===")
df.printSchema()

# (b) Count: a real number means Spark read all the rows. Expect ~3-4 million for a full day.
#     NOTE: count() is an ACTION — this is the line that actually triggers the read/work.
print("=== count ===")
print(df.count())

# (c) Eyeball real values: pull a few fields out of the nested structs to confirm they parsed.
#     col("actor.login") reaches INTO the actor struct using dot notation.
#     truncate=False so long repo names/messages aren't cut off.
print("=== sample rows ===")
df.select(
    col("id"),
    col("type"),
    col("actor.login").alias("actor_login"),   # reach into nested struct
    col("repo.name").alias("repo_name"),
    col("payload.action").alias("action"),     # null on PushEvents — expected
).show(10, truncate=False)

# (d) Confirm the commits ARRAY survived: show a PushEvent that actually has commits.
print("=== a push WITH commits ===")
df.filter((col("type") == "PushEvent") & (col("payload.commits").isNotNull())) \
  .select(col("id"), col("payload.commits")) \
  .show(3, truncate=False)

spark.stop()  # clean shutdown