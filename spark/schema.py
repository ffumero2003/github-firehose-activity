# spark/schema.py
# The explicit schema = our "contract" for the raw GH Archive JSON.
# We declare the exact shape so Spark NEVER infers it (faster + safer).

from pyspark.sql.types import (
    StructType,   # a container for a group of fields (an "object")
    StructField,  # one named field: (name, type, nullable?)
    StringType,   # text
    LongType,     # 64-bit integer — needed because GitHub IDs are huge
    ArrayType,    # a list/array — used for the commits array
)

# --- Innermost piece first: ONE commit ---
# In the raw JSON, payload.commits is an array of objects like {sha, message, ...}.
# We only KEEP sha + message (our locked field list), ignoring author/url/distinct.
# Spark simply drops any JSON field we don't declare — that's a feature: the schema
# is also a FILTER. We're saying "of all the commit fields, these two are the contract."
commit_schema = StructType([
    StructField("sha", StringType(), True),       # commit hash, text
    StructField("message", StringType(), True),   # commit message, text
])

# --- payload: holds the type-specific fields ---
# action  -> only present on PR/Issues/Watch events (null on Push) -> nullable
# commits -> only present on (some) Push events, an ARRAY of commit_schema -> nullable
# ArrayType(commit_schema) means "a list whose elements have the shape above".
payload_schema = StructType([
    StructField("action", StringType(), True),            # nullable: not on every event
    StructField("commits", ArrayType(commit_schema), True) # nullable: absent on many pushes
])

# --- the small nested objects: actor, repo, org ---
# IMPORTANT: actor.id / repo.id / org.id are HUGE numbers (e.g. 1120555592).
# Those overflow a normal 32-bit IntegerType, so we use LongType (64-bit).
# This is a real correctness decision, not a style choice — IntegerType would
# silently corrupt or null these IDs.
actor_schema = StructType([
    StructField("id", LongType(), True),       # big int -> LongType
    StructField("login", StringType(), True),  # username, text
])

repo_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),   # "owner/repo" string
])

org_schema = StructType([
    StructField("id", LongType(), True),
    StructField("login", StringType(), True),
])

# --- the top-level event ---
# This is the full row shape for one line of the .json.gz file.
event_schema = StructType([
    StructField("id", StringType(), True),          # event id came QUOTED in JSON -> string
    StructField("type", StringType(), True),        # "PushEvent", etc.
    StructField("created_at", StringType(), True),  # keep as STRING now; parse to timestamp in transform
    StructField("actor", actor_schema, True),       # nested object
    StructField("repo", repo_schema, True),         # nested object
    StructField("org", org_schema, True),           # nested object, often null -> nullable
    StructField("payload", payload_schema, True),   # nested object holding action + commits
])