# GitHub Firehose — Developer Activity Intelligence

An end-to-end data pipeline that ingests **100M+ nested JSON events** from the public GitHub Archive firehose, transforms them with **PySpark** into a **BigQuery star schema**, models analytical marts with **dbt**, enforces data quality with **dbt tests**, and visualizes the result in a **Looker Studio** dashboard — orchestrated daily with **Airflow**.

---

## Why this project

GitHub Archive emits every public GitHub event as newline-delimited JSON, ~4 million events **per day**. The data is both **large** (100M+ events across a month) and **deeply nested** (arrays of objects inside event payloads).

This is the rare workload where **pandas genuinely cannot do the job** — not because of one limitation, but two at once:

- **Volume:** a full month is too large to hold and process in memory on a single machine.
- **Shape:** commits arrive as a nested array inside each push event (`payload.commits = [{sha, message}, ...]`), which requires a real distributed _explode_ to flatten into rows.

So Spark isn't a stylistic choice here — it's the right tool for distributed processing of raw, nested, high-volume data before it lands in the warehouse.

---

## Architecture

```
GitHub Archive (raw nested .json.gz, hourly)
        │
        ▼
   ingest/  ── idempotent download, validates each file
        │
        ▼
   PySpark  ── explicit schema, filter early, flatten structs,
   (spark/)    explode commits array (grain change), dedup dims
        │
        ▼
   BigQuery ── star schema: 2 facts + 4 dimensions (direct Storage Write API)
        │
        ▼
   dbt      ── 5 analytical marts + 11 data-quality tests
   (dbt_gh/)
        │
        ▼
   Looker Studio ── dashboard (scorecards + charts)

   Airflow (airflow/) orchestrates the whole flow daily:
   download → spark → dbt run → dbt test  (stops on any failure)
```

### Star schema

Two fact tables at **different grains** (a deliberate modeling decision):

- **`fact_events`** — _event grain_: one row per GitHub event (~3.0M rows/day).
- **`fact_commits`** — _commit grain_: one row per commit, produced by exploding the nested `payload.commits` array (~3.4M rows/day; commits > events because multi-commit pushes fan out).

Four dimensions, deduplicated lookups (`dropDuplicates` on the key):

- **`dim_actor`** (actor_id → login) · **`dim_repo`** (repo_id → name, derived owner) · **`dim_org`** (org_id → login) · **`dim_time`** (date_key → date parts)

---

## Key engineering decisions

**Explicit schema, no inference.** A hand-written `StructType` declares the exact shape of the JSON, so Spark validates against a contract instead of scanning to guess types (slow and fragile on nested data). It also acts as a filter — undeclared fields are dropped. IDs use `LongType` because GitHub IDs (e.g. `1120555592`) overflow 32-bit integers.

**Window chosen for payload completeness, not recency.** During the initial smoke test, recent (2026) PushEvents were found to have dropped their inline `commits` array entirely. The data window was therefore locked to **June 2024**, where commit payloads are complete — chosen on what the data actually contains, verified by hand.

**Filter early.** Events are filtered to the four target types (Push, PullRequest, Issues, Watch) before any flatten or explode, so the expensive work only runs on kept rows (predicate pushdown).

**Explode changes grain.** `explode` (not `explode_outer`) unpacks the commits array — pushes with no commits correctly contribute zero commit rows, keeping `fact_commits` clean.

**UTC session timezone.** GH Archive files are UTC-named; Spark's default local-timezone interpretation shifted early-UTC events into the previous day. Setting `spark.sql.session.timeZone = UTC` aligns `date_key` with the source convention.

**Direct BigQuery write.** The Spark-BigQuery connector's indirect (GCS-staging) path required a second connector jar whose bundled Guava version collided with the BigQuery connector's. Switching to `writeMethod=direct` (BigQuery Storage Write API) streams rows straight in — no GCS staging, no version conflict.

**Hybrid ETL/ELT.** Spark does the heavy transform _before_ load (ETL: filter, flatten, explode); dbt does light analytical shaping _after_ load (ELT: joins, aggregations).

---

## Data quality

11 dbt tests run as an automated gate (`dbt test`):

- **`unique`** + **`not_null`** on every dimension primary key — verifies the `dropDuplicates` dedup worked.
- **`relationships`** on fact foreign keys — confirms every `actor_id` / `repo_id` in `fact_events` references a real row in its dimension (no orphans).

All 11 pass. Mart counts also reconcile exactly against the Spark layer (PushEvent 2,591,080, etc.), confirming end-to-end consistency.

---

## A note on data skew

The data exhibits significant skew — a small number of entities dominate. `github-actions[bot]` alone generated **813,076 events (~27% of all events)** in a single day. In a larger distributed job this concentrates work on a single partition; it would be addressed with key salting or by broadcasting the small dimension side of joins.

---

## Tech stack

| Layer         | Tool                                            |
| ------------- | ----------------------------------------------- |
| Ingestion     | Python (idempotent downloader, file validation) |
| Transform     | PySpark 3.5.3 (local; Dataproc-ready)           |
| Warehouse     | Google BigQuery (star schema)                   |
| Modeling      | dbt (5 marts, 11 tests)                         |
| Visualization | Looker Studio                                   |
| Orchestration | Apache Airflow (DAG, Docker)                    |

---

## Pipeline (Airflow DAG)

```
download_gharchive → spark_transform_write → dbt_run → dbt_test
```

Each task must succeed before the next runs; any failure halts the DAG. The download task is idempotent and keyed on the run date (`{{ ds_nodash }}`), so daily runs are incremental and safe to retry.

---

## How to run

```bash
# 1. environment
python3 -m venv venv && source venv/bin/activate
pip install pyspark==3.5.3 dbt-bigquery
export JAVA_HOME=$(/usr/libexec/java_home -v 17)   # Spark needs JDK 17

# 2. authenticate to BigQuery
gcloud auth application-default login

# 3. ingest one day
python ingest/download.py 20240602

# 4. transform + load to BigQuery
cd spark && python write_to_bigquery.py && cd ..

# 5. build marts + run tests
cd dbt_gh && dbt run && dbt test
```

---

## Honest limitations

- **Local Spark.** Runs on a single machine via `local[*]`. The transform code is cluster-ready; production would run it on Dataproc by swapping the master — the README notes this rather than paying the cluster setup cost for a portfolio build.
- **Fixed window.** Scoped to June 2024 for payload completeness; not a live, rolling feed.
- **Four event types.** Push / PullRequest / Issues / Watch only — the four with the richest, most distinct nested shapes. The other ~11 event types are filtered out.
- **Partitioning deferred.** Tables are written unpartitioned; date-key range-partitioning is a documented next step.
