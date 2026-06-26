# airflow/dags/github_firehose_dag.py
# Airflow DAG: orchestrates the full GitHub Firehose pipeline.
# Flow: download → spark transform/write → dbt run → dbt test.
# Each step must succeed before the next runs; any failure stops the DAG.

from airflow import DAG
from airflow.operators.bash import BashOperator   # runs a shell command as a task
from datetime import datetime, timedelta

# --- Default settings applied to every task in this DAG ---
default_args = {
    "owner": "felipe",
    "retries": 1,                          # if a task fails, retry once before giving up
    "retry_delay": timedelta(minutes=2),   # wait 2 min between retries
    "depends_on_past": False,              # a run doesn't wait on the previous day's run
}

# --- The DAG itself ---
with DAG(
    dag_id="github_firehose_pipeline",
    default_args=default_args,
    description="Daily GitHub Archive → Spark → BigQuery → dbt pipeline",
    schedule_interval="@daily",            # run once per day
    start_date=datetime(2024, 6, 1),
    catchup=False,                         # don't back-fill old dates on first run
    tags=["portfolio", "spark", "dbt", "bigquery"],
) as dag:

    PROJECT = "/Users/felipefumero/projects/github-firehose-activity"

    # TASK 1 — download one day's hourly files (idempotent: skips files already on disk)
    download = BashOperator(
        task_id="download_gharchive",
        bash_command=f"cd {PROJECT} && python ingest/download.py {{{{ ds_nodash }}}}",
        # {{ ds_nodash }} = Airflow's run date as YYYYMMDD — passed to the script so each
        # run processes ITS date. This is what makes the pipeline incremental/daily.
    )

    # TASK 2 — Spark: transform raw JSON and write facts + dims to BigQuery
    spark_transform = BashOperator(
        task_id="spark_transform_write",
        bash_command=f"cd {PROJECT}/spark && python write_to_bigquery.py",
    )

    # TASK 3 — dbt: build the marts on top of BigQuery
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT}/dbt_gh && dbt run",
    )

    # TASK 4 — dbt: run the data-quality tests (the gate)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT}/dbt_gh && dbt test",
    )

    # --- DEPENDENCIES: the order Airflow runs them ---
    # >> means "must finish successfully before". This chain is the whole pipeline.
    download >> spark_transform >> dbt_run >> dbt_test