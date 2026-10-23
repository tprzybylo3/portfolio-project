"""
source_simulator -- one day of synthetic activity in postgres-source (step 6.7).

NOT part of the pipeline: it plays the role of the source APPLICATION (in a
company that is someone else's system, the pipeline only reads from it). It
runs scripts/seed.py --mode daily, which appends today's events -- new
customers, attribute changes (-> new SCD2 versions), sometimes a price change,
new orders with items/payments/status history, lifecycle progress of open
orders. History is never modified, so backfills of past days stay valid.

Idempotent per day (the script checks whether today's batch exists), so a
retry, a manual trigger or a second run the same day does nothing.

Schedule from config/<PIPELINE_ENV>.yaml (source_simulator.schedule): dev runs
it at 23:30 UTC, half an hour before its own pipeline run at 00:00, whose
window [D, D+1) then contains the new rows; prod has no schedule (null) --
both deployments read the same postgres-source, one generator is enough.
"""

import os
from datetime import timedelta

import pendulum
import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator

from alerting.asana import create_asana_incident  # dags/ is on sys.path

SCRIPTS_DIR = "/opt/airflow/scripts"  # bind-mounted from ./scripts (docker-compose.yml)
PIPELINE_ENV = os.environ.get("PIPELINE_ENV", "dev")
with open(f"/opt/airflow/pipeline_config/{PIPELINE_ENV}.yaml") as config_file:  # mounted from ./config
    ENV_CONFIG = yaml.safe_load(config_file)
RETRY_CONFIG = ENV_CONFIG["retries"]

with DAG(
    dag_id="source_simulator",
    description=f"Synthetic daily activity in postgres-source (plays the source application) [{PIPELINE_ENV}]",
    schedule=ENV_CONFIG["source_simulator"]["schedule"],  # None (null in YAML) = manual only
    start_date=pendulum.datetime(2026, 10, 23, tz="UTC"),
    catchup=False,  # events are stamped with NOW: catching up past days would make no sense
    max_active_runs=1,
    default_args={
        "retries": RETRY_CONFIG["count"],  # safe: one transaction + idempotent per day
        "retry_delay": timedelta(seconds=RETRY_CONFIG["delay_seconds"]),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(seconds=RETRY_CONFIG["max_delay_seconds"]),
        "on_failure_callback": create_asana_incident,
    },
    tags=["portfolio", "phase-6", "source", f"env:{PIPELINE_ENV}"],
) as dag:
    generate_daily_activity = BashOperator(
        task_id="generate_daily_activity",
        bash_command=f"python {SCRIPTS_DIR}/seed.py --mode daily",
        execution_timeout=timedelta(minutes=5),  # a normal run takes seconds
    )
