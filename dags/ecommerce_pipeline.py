"""
ecommerce_pipeline -- Phase 6 orchestration of the portfolio pipeline.

    extract_postgres ──> has_new_rows ──> validate_raw ──> merge_dimensions ──> merge_facts
    extract_github   (parallel branch, lands in raw only)

    has_new_rows : TaskFlow short-circuit -- 0 new rows => everything downstream
                   is SKIPPED (no JVM, later no Snowflake credits), not failed.
    validate_raw : quality gate -- any quarantined row => task fails => no MERGE.

Window per run (ADR-006): the run's data interval [data_interval_start,
data_interval_end), with a LOOKBACK subtracted from the start for the Postgres
extract (now() in Postgres = transaction START time, so late commits would
otherwise be missed). Overlap duplicates are removed by the dedup-before-MERGE
in merge_*.py. No watermark state anywhere -- re-running or backfilling a run
re-extracts exactly its window.

Design notes:
- Scripts stay orchestrator-agnostic (CLAUDE.md decision 8): tasks call them
  with --start/--end; nothing in scripts/ imports Airflow.
- Spark does NOT run inside the Airflow image. merge_*.py run in short-lived
  containers from the existing Spark image (portfolio-spark:latest) via
  DockerOperator -- dependency isolation: JVM + Iceberg jars stay out of the
  Airflow image. Mount sources are HOST paths (HOST_PROJECT_DIR), because the
  host's Docker daemon resolves them, not this container.
- GitHub data lands in raw only; merge_*.py handle the Postgres tables, so
  extract_github is a parallel branch with nothing downstream (Phase 1 scope).
- TaskFlow (@task) only for logic that genuinely belongs to orchestration
  (here: "is there anything to process?"); data processing stays in scripts.
- depends_on_past=True: a run starts only if the same task succeeded (or was
  skipped) in the previous run -- a failed day blocks later days instead of
  leaving a silent gap. max_active_runs=1 alone only prevents overlap.
- Retries/backoff/alerting come in step 6.5, the dbt task in 6.4b.
"""

import os
import re

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

SCRIPTS_DIR = "/opt/airflow/scripts"  # bind-mounted from ./scripts (docker-compose.yml)
HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "")  # repo path on the HOST, for DockerOperator mounts
SPARK_IMAGE = "portfolio-spark:latest"
LOOKBACK_MINUTES = 10  # must exceed the longest write transaction in postgres-source (ADR-006)

WINDOW_START = "{{ data_interval_start.isoformat() }}"
WINDOW_START_WITH_LOOKBACK = f"{{{{ (data_interval_start - macros.timedelta(minutes={LOOKBACK_MINUTES})).isoformat() }}}}"
WINDOW_END = "{{ data_interval_end.isoformat() }}"

# Same variables the Airflow containers get from .env; passed to the Spark
# containers as private_environment so they are not rendered in the UI.
AWS_ENVIRONMENT = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_REGION": os.environ.get("AWS_REGION", "eu-central-1"),  # read by the Java SDK inside Iceberg
    "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"),  # read by boto3
}


def spark_merge_task(task_id: str, script_name: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=f"python3 /opt/spark-scripts/{script_name}",
        mounts=[
            Mount(source=f"{HOST_PROJECT_DIR}/scripts", target="/opt/spark-scripts", type="bind", read_only=True),
        ],
        private_environment=AWS_ENVIRONMENT,
        auto_remove="success",  # keep the container after a failure, for debugging
        mount_tmp_dir=False,  # Docker Desktop cannot bind Airflow's temp dir from inside a container
        docker_url="unix://var/run/docker.sock",
    )


@task.short_circuit
def has_new_rows(extract_summary: str) -> bool:
    """BashOperator pushes the LAST line of the script's output to XCom:
    extract.py ends with "Done. <N> rows extracted across 6 tables."
    Returning False skips every downstream task (shown as skipped, not failed).
    If the line cannot be parsed, do not skip -- better to run than to miss data."""
    match = re.search(r"Done\. (\d+) rows extracted", extract_summary or "")
    if match is None:
        print(f"Could not parse extract summary {extract_summary!r} -- not skipping")
        return True
    new_rows = int(match.group(1))
    print(f"extract_postgres landed {new_rows} rows")
    return new_rows > 0


with DAG(
    dag_id="ecommerce_pipeline",
    description="Postgres + GitHub -> S3 raw -> validation -> Iceberg (Spark MERGE)",
    schedule="@daily",
    # Seed data in postgres-source ends mid-September: start_date must be before
    # the days used for tests/backfill, or Airflow creates runs with NO tasks.
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,  # backfill is done explicitly (step 6.6), not on DAG enable
    max_active_runs=1,  # runs go in order: SCD2 versions and logs stay readable
    default_args={
        "depends_on_past": True,  # day N waits for a successful (or skipped) day N-1
    },
    tags=["portfolio", "phase-6"],
) as dag:
    extract_postgres = BashOperator(
        task_id="extract_postgres",
        bash_command=f"python {SCRIPTS_DIR}/extract.py --start '{WINDOW_START_WITH_LOOKBACK}' --end '{WINDOW_END}'",
    )

    extract_github = BashOperator(
        task_id="extract_github",
        bash_command=f"python {SCRIPTS_DIR}/github_extract.py --start '{WINDOW_START}' --end '{WINDOW_END}'",
    )

    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=f"python {SCRIPTS_DIR}/validate.py",
    )

    merge_dimensions = spark_merge_task("merge_dimensions", "merge_dimensions.py")
    merge_facts = spark_merge_task("merge_facts", "merge_facts.py")

    # Passing extract_postgres.output both feeds the XCom value in and creates
    # the dependency extract_postgres >> new_rows_gate.
    new_rows_gate = has_new_rows(extract_postgres.output)

    # Dimensions before facts: sequential on purpose (one JVM at a time on a laptop).
    new_rows_gate >> validate_raw >> merge_dimensions >> merge_facts
