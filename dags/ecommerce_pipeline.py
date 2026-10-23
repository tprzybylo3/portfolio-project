"""
ecommerce_pipeline -- Phase 6 orchestration of the portfolio pipeline.

    [ingest]  extract_postgres ──> has_new_rows ──> validate_raw ──> [spark_merge] merge_dimensions
                                                                                 ──> merge_facts
                                                                   ──> [dbt] dbt_source_freshness ──> dbt_build
    [ingest]  extract_github   (parallel branch, lands in raw only)

    has_new_rows : TaskFlow short-circuit -- 0 new rows => everything downstream
                   is SKIPPED (no JVM, later no Snowflake credits), not failed.
    validate_raw : quality gate -- any quarantined row => task fails => no MERGE.
    dbt_source_freshness : warn-only (sources have warn_after, no error_after),
                   so a stale source is logged, never blocks dbt_build.

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
- dbt runs from its own virtualenv in the Airflow image (dbt and Airflow pin
  conflicting libraries). Profile = the env-var-only template from the repo,
  mounted as profiles.yml; the Snowflake key is a read-only mount.
- Two mocked environments (CLAUDE.md decision 10): PIPELINE_ENV=dev|prod picks
  the dbt target (dbt_dev_* vs dbt_prod_* schemas). One codebase, config only.
- TaskGroups (ingest / spark_merge / dbt) are visual grouping; dependencies are
  still set task-to-task, so extract_github does not gate the pipeline.
- Retries with exponential backoff and per-stage execution_timeout come from
  config/<env>.yaml (dev retries short, prod longer). Retries are for TRANSIENT
  errors (network, API, Iceberg commit conflict); tasks whose failure is
  deterministic (validate_raw gate, has_new_rows) get retries=0. Every task is
  idempotent, so a retry is safe. After the last failed attempt
  on_failure_callback opens an Asana task (dags/alerting/asana.py).
"""

import os
import re
from datetime import timedelta

import pendulum
import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount

from alerting.asana import create_asana_incident  # dags/ is on sys.path

SCRIPTS_DIR = "/opt/airflow/scripts"  # bind-mounted from ./scripts (docker-compose.yml)
HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "")  # repo path on the HOST, for DockerOperator mounts
SPARK_IMAGE = "portfolio-spark:latest"
PIPELINE_ENV = os.environ.get("PIPELINE_ENV", "dev")  # dev | prod -> dbt target (decision 10)
DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"  # separate venv, see airflow-docker/Dockerfile
DBT_PROJECT_DIR = "/opt/airflow/dbt_portfolio"  # bind-mounted from ./dbt_portfolio
# Per-environment settings (schedule, dbt flags) from config/<PIPELINE_ENV>.yaml;
# a missing file fails the DAG import instead of silently using defaults.
with open(f"/opt/airflow/pipeline_config/{PIPELINE_ENV}.yaml") as config_file:  # mounted from ./config
    ENV_CONFIG = yaml.safe_load(config_file)
DBT_BUILD_FLAGS = ENV_CONFIG["dbt"]["build_flags"]
RETRY_CONFIG = ENV_CONFIG["retries"]
TIMEOUT_MINUTES = ENV_CONFIG["execution_timeout_minutes"]  # per stage: extract / validate / spark_merge / dbt
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
        execution_timeout=timedelta(minutes=TIMEOUT_MINUTES["spark_merge"]),
        command=f"python3 /opt/spark-scripts/{script_name}",
        mounts=[
            Mount(source=f"{HOST_PROJECT_DIR}/scripts", target="/opt/spark-scripts", type="bind", read_only=True),
        ],
        private_environment=AWS_ENVIRONMENT,
        auto_remove="success",  # keep the container after a failure, for debugging
        mount_tmp_dir=False,  # Docker Desktop cannot bind Airflow's temp dir from inside a container
        docker_url="unix://var/run/docker.sock",
    )


@task.short_circuit(retries=0)  # parses a value already in XCom: a retry gives the same answer
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
    description=f"Postgres + GitHub -> S3 raw -> validation -> Iceberg (Spark MERGE) -> dbt [{PIPELINE_ENV}]",
    schedule=ENV_CONFIG["schedule"],
    # Seed data in postgres-source ends mid-September: start_date must be before
    # the days used for tests/backfill, or Airflow creates runs with NO tasks.
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,  # backfill is done explicitly (step 6.6), not on DAG enable
    max_active_runs=1,  # runs go in order: SCD2 versions and logs stay readable
    default_args={
        "depends_on_past": True,  # day N waits for a successful (or skipped) day N-1
        # Step 6.5: retry transient errors with growing pauses, then alert once.
        "retries": RETRY_CONFIG["count"],
        "retry_delay": timedelta(seconds=RETRY_CONFIG["delay_seconds"]),
        "retry_exponential_backoff": True,  # pause doubles per attempt (+ deterministic jitter)
        "max_retry_delay": timedelta(seconds=RETRY_CONFIG["max_delay_seconds"]),
        "on_failure_callback": create_asana_incident,  # only after the LAST attempt
    },
    tags=["portfolio", "phase-6", f"env:{PIPELINE_ENV}"],
) as dag:
    with TaskGroup("ingest") as ingest:
        extract_postgres = BashOperator(
            task_id="extract_postgres",
            bash_command=f"python {SCRIPTS_DIR}/extract.py --start '{WINDOW_START_WITH_LOOKBACK}' --end '{WINDOW_END}'",
            execution_timeout=timedelta(minutes=TIMEOUT_MINUTES["extract"]),
        )

        extract_github = BashOperator(
            task_id="extract_github",
            bash_command=f"python {SCRIPTS_DIR}/github_extract.py --start '{WINDOW_START}' --end '{WINDOW_END}'",
            execution_timeout=timedelta(minutes=TIMEOUT_MINUTES["extract"]),
        )

    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=f"python {SCRIPTS_DIR}/validate.py",
        execution_timeout=timedelta(minutes=TIMEOUT_MINUTES["validate"]),
        retries=0,  # quarantined rows = deterministic failure: a retry would fail the same way
    )

    with TaskGroup("spark_merge") as spark_merge:
        merge_dimensions = spark_merge_task("merge_dimensions", "merge_dimensions.py")
        merge_facts = spark_merge_task("merge_facts", "merge_facts.py")

    with TaskGroup("dbt") as dbt:
        # Are the Iceberg sources recent? Exit code is non-zero only on error_after
        # (none configured) or a connection error, so this only warns.
        dbt_source_freshness = BashOperator(
            task_id="dbt_source_freshness",
            bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} source freshness --target {PIPELINE_ENV}",
            execution_timeout=timedelta(minutes=TIMEOUT_MINUTES["dbt"]),
        )

        # dbt build = unit tests + run + data tests in dependency order (a failing
        # test stops the models downstream of it). --target follows the environment.
        dbt_build = BashOperator(
            task_id="dbt_build",
            bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} build --target {PIPELINE_ENV} {DBT_BUILD_FLAGS}",
            execution_timeout=timedelta(minutes=TIMEOUT_MINUTES["dbt"]),
        )

    # extract_postgres.output = the last line extract.py printed (XCom), e.g.
    # "Done. 26 rows extracted across 6 tables." -- see has_new_rows above.
    new_rows_gate = has_new_rows(extract_postgres.output)

    # ------------- Pipeline: ALL dependencies in one place, in flow order -------------
    # Roots (no upstream, start in parallel): extract_postgres, extract_github.
    # extract_github is an independent branch with nothing downstream: GitHub data
    # lands in raw only (merge_*.py handle the Postgres tables), so a GitHub API
    # outage must not block the Postgres pipeline.
    #
    # extract_postgres >> new_rows_gate is also implied by
    # has_new_rows(extract_postgres.output) above (passing a task's output creates
    # the dependency); stated here explicitly so the whole flow reads in one line.
    # Dimensions before facts: sequential on purpose (one JVM at a time on a laptop).
    extract_postgres >> new_rows_gate >> validate_raw >> merge_dimensions >> merge_facts >> dbt_source_freshness >> dbt_build
