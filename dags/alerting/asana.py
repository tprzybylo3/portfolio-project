"""
Failure alert -> Asana task (Phase 6, step 6.5).

Used as on_failure_callback in default_args, so Airflow calls it once per task
instance when the task fails for the LAST time (after all retries; retries go
through on_retry_callback, which we do not set). A timeout (execution_timeout)
is a failure too, so a hung task also creates an incident.

One-way and minimal on purpose: one POST /tasks per final failure. Not done yet
(next steps): closing the Asana task on the next success, and adding a comment
to an already-open task instead of opening a duplicate.

Rules for a callback:
- It must never raise: an exception here would hide the original failure in
  the logs. Every error is logged and swallowed.
- Secrets come from env vars (ASANA_TOKEN, ASANA_PROJECT_ID via .env ->
  docker-compose), never from the repo. Missing config = loud log line, no crash.
"""

import logging
import os

import requests

ASANA_TASKS_URL = "https://app.asana.com/api/1.0/tasks"
REQUEST_TIMEOUT_SECONDS = 10
MAX_ERROR_CHARS = 1500  # keep the Asana note readable; full trace is in the Airflow log

log = logging.getLogger(__name__)


def create_asana_incident(context: dict) -> None:
    token = os.environ.get("ASANA_TOKEN")
    project_id = os.environ.get("ASANA_PROJECT_ID")
    if not token or not project_id:
        log.error("ASANA_TOKEN / ASANA_PROJECT_ID not set -- failure NOT reported to Asana")
        return

    task_instance = context["task_instance"]
    pipeline_env = os.environ.get("PIPELINE_ENV", "dev")
    window_start = context["data_interval_start"]
    window_end = context["data_interval_end"]
    error_text = str(context.get("exception") or "no exception in context")[:MAX_ERROR_CHARS]

    title = f"[{pipeline_env}] {task_instance.dag_id}.{task_instance.task_id} failed (window {window_start:%Y-%m-%d})"
    notes = "\n".join([
        f"Environment: {pipeline_env}",
        f"DAG run: {context['run_id']}",
        f"Data window: {window_start.isoformat()} -> {window_end.isoformat()}",
        f"Attempts allowed: {task_instance.max_tries + 1} (all used)",
        f"Log: {task_instance.log_url}",
        "",
        "Error:",
        error_text,
    ])

    try:
        response = requests.post(
            ASANA_TASKS_URL,
            headers={"Authorization": f"Bearer {token}"},
            json={"data": {"name": title, "notes": notes, "projects": [project_id]}},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        created_task = response.json().get("data", {})
        log.info("Asana incident created: %s", created_task.get("permalink_url") or created_task.get("gid"))
    except Exception:  # noqa: BLE001 -- a callback must never raise, see module docstring
        log.exception("Could not create the Asana incident for %s", title)
