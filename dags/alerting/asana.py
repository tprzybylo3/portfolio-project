"""
Failure alerts -> Asana incidents (step 6.5, v2).

- report_task_failure: on_failure_callback (default_args). Called once per task
  instance after its LAST attempt (retries do not call it); a timeout is a failure too.
  One open incident per [env] dag.task: the first failure creates an Asana task, every
  further failure of the same task adds a comment to it instead of a duplicate.
- close_recovered_incidents: DAG-level on_success_callback. A successful run completes
  every open incident of that DAG and environment, with a "recovered" comment.

The incident key is the stable title prefix "[env] dag_id.task_id"; the data window
goes into the description, so failures on different days land in the same incident.
The description carries the end of the task log (from the last traceback), because for
a BashOperator the exception is only "command returned a non-zero exit code".

A callback must never raise (it would hide the original failure): every error is logged
and swallowed. Secrets come from env vars (ASANA_TOKEN, ASANA_PROJECT_ID via .env ->
docker-compose); missing config = loud log line, no crash.
"""

import glob
import logging
import os
import re

import requests

ASANA_API = "https://app.asana.com/api/1.0"
REQUEST_TIMEOUT_SECONDS = 10
MAX_LOG_LINES = 25
MAX_ERROR_CHARS = 2500  # keep the Asana text readable; the full log is behind the link
LOG_FOLDER = os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")
SUBPROCESS_PREFIX = re.compile(r"^\[[^\]]*\] \{subprocess\.py:\d+\} \w+ - ?")  # "[ts] {subprocess.py:93} INFO - "

log = logging.getLogger(__name__)


def _asana_config() -> tuple[str, str] | None:
    token, project_id = os.environ.get("ASANA_TOKEN"), os.environ.get("ASANA_PROJECT_ID")
    if not token or not project_id:
        log.error("ASANA_TOKEN / ASANA_PROJECT_ID not set -- Asana NOT updated")
        return None
    return token, project_id


def _call(token: str, method: str, path: str, **kwargs) -> dict:
    response = requests.request(
        method,
        f"{ASANA_API}{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT_SECONDS,
        **kwargs,
    )
    response.raise_for_status()
    return response.json().get("data", {})


def _open_tasks_with_prefix(token: str, project_id: str, prefix: str) -> list[dict]:
    # completed_since=now returns only incomplete tasks
    open_tasks = _call(
        token, "GET", f"/projects/{project_id}/tasks",
        params={"completed_since": "now", "opt_fields": "name,permalink_url", "limit": 100},
    )
    return [task for task in open_tasks if task.get("name", "").startswith(prefix)]


def _log_tail(task_instance) -> str:
    """The real cause from the newest attempt's log.

    For a BashOperator the script's own output is logged on lines from subprocess.py;
    the LAST traceback in the file is Airflow's wrapper ("Bash command failed"), so the
    script's output is searched first. Other operators: from the last traceback.
    """
    pattern = os.path.join(
        LOG_FOLDER, f"dag_id={task_instance.dag_id}", f"run_id={task_instance.run_id}",
        f"task_id={task_instance.task_id}", "attempt=*.log",
    )
    attempt_logs = sorted(glob.glob(pattern), key=os.path.getmtime)
    if not attempt_logs:
        return "(task log not found)"
    with open(attempt_logs[-1], encoding="utf-8", errors="replace") as log_file:
        lines = [line.rstrip() for line in log_file if line.strip()]
    script_output = [SUBPROCESS_PREFIX.sub("", line) for line in lines if "{subprocess.py" in line]
    candidate_lines = script_output or lines
    traceback_starts = [i for i, line in enumerate(candidate_lines) if "Traceback (most recent call last)" in line]
    tail = candidate_lines[traceback_starts[-1]:] if traceback_starts else candidate_lines
    return "\n".join(tail[-MAX_LOG_LINES:])[-MAX_ERROR_CHARS:]


def report_task_failure(context: dict) -> None:
    config = _asana_config()
    if config is None:
        return
    token, project_id = config
    task_instance = context["task_instance"]
    pipeline_env = os.environ.get("PIPELINE_ENV", "dev")
    incident_key = f"[{pipeline_env}] {task_instance.dag_id}.{task_instance.task_id}"
    window_start, window_end = context["data_interval_start"], context["data_interval_end"]

    try:
        failure_details = "\n".join([
            f"DAG run: {context['run_id']}",
            f"Data window: {window_start.isoformat()} -> {window_end.isoformat()}",
            f"Attempts: {task_instance.max_tries + 1} (all used)",
            f"Exception: {str(context.get('exception') or 'none in context')[:300]}",
            f"Log: {task_instance.log_url}",
            "",
            "End of the task log:",
            _log_tail(task_instance),
        ])
        already_open = _open_tasks_with_prefix(token, project_id, incident_key)
        if already_open:
            incident = already_open[0]
            _call(token, "POST", f"/tasks/{incident['gid']}/stories",
                  json={"data": {"text": f"Failed again.\n\n{failure_details}"}})
            log.info("Asana incident updated: %s", incident.get("permalink_url") or incident["gid"])
        else:
            incident = _call(token, "POST", "/tasks", json={"data": {
                "name": f"{incident_key} failing",
                "notes": f"Environment: {pipeline_env}\n{failure_details}",
                "projects": [project_id],
            }})
            log.info("Asana incident created: %s", incident.get("permalink_url") or incident.get("gid"))
    except Exception:  # noqa: BLE001 -- a callback must never raise, see module docstring
        log.exception("Could not report %s to Asana", incident_key)


def close_recovered_incidents(context: dict) -> None:
    config = _asana_config()
    if config is None:
        return
    token, project_id = config
    pipeline_env = os.environ.get("PIPELINE_ENV", "dev")
    dag_prefix = f"[{pipeline_env}] {context['dag'].dag_id}."

    try:
        for incident in _open_tasks_with_prefix(token, project_id, dag_prefix):
            _call(token, "POST", f"/tasks/{incident['gid']}/stories",
                  json={"data": {"text": f"Recovered: DAG run {context['run_id']} succeeded."}})
            _call(token, "PUT", f"/tasks/{incident['gid']}", json={"data": {"completed": True}})
            log.info("Asana incident closed: %s", incident.get("permalink_url") or incident["gid"])
    except Exception:  # noqa: BLE001 -- a callback must never raise, see module docstring
        log.exception("Could not close Asana incidents for %s", dag_prefix)
