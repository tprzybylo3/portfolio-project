# ADR-006: Extraction window — data interval with a lookback instead of watermark state

## Status
Accepted (decision). Implemented in Phase 6, step 6.3 (refactor of `extract.py` and
`github_extract.py`), verified in step 6.6 (backfill test).

## Context

Up to Phase 5 both extraction scripts kept a watermark in state: local files
`scripts/state/watermark_state.json` and `github_watermark_state.json`. Every run read the
last value, fetched newer rows and overwrote the state with `MAX(updated_at)` from the
fetched batch. The docstring of `extract.py` announced that in Phase 6 the state would move
to Airflow Variables.

Airflow introduces two requirements that a watermark in state does not meet well:

1. **Backfill and re-runs.** A moving watermark has already moved forward, so reprocessing
   October 3 requires manually rewinding the state and restoring it afterwards. A mistake
   in that sequence loses or duplicates data.
2. **Auditability.** A watermark is a single overwritten value. There is no record of which
   run processed which range.

In addition, the existing watermark has a gap independent of orchestration: in Postgres
`now()` (and `DEFAULT now()` on `updated_at`) returns the **start** time of the
transaction, not the commit time. A transaction started before the read and committed
after it gets an `updated_at` lower than the new watermark, and the row is never fetched.

## Decision

1. **Extraction window = the Airflow run's data interval.** The script gets
   `--start`/`--end` from templates (`data_interval_start`/`data_interval_end`) and filters
   `updated_at >= start AND updated_at < end` (for `order_status_history`: `changed_at`).
   The script knows nothing about Airflow and can be run manually with any range.
2. **A lookback at the start of the window**, added in the task:
   `data_interval_start - 10 min`. The lookback must exceed the longest writing transaction
   at the source. The end of the window stays sharp.
3. **No watermark state.** `scripts/state/*.json` and Airflow Variables are not used for
   extraction. The history of runs and their windows is kept by the Airflow metadata
   database (`dag_run`).
4. **Duplicates from the lookback and from re-runs are absorbed by the layer below**, with no
   code changes: `merge_facts.py`/`merge_dimensions.py` deduplicate the batch by PK (latest
   `updated_at` wins) before `MERGE`. Raw in S3 stays append-only with `run_id` (ADR-002).
5. **GitHub API, per entity** (the API has no uniform "from–to" filter):
   - `issues`: `since = start` server-side, `updated_at < end` client-side.
   - `pull_requests`: sort by `updated desc`, stop at the first element `< start`, filter
     `< end` client-side.
   - `repositories`, `contributors`: no window, a full snapshot on every run (no
     `updated_at`, or a single object).

## Alternatives considered

**A: Watermark in Airflow Variables** (the plan from the `extract.py` docstring).
Rejected: it moves the problem from a file into the Airflow database but does not solve it.
A backfill still requires rewinding the state by hand, and parallel runs would race for a
single variable.

**B: Watermark as `MAX(updated_at)` from the target table** (Iceberg/Snowflake, the variant
considered in phase 1). Self-healing (no separate state to lose), but backfilling a
specific day is still impossible without overriding the logic, and extraction depends on
the availability of the target layer.

**C: Log-based CDC (Debezium reading the WAL).** The only solution without windows and
lookbacks, also capturing intermediate values and DELETEs. Rejected for this project:
separate infrastructure (Kafka/Debezium), disproportionate to the goal of a portfolio. It
is described in the author's study notes as the target answer to "how would you fix this
completely".

## Consequences

**Positive:**
- Every run is idempotent: the same window yields the same rows from the source, and
  `MERGE` with deduplication yields the same Silver state.
- Backfilling a range is `airflow dags backfill -s ... -e ...` or Clear in the UI, without
  editing state by hand.
- The "late commit" gap is closed by the lookback.
- Fewer moving parts: the state file that could be lost or corrupted is gone.

**Negative / to remember:**
- **The initial load** requires a deliberate decision: a DAG `start_date` covering the
  oldest data (`catchup=True`, many runs) or a one-off run with `--start` far in the past.
- **Intermediate values are still lost** (OLTP overwrites in place). Windows do not fix
  this; it is a limitation of CDC based on table state.
- **Backfilling an API source does not reproduce the past.** GitHub returns the current
  state of objects, so an old run fetches only those whose *current* `updated_at` falls
  into its window. Idempotent (no duplicates), but not historically reproducible.
  `contributors`/`repositories` in a backfill return the state as of the day of the run.
- The lookback increases raw volume by ~10 minutes of duplicates per run. Negligible at
  this scale.
- Run order: `max_active_runs=1`, so that a backfill proceeds chronologically.

## Related

- ADR-002 (append-only raw + idempotent `MERGE`, on which this decision relies)
- README, question 2 (the backfill test of this window strategy)
- Author's study notes: CDC, MVCC, data interval, lookback, backfill (not part of the
  repository)
