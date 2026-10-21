# Official Airflow image, Python version pinned EXPLICITLY (the bare
# 2.9.0 tag does not say which Python it ships). 3.12 matches the
# project's requires-python and the constraints file below.
FROM apache/airflow:2.9.0-python3.12

# --- Layer 1: packages INTO Airflow's own environment ---
# Installed with Airflow's official constraints file: a list of versions
# tested together for this exact Airflow + Python combo. Providers in
# requirements.txt stay unpinned on purpose -- the constraints file pins
# them. Re-stating apache-airflow==${AIRFLOW_VERSION} stops pip from
# silently upgrading/downgrading Airflow itself while resolving.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

# --- Layer 2: dbt in its OWN virtualenv ---
# dbt-core and Airflow pin conflicting versions of shared libraries
# (well-known dependency clash), so dbt never shares Airflow's env.
# Tasks call it by absolute path: /opt/airflow/dbt_venv/bin/dbt.
# Versions match pyproject.toml so local runs == orchestrated runs.
ARG DBT_CORE_VERSION=1.12.5
ARG DBT_SNOWFLAKE_VERSION=1.12.1
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
         "dbt-core==${DBT_CORE_VERSION}" \
         "dbt-snowflake==${DBT_SNOWFLAKE_VERSION}"
