# Based on the official Apache Airflow image -- we do NOT install Airflow
# manually, the image already ships its full runtime and system deps.
FROM apache/airflow:2.9.0

# requirements.txt + pip stays here ON PURPOSE -- this installs packages
# INTO THE AIRFLOW IMAGE, not the project's own code. Airflow provider
# images historically expect this pattern (COPY + pip install), and mixing
# it with uv here would add no real benefit -- this is a different layer
# than your ingestion/dbt project code, which goes through uv instead
# (see cheat sheet section 18.4).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
