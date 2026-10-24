# Portfolio Project — Build log

A chronological record: what we did, in what order, and which questions/answers came up
along the way. General theory with examples lives in `DE_Interview_Cheatsheet.docx` — this
is the history of THIS particular project.

---

## 1. Local environment (macOS)

**Done:** Homebrew → Git + SSH configuration for GitHub → `uv` → Docker Desktop.

**Questions/answers:**
- *What is Homebrew, what are the alternatives?* — a package manager for macOS (like `apt`); alternatives: MacPorts, Nix, manual installation, pyenv/nvm (the latter only per language, they don't replace Homebrew).
- *How do I configure SSH for GitHub?* — `ssh-keygen -t ed25519`, add it to ssh-agent/Keychain, paste the public key into GitHub → Settings → SSH keys, test with `ssh -T git@github.com`.
- *Why `ed25519` in the key name?* — the name of the algorithm (the elliptic curve Curve25519; the number 2²⁵⁵−19 is a mathematical CONSTANT, public for everyone — only the random private key generated ON that curve is unique).
- *How do I have two different SSH keys (e.g. for a router and for GitHub)?* — `-f ~/.ssh/id_ed25519_name` when generating + separate `Host` blocks in `~/.ssh/config`.
- *Stuck in `vim`* — `Esc` + `:q!` (exit without saving); recommended `nano` for simple edits instead of `vim`.
- *Why `uv`, not `requirements.txt`/`pip`?* — `uv` = a faster successor of `pip`, with a built-in lock file (`uv.lock`), and it also manages the Python version. `pip`/`requirements.txt` is still used where documentation/the ecosystem (e.g. Airflow) hasn't caught up yet — `uv pip install` is fully backward compatible.
- *Why Python 3.12, not the newest (3.14/3.15)?* — compatibility with PySpark (it lags in supporting new versions), maturity of wheels for native packages (psycopg2, pyarrow), "boring technology" as a signal in an interview.

## 2. Repo structure and first files

**Done:** `docker-compose.yml` (postgres-source, postgres-airflow, airflow-init/webserver/scheduler), `sql/ddl.sql` (6 tables), `airflow-docker/Dockerfile`+`requirements.txt`.

**Questions/answers:**
- *What is `docker-compose.yml`, is it a universal file?* — NO, it is tailored to a specific project; Docker packages the WHOLE environment (system, any language), not just Python dependencies.
- *YAML error: `depends_on` with a dash + a nested `condition`* — the two syntaxes (`depends_on: [- name]` vs `depends_on: {name: {condition: ...}}`) don't mix; fixed to the mapping form.
- *Error: the `airflow-init` command split into separate bash lines* — a YAML folded scalar (`>`) joins lines with a space ONLY at identical indentation; deeper indentation keeps real newlines. Fix: one flat line.
- *`airflow-init` fails on the SECOND start* — `airflow users create` is not idempotent (the user already exists); fix: `|| true` to swallow that specific, expected error.
- *Why `SERIAL` instead of `INT` for the PK?* — a surrogate key: `INTEGER` + `SEQUENCE` + `DEFAULT nextval(...)` in one, auto-increment without managing uniqueness by hand.
- *What does `CREATE INDEX` on `updated_at` do?* — a B-tree speeding up `WHERE updated_at > watermark`; the cost: it slows down writes.
- *What do `CREATE TRIGGER` + `CREATE OR REPLACE FUNCTION ... RETURNS TRIGGER` do?* — two separate objects: a function (the logic) + a trigger (attaching it to the `BEFORE UPDATE` event). `NEW`/`OLD` are variables injected automatically by Postgres when it is called as a trigger.
- *Why does `WHERE updated_at > watermark` also catch NEW rows, not just updates?* — `DEFAULT now()` also applies on INSERT, not only the trigger on UPDATE.
- *Does an UPDATE keep the old version of the row?* — NO (SCD Type 1, overwritten in place); history (SCD2) is deliberately a separate layer further down the pipeline (dbt snapshot/MERGE on Iceberg), not something the OLTP should do itself.
- *Do I need to run VACUUM manually?* — no, `autovacuum` is on by default and cleans up MVCC dead tuples in the background automatically.

## 3. AWS setup from scratch

**Done:** AWS account (root) → budget alert → IAM user (`portfolio-pipeline`, no console access) → policy `AmazonS3FullAccess` → access key (use case "Local code") → `awscli` via Homebrew → `aws configure` (region `eu-central-1`) → S3 bucket (`tp-portfolio-ecommerce-raw`).

**Questions/answers:**
- *Why an "access key", not an "API key"/SSH?* — AWS-specific naming; SSH is a different protocol (logging into a machine), not used for API calls.
- *Do you log into AWS with Gmail?* — yes, the root user = a normal email + password; an IAM user logs in differently (account ID + username), but here deliberately without console access at all.
- *Is user1/portfolio-pipeline like a new employee in a corporation?* — partly; in mature companies people log in via SSO/IAM Identity Center (federation), and a single IAM user with an access key remains the pattern for **service accounts** (automation), not people.
- *I accidentally landed in "IAM Identity Center" (multi-region, KMS charges)* — that is a different service than plain IAM; Cancel, look for plain "IAM" without "Identity Center".
- *`uv install awscli`?* — NO; `awscli` is a system tool (Homebrew), not a Python library imported by the code — `uv` only manages the project's dependencies.
- *Why is the bucket in `us-east-1` while `aws configure` has `eu-central-1`?* — it turned out to be just the UI's default view before a reload; `aws s3api get-bucket-location` confirmed it is `eu-central-1`.

## 4. Data seed (`seed.py`)

**Done:** Faker (`Faker.seed(42)`) → 50 customers, 30 products, 200 orders + items/payments/status history, tested end to end (a separate Postgres in a sandbox), identical counts confirmed on your side (56/47/14/40/43 status breakdown).

**Questions/answers:**
- *What is `psycopg2`?* — a Python driver/adapter for Postgres; `psycopg2-binary` = precompiled binaries.
- *Why `uv run python script.py`, not `python script.py`?* — `.venv` is an isolated environment with the dependencies actually installed; a bare `python` uses the system Python without those packages.
- *What is a lock file, in general?* — an automatically generated file with the exact versions of the WHOLE dependency tree; the same concept as `package-lock.json` (npm), `Gemfile.lock` (Ruby), `Cargo.lock` (Rust).
- *If I have `uv.lock`, is `pyproject.toml` redundant?* — NO; `pyproject.toml` = the intent (edited with `uv add`/`uv remove`), `uv.lock` = the exact, automatically resolved result of that intent. Both are committed together.

## 5. Incremental extract: Postgres → S3 (`extract.py`)

**Done:** a watermark per table in a local JSON file, `MAX(watermark)` from the rows actually fetched as the new cut-off point, append-only partitioning in S3 (`ingestion_date=.../run_id=...`). Tested: the first run (full), the second run (zero new rows), an UPDATE caught correctly (exactly 1 row).

**Questions/answers:**
- *What does `load_watermark_state()` do, why `return {}` without an `else`?* — an "early return": `return` in the first branch ends the function, so the code below runs ONLY when that branch didn't happen — functionally identical to an explicit `else`.
- *What does `try/finally` do?* — it guarantees the cleanup (`connection.close()`) runs regardless of whether an error happened in the `try` block — without it, an error halfway through the loop would leave the connection open.
- *Why does the watermark catch new AND changed records, not just "new" ones?* — because it asks "what is newer than the last checkpoint", not "was it updated" — both an insert and an update set `updated_at`.
- *What does this look like in production (not a local file)?* — two patterns: (A) an external state store (an Airflow Variable/a control table), (B) querying `MAX(watermark)` directly from the TARGET table (Snowflake/Iceberg) — self-healing, no separate state to lose. The local JSON is a deliberate placeholder until the target table exists.

## 6. Repo on GitHub

**Done:** `.gitignore` (newly added, because `uv init` didn't create it — the folder was already a git repo), the first commit, a new empty repo on GitHub (Public, without README/gitignore on their side), `git remote add origin` (SSH), `git push -u origin main`.

**Questions/answers:**
- *Why didn't `.gitignore` exist despite the earlier `uv init`?* — `uv init` creates `.gitignore` only when it creates a new git repo ITSELF; if the folder was already a repo (`.git` existed), it skips that step.
- *What do `git remote add origin`/`git branch -M main`/`git push -u origin main` do?* — in order: register the remote repo's address under the alias `origin`; force the branch name `main`; the actual transfer + remembering the link between the local and remote branch (`-u`).
- *How do port/IP work (SSH on port 22)?* — the IP = the building's address, the port = the room number inside; port 22 is an IANA convention (a registry), not a physical property — an SSH server can listen on any other port.
- *How does SSH (for `git`) differ from PAT+HTTPS (for the REST API)?* — SSH = the transport protocol used by `git` for push/pull; a PAT = a credential sent INSIDE ordinary HTTPS requests to the REST API — two independent layers (transport vs identity), not variants of the same thing.

## 7. GitHub API extract (`github_extract.py`)

**Done:** a fine-grained PAT ("Public Repositories, read-only"), `.env` + `python-dotenv`, 4 entities (`repositories`/`issues`/`pull_requests`/`contributors`) from `apache/airflow`, three different incremental patterns per endpoint (the GitHub API imposes different constraints), pagination via the `Link` header, multithreading via `ThreadPoolExecutor`+`as_completed`.

**Bugs found and fixed (real, empirical):**
- GitHub API: `since=1970-01-01` (the epoch) SILENTLY returns an empty list (status 200, `[]`) — an undocumented edge case; fix: skip `since` entirely on the first run instead of using the epoch.
- The first `pull_requests` run hung for minutes — sequential pagination through the WHOLE PR history of a very active repo; fix: limit the first run to 90 days back + a hard limit of 20 pages as a safeguard + page-by-page progress logging.

**Questions/answers:**
- *What is a PAT, what are the alternatives?* — a token identifying the user to the API (instead of login+password, disabled by GitHub in 2021); alternatives: an OAuth App (logs in many users), a GitHub App (organisation-level integration), a classic PAT (broader scopes, less precise).
- *What data are we actually pulling from `apache/airflow`?* — public metadata of the repo/issues/PRs/contributors, exactly what you see on the GitHub page, just as JSON instead of HTML.
- *What does `as_completed` do?* — it does NOT add concurrency (that's what `submit()` does); it only changes the ORDER in which results are received to "as they finish" instead of "in submission order".
- *`multiprocessing.pool.ThreadPool` vs `concurrent.futures.ThreadPoolExecutor`?* — two independent dialects of the same idea (older vs newer), they differ only in API style (`.map()` vs `.submit()`+`as_completed`), and have nothing to do with whether the code is about Spark or not.
- *When do threads actually help?* — I/O-bound (waiting for the network/disk/cluster) — the GIL is released while waiting. CPU-bound (in-memory computation) — threads do NOT help because of the GIL; you need `ProcessPoolExecutor`.
- *What are threads, intuitively?* — a process = a building (isolated memory), a thread = an employee inside (shares memory with other threads); the GIL = "one photocopier per building".
- *Code with Spark and a `ThreadPool` — does it make sense, where does Spark come in?* — YES, it is a legit and documented technique (Spark "Scheduling Within an Application"); three layers: (A) Spark itself distributes ONE query across executors (JVM, independent of Python), (B) threads on the driver decide how many INDEPENDENT queries go to Spark at once (pure Python, the same GIL/I-O as with the API), (C) Spark's Fair Scheduler (optional, has to be enabled with `spark.scheduler.mode=FAIR`) fairly shares cluster resources between parallel jobs from the threads.
- *Do you always have to add the Fair Scheduler?* — NO, the default FIFO is fully correct functionally; FAIR is an optional optimisation with many jobs of different sizes/limited cluster resources.
- *A bug found in the user's sample code (outside this project)* — the `else` branch (`multithread_processing=False`) had `.append()` OUTSIDE the loop (wrong indentation) — it would only work for the last iteration; plus a missing initialisation `schema_output=[]` in that branch.

## 8. Data validation (`validate.py`, Phase 2)

**Done:** per-table `TABLE_EXPECTATIONS` (required_columns, not_null, PK, accepted_values), finding the newest file in S3 by `LastModified`, validation + quarantine (nothing is lost, everything rejected goes to `quarantine/` with the reason added). Tested on mocked bad data (null, duplicate PK, wrong status, missing columns) — all 4 mechanisms caught correctly. On real data: 0 quarantined (clean data).

**Questions/answers:**
- *Do extra columns raise an error?* — no, only MISSING required columns are checked; extra ones pass untouched.
- *Does `validate_row` say WHICH row has the error?* — indirectly yes: the whole original row + `_quarantine_reasons` go together into the file in S3, so the context (the PK and other fields) is always visible next to the reason.
- *Set vs list for checking PK uniqueness — is a set overkill?* — NO; a `set` (hash table) gives a near-instant membership check regardless of size, a list requires going through everything one by one (O(n²) in total for a list vs O(n) for a set at large scale).
- *Why is PK uniqueness computed BEFORE the main loop, not during it?* — a duplicate is a property of the WHOLE batch, not of a single row; you can't judge "is this a duplicate" by looking only at what you've already seen in the loop — you need to know the whole set upfront.
- *What does the `_` at the start of `_quarantine_reasons` do?* — a pure naming convention (not a language requirement), signalling "this is metadata added by me, not an original source column".
- *What does `{value!r}` do in an f-string?* — `!r` = use `repr()` instead of `str()` — it shows the "programmer's" representation (e.g. quotes around strings), which helps tell a string apart from other types in diagnostic messages.
- *Set vs tuple?* — a tuple: ordered, immutable, duplicates allowed (for grouping fixed values); a set: unordered, no duplicates, optimised for fast membership checks — different purposes, not variants of the same thing.
- *What is the procedure for handling quarantine?* — at this stage fully MANUAL (manually reviewing `s3://.../quarantine/`); automating alerts/reactions is Advanced scope (full Great Expectations), deliberately postponed.

## 9. PySpark + Iceberg (Phase 3, in progress)

**Done:** `spark-docker/` (a new service `spark-iceberg` in `docker-compose.yml`, running in the background as a "workbench" — `docker exec` to run scripts), the IAM user extended with `AWSGlueConsoleFullAccess`, the `portfolio_ecommerce` database in the Glue Data Catalog, `merge_customers.py` (full SCD2 via `MERGE INTO` in two steps on an Iceberg table).

**Problems encountered and fixed (real, empirical):**
- `bitnami/spark:3.5` — in 2025 Bitnami/Broadcom removed the free version tags from Docker Hub ("No tags available"). `apache/spark-py` (official) hasn't been updated for ~3 years, max `v3.4.0` (too old for the Iceberg jars for 3.5). Fix: our own `Dockerfile` from `eclipse-temurin:17-jdk-jammy` (an official, actively maintained OpenJDK) + `pip install pyspark==3.5.5` — full control over the version, less dependence on other people's business decisions.
- `SdkClientException: Unable to load region` when trying `CREATE TABLE` through Glue — the AWS SDK v2 for Java (inside `iceberg-aws-bundle`, used by Iceberg/Spark) reads ONLY `AWS_REGION`, not `AWS_DEFAULT_REGION` (which `boto3`/Python uses). Fix: set BOTH variables in `docker-compose.yml`.

**Questions/answers:**
- *Why Docker instead of installing Spark/Java locally on macOS?* — PySpark requires a JVM regardless of `pip install`; `import pyspark` succeeds, but real use (`SparkSession`) fails without Java. Docker avoids struggling with Java versions on the host, consistent with the rest of the project.
- *What changed that forced a different approach (Bitnami)?* — an external change in the image provider's policy, not a bug in the project; a good illustration of the risk of relying on someone else's "convenient" base image (analogous to vendor lock-in, cheat sheet 15.4).
- *Is `spark-docker/Dockerfile` an instruction for downloading binaries?* — yes: `FROM` = the base (Docker Hub), `apt-get install` = Ubuntu packages, `pip install pyspark` = Spark+Python from PyPI, `curl` = the Iceberg jars from Maven Central — four different sources, each downloaded and assembled into one ready image.
- *Why `docker exec -it spark-iceberg ...` and the "weird" path `/opt/spark-scripts/...`?* — `spark-iceberg` is the container's name (like `postgres-source` earlier); the path is an address INSIDE the container, different from the host, because it is a different, isolated file system.
- *Why do we need the file under two addresses, why doesn't the container see the host folder normally?* — containers are isolated from the host BY DEFAULT (a security feature, not a limitation); a bind mount (`volumes: - ./scripts:/opt/spark-scripts`) is an explicit, deliberate exception — THE SAME physical file, available under two paths, not a copy (in contrast to `COPY` in a Dockerfile, which makes a real copy once, during the build).
- *Are we moving from boto3 to the AWS SDK v2 for Java?* — NO; both coexist in one container, on the two sides of the py4j bridge (section 22.6) — boto3 on the Python side (downloading raw files from S3), the AWS SDK v2 for Java inside the JVM/Iceberg (operations on table metadata through Glue), independently of each other.
- *What do `up -d`/`--build` do?* — `-d` = in the background; `--build` = force rebuilding the image (needed when the Dockerfile changes, not needed when only environment variables change in docker-compose.yml).

*Status: the `merge_customers.py` script is being debugged live — further runs/fixes will be added here.*

**Another bug found and fixed:** `changed_customer_ids` as a `TEMP VIEW` (lazily evaluated) instead of a concrete list — when it was referenced in Step 2, Spark RECOMPUTED the query on the state of the table AFTER the MERGE had already run (where the changed row already had `is_current=false`), so the result was empty. Fix: `.collect()` into a plain Python list RIGHT AFTER computing it, BEFORE running the MERGE — it "freezes" the result before the table changes.

**Full SCD2 verified live:** `customer_id=5`, `country` changed LY→FR in Postgres → `extract.py` caught the delta → `merge_customers.py` correctly closed the old version (`is_current=false`, `valid_to` set) and opened a new one (`is_current=true`, `valid_to=NULL`) — exactly the "wow moment" from the design document.

**AWS Athena set up as an alternative, lighter way of reading the Iceberg tables** — no Spark/container, pure SQL through the Glue Data Catalog. Confirmed: the same query gives an identical result through Athena (566 ms) and through Spark — proof that reading does not require Spark (only writing/MERGE does), in line with the reason for choosing Iceberg (multi-engine, cheat sheet 15.1).

**Questions/answers (continued):**
- *Does this test command connect to S3?* — yes, to TWO services: Glue (metadata/schema/location) + S3 (the table's actual data files).
- *Could this be checked more simply, without building Spark?* — yes, for READING an engine that reads the Glue Catalog is enough (Athena, Trino, eventually Snowflake); Spark is necessary only for WRITING/MERGE, because only it physically rewrites the table's files.
- *Athena from the terminal through the SDK?* — yes (`aws athena start-query-execution` / `get-query-execution` / `get-query-results`, asynchronously in 3 steps — Athena is a serverless service in the background, not a local process like `psql`); it requires: the `AmazonAthenaFullAccess` policy, a results location in S3 (`athena-results/`), a one-time workgroup configuration.
- *Is the AWS SDK v2 for Java something we choose/install instead of boto3?* — no, it came "in the package" inside `iceberg-aws-bundle`; it coexists with boto3 in the same container, on the two sides of the py4j bridge, each for different tasks (boto3 — simple operations on S3 files from Python; the Java SDK v2 — Iceberg's internal operations on Glue/S3).
- *What is a JAR?* — a packaged, compiled Java library — the exact equivalent of a Python package (`pip`/`uv add`), only from Maven Central instead of PyPI, downloaded manually with `curl` in the Dockerfile (Spark has no convenient "package manager" like `uv`).
- *Does the AWS SDK v2 actually "talk" to AWS?* — yes, it is the layer that physically sends signed HTTP requests to AWS; Iceberg delegates the technical work of network communication to it, focusing itself on the table/MERGE logic.

## 10. Phase 3 — completion: partitioning, Athena, consolidating the scripts

**Done:** partitioning of `dim_customer` added RETROACTIVELY (`ALTER TABLE ... ADD PARTITION FIELD days(valid_from)` — partition evolution, without rewriting old data) → `merge_products.py` (SCD2, analogous to customers) → `merge_facts.py` (generic, config-driven, upsert for `orders`/`order_items`/`payments`, append-only for `order_status_history`) → at the request to unify: `merge_customers.py` + `merge_products.py` combined into one `merge_dimensions.py` (config-driven SCD2), old files removed → AWS Athena set up as a lightweight alternative for reading (without Spark) → a bug found and fixed: Spark's JSON reader returns `STRING` for time fields (JSON has no native timestamp type) → `MERGE`/`INSERT` into `TIMESTAMP` columns requires an explicit `F.to_timestamp()` before use.

**The rule for splitting scripts (the answer to "couldn't this be unified"):** parameterise what differs ONLY in values (column names, keys) — `merge_dimensions.py` combines customers+products, because they have an IDENTICAL SCD2 algorithm. Do NOT combine things that differ in LOGIC (two-step SCD2 vs a simple one-step upsert) — `merge_facts.py` stays separate, because it is a different algorithm, not a different configuration.

**Questions/answers:**
- *Why can't Hive partition retroactively, while Iceberg can?* — in Hive a partition IS a physical folder path (the data must sit in `column=value/`), and changing it requires rewriting all files. In Iceberg a partition is an entry in the METADATA LAYER (the manifest), independent of the physical file location — `ALTER TABLE ADD PARTITION FIELD` changes the rule for the future, old files stay untouched.
- *So is Iceberg "flat"?* — not quite; it still has some physical structure on disk, but the engine does NOT read the partition from the folder name, only from the metadata — the metadata is the source of truth, not the path.
- *The difference between CDC and SCD?* — CDC (the watermark in `extract.py`) is a TECHNICAL mechanism for detecting/transporting a change from the source; SCD (Type 1/2/3, `merge_dimensions.py`) is a MODELLING pattern for how to record that change in the target layer. CDC applies to ALL source tables; SCD2 only to dimensions (facts get a simple upsert).
- *Will I lose an intermediate change (e.g. a country changed twice during an extraction break) with a watermark?* — YES, always, regardless of whether the backfill is done day by day or in one run — because Postgres (OLTP, SCD Type 1) overwrites in place, and the intermediate state physically disappears from the source BEFORE the pipeline sees it. This is a limitation of watermark-based CDC, not of the backfill — only log-based CDC (Debezium) fixes it.
- *Then why backfill day by day, if a continuous watermark catches everything in one run anyway?* — for PIPELINES PARAMETERISED BY DATE (e.g. `WHERE order_date = '2026-09-15'` as a filter, not a continuous watermark) day by day is NECESSARY, because each day is a different slice of the query. For a continuous watermark (our `extract.py`) the difference is mainly error isolation/auditability in the UI, NOT a difference in the data caught.
- *What is the "logical date" (`execution_date`/`logical_date`)?* — the date FOR WHICH period the data is meant to be, independent of WHEN the code physically ran (important for backfills — code run on 25 September can "pretend" it is 15 September).
- *Why should `valid_from`/`valid_to` come from `source.updated_at`, not `current_timestamp()`?* — event time (when the change REALLY happened in Postgres) vs processing time (when the MERGE script ran) — if the MERGE runs with a delay relative to the extraction (or when rebuilding the table from scratch), `current_timestamp()` would give wrong, misleading dates in the SCD2 history. It does NOT fix data loss at the source (a different, deeper problem) — only the correctness of the dates FOR THE DATA that actually made it to S3.
- *Is it worth keeping the config (the `DIMENSIONS`/`TABLES` dict) in a separate JSON/YAML file instead of in Python?* — commercially yes at a LARGE scale (the metadata-driven pattern, as in my own experience at P&G/STTM) — YAML/a control table lets non-programmers edit the config, with a more readable diff. At a small scale (2-4 tables, as here) Python is more readable and type-safer (type hints) — a decision about scale, not a universal rule.
- *What does the `spark-iceberg` container do — is it only communication with the AWS SDK?* — NO, it is first of all a PROCESSING ENGINE (Spark/JVM executing `MERGE`, dedup, etc.); the AWS SDK v2 is only ONE of the components inside, used when Iceberg needs to reach Glue/S3 — an analogy: a factory (produces), not a courier (only delivers).
- *Is such a setup (Spark in a single Docker container) used commercially?* — yes, but ONLY for local dev/CI/CD/small jobs — one container = one machine, no real distribution. In production: Databricks, AWS EMR/EMR Serverless, AWS Glue Jobs (the same Glue already used as the catalog!), Spark on Kubernetes.
- *What is a sandbox, how does it relate to Docker?* — a sandbox is a GENERAL CONCEPT (an isolated environment for safe testing), Docker is ONE of the technologies to implement it (others: a separate AWS account, a VM, a browser/JS sandbox, a CI environment). Not every Docker container acts as a sandbox (e.g. `postgres-source` is real infrastructure, not a sandbox) — it depends on the PURPOSE of use, not the technology itself.

**Another bug found and fixed (the same mechanism as the timestamp, a different type):** `extract.py`'s `json.dumps(..., default=str)` turns `Decimal` values into strings TOO (not only dates) — `price`/`unit_price`/`amount` also needed an explicit cast (`cast(col_type)`) before being written to `DECIMAL(10,2)` columns in Iceberg.

**Phase 3 complete, fully verified on real data:** all 6 tables correctly merged — `dim_customer` (51 rows with history), `dim_product` (30), `fact_orders` (200), `fact_order_items` (502), `fact_payments` (160), `fact_order_status_history` (493) — counts consistent with the raw data from `seed.py`.

**Partitioning verification (Athena, `SELECT * FROM "table$partitions"`):** `fact_orders` → 80 partitions (one per day with activity, spread between 20 June and 13 September, consistent with the 90-day spread of `seed.py`); `dim_product` → 24 partitions (similarly spread, because we never manually tested an UPDATE on products); `dim_customer` → only 1 partition (because all SCD2 test changes happened "today", during our tests) — all three results correct and as expected, not errors.

**Questions/answers (partitioning in practice):**
- *How can I see the partitioning tangibly (S3/Glue/Athena)?* — `aws s3 ls .../data/ --recursive` shows the RAW files (but WITHOUT readable partition folder names — see below); the best proof is Athena `SELECT * FROM "table$partitions"` or Spark `SELECT * FROM catalog.db.table.partitions`.
- *Why don't the files in S3 have the partition value in their name/path (e.g. `valid_from_day=...`)?* — a corrected earlier answer: this write engine does NOT encode partition values in the file path at all — an even more "hidden" partitioning than originally described; all the information lives ONLY in the metadata (Avro manifests), unreadable with a plain `ls`.
- *Where in the code can you see the difference between upsert (facts) and append-only (order_status_history)?* — `merge_facts.py`, the `config["append_only"]` flag (per table) branches the generated SQL: `True` → only `WHEN NOT MATCHED THEN INSERT` (no UPDATE branch at all); `False` → additionally `WHEN MATCHED THEN UPDATE SET ...` (a full upsert).

## 11. Phase 3 closed — handoff to a new conversation

**Status:** Phase 3 (PySpark + Iceberg) fully completed and verified: MERGE/SCD2/idempotency/partitioning/Glue REST catalog — all points from ADR-002 covered, on real data, with evidence from two independent engines (Spark for writing, Athena for reading).

**Decision:** continue (Phase 4 — Snowflake) in a NEW chat, to avoid quality degradation in a very long conversation. This file (`PROGRESS_LOG.md`) + `DE_Interview_Cheatsheet.docx` are the entry point for the new chat — they must be uploaded as current project files (not just downloaded locally), so that the new conversation has full context without repeating the history.

**What the new chat should know at the start:** all the infrastructure is up (`docker-compose.yml`: source+metadata Postgres, Airflow, Spark+Iceberg), AWS is ready (S3 bucket, IAM user `portfolio-pipeline`, Glue Data Catalog with the `portfolio_ecommerce` database, Athena configured), 6 Iceberg tables exist and hold data (`dim_customer`, `dim_product`, `fact_orders`, `fact_order_items`, `fact_payments`, `fact_order_status_history`), GitHub ingestion is ready too (4 entities from `apache/airflow`). The next step according to the plan: Phase 4 — integrating Snowflake with the same Iceberg tables through the Glue Catalog, RBAC, dynamic masking, RLS.

## 12. Phase 4 — start: plan and decisions for the Snowflake integration

**Starting context (new chat):** all the infrastructure from Phases 1-3 is up and running —
AWS (S3 bucket, IAM user `portfolio-pipeline`, Glue Data Catalog with the
`portfolio_ecommerce` database, Athena), Docker (source+metadata Postgres, Airflow,
Spark+Iceberg), 6 Iceberg tables with real data.

**Phase 4 plan (agreed):**
1. Snowflake trial (30 days, $400 credit), AWS + `eu-central-1`
2. An IAM role + trust policy (external ID) for Snowflake
3. A REST catalog integration (`ICEBERG_REST`, `CATALOG_API_TYPE = AWS_GLUE`, `SIGV4`)
4. External volume → S3 bucket
5. A catalog-linked database (auto-discovery of the existing 6 tables from Glue, without manual `CREATE ICEBERG TABLE`)
6. RBAC (role hierarchy, grants)
7. Dynamic data masking (e.g. `email` in `dim_customer`)
8. Row-level security (e.g. visibility of orders per region/segment)
9. Verification through the Query Profile (proof of reading from Iceberg/S3, not a copy)

**Decision (full justification and rejected alternatives → `ADR-004`):**
- A REST catalog integration, not the classic `CATALOG_SOURCE = GLUE` (Snowflake
  explicitly recommends REST; the classic path is marked as being phased out).
- `ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS`, not `VENDED_CREDENTIALS`
  — we avoid introducing AWS Lake Formation as a new access layer next to the already
  working IAM-role-per-service model; the same decision pattern as with the
  Python dict vs YAML in Phase 3 (don't add complexity unless scale forces it).
- A catalog-linked database instead of declaring tables manually — fits the fact
  that the tables already exist from Phase 3 and are not created from scratch by Snowflake.

**Status:** plan accepted, starting from point 1 (creating the Snowflake trial account)
in the next message.

## 13. Phase 4 — Snowflake account created, a primer on the basics

**Done:** trial registration (30 days, $400 credit) — **Enterprise** edition
(required for Dynamic Data Masking and Row Access Policies from points 7-8 of the plan;
Standard doesn't have them at all — a hard gate at the edition level, not a setting to
turn on), cloud **AWS**, region **EU (Frankfurt)** — consistent with S3/Glue.
Logged in with the `ACCOUNTADMIN` role (super-admin, used only for setup, not
for everyday work — part of the RBAC topic from point 6).

**Questions/answers:**
- *Is the trial free, does the Enterprise vs Standard edition cost anything in the trial?*
  — fully free, no credit card; both editions cost exactly the same
  from the $400 pool during the trial; the difference in credit price (Standard $2 vs
  Enterprise $3) only counts AFTER the trial, when moving to a paid account.
  The real cost to watch is credit consumption (storage + compute), not the edition
  — auto-suspend on the warehouse is to be kept on, not turned off.
- *Is it worth adding Snowflake CoCo (an AI coding agent native to Snowflake) to the
  project?* — rejected. It contradicts ADR-003 (a deliberately limited workflow of
  Claude Code + Codex); the risk that the AI writes the RBAC/masking/RLS for the user,
  which undermines the goal of Phase 4 (understanding these mechanisms for interviews). A possible
  mention as "awareness" in the cheat sheet postponed, not done.
- *Is the data in Snowflake only metadata?* — no: the metadata (schema, Iceberg
  manifests, statistics for file pruning) is the only thing synchronised by
  Snowflake; the row values themselves physically stay in Parquet files in S3 and are
  read live on every query by Snowflake's compute (the warehouse) —
  the same multi-engine pattern as Spark/Athena in Phase 3, not a copy of the data into
  Snowflake's storage (which is what distinguishes it from an ordinary native Snowflake table).
- *What is a manifest, Avro vs Parquet vs the Delta transaction log?* — the full
  explanation with examples and an analogy to `git checkout` vs a queue of patches
  moved to the cheat sheet, section **15.1.1** (new), so as not to duplicate
  general theory in two places — here only a note that the topic came up and where
  to look for details.

**A primer on Snowflake basics (before IAM/catalog integration):** a warehouse (a compute
engine, not storage — that's what you pay for), database → schema → table (analogous
to `catalog.schema.table` in Glue/Spark), RBAC through roles (not directly on
the user), a worksheet (SQL through the UI, the equivalent of `docker exec` into Spark),
`ACCOUNTADMIN` as the starting role only for setup, the cost model (storage cheap/
almost zero for us, because the data sits in S3; compute = the warehouse, paid only when
it "runs").

**Status:** ready for step 2 of the plan — the IAM role + trust policy for Snowflake.

## 14. Phase 4 — catalog integration finished (Glue REST)

**Done:** IAM policy `snowflake-glue-catalog-read` (read-only: `GetCatalog`/
`GetDatabase`/`GetDatabases`/`GetTable`/`GetTables`, scoped to the database
`portfolio_ecommerce`) → IAM role `snowflake-glue-catalog-role` with a temporary
trust policy (a placeholder: trust in our own account + a dummy external ID) →
`CREATE CATALOG INTEGRATION glue_rest_catalog_int` in Snowflake (`ICEBERG_REST`,
`CATALOG_API_TYPE = AWS_GLUE`, `SIGV4`, `ACCESS_DELEGATION_MODE =
EXTERNAL_VOLUME_CREDENTIALS` in line with ADR-004) → `DESCRIBE CATALOG INTEGRATION`
→ read `GLUE_AWS_IAM_USER_ARN` + `GLUE_AWS_EXTERNAL_ID` → the role's trust policy
updated to the final values (the placeholder replaced with the real
Snowflake principal).

**Questions/answers:**
- *Why a role, when so far I have only created users?* — a user = permanent
  credentials (an access key) for its OWN long-term use; a role = no permanent
  credentials of its own, a temporary borrowing of permissions via
  `AssumeRole` (a token of ~1h that expires by itself) — the standard pattern for cross-account
  access, when an EXTERNAL system (here: Snowflake) needs access and you don't want to
  share a permanent secret.
- *Does the role "expire" after one use?* — no, the role exists permanently like a user;
  only the TOKEN issued on every `AssumeRole` is one-off/temporary,
  not the role itself — Snowflake will assume this role on every query,
  each time getting a new, short-lived token.
- *Policy vs role — which is under which?* — a policy is a dead document (a list of
  permissions), it does nothing by itself until it is attached to an
  identity (user/role/group); a role = a container made of TWO parts: a trust
  policy ("who can assume me" = authn) + an attached permissions policy
  ("what is allowed once inside" = authz).
- *Is this authentication and authorization?* — yes: the trust policy = AuthN
  (do I let you in at all), the permissions policy = AuthZ (what you may touch);
  a caveat: in AWS `AssumeRole` both steps merge into one API call
  (they are not as sharply separated as e.g. in classic OAuth), but the conceptual
  split is identical — the same universal rule as in cheat sheet 14.6.
- *Is writing the AWS Account ID explicitly in ARNs dangerous?* — no,
  the Account ID is a public identifier (like a tax ID), not a secret — by itself it does not
  give access. The truly sensitive things are the access/secret keys. The same is done
  in production (Terraform/CloudFormation with the Account ID as an ordinary variable, not
  a secret).
- *Why does the `Principal` in the trust policy change from the placeholder to
  `GLUE_AWS_IAM_USER_ARN`?* — an order forced by a time dependency: Snowflake
  requires the ARN of SOME role already at `CREATE CATALOG INTEGRATION`, but it generates its own,
  final IAM user (which the trust policy is ultimately meant to trust)
  ONLY while creating the integration — we only learn it from `DESCRIBE`.
  The placeholder was never the final value, only the technical requirement
  "the trust policy can't be empty".
- *Why an external ID when there is already an ARN?* — Snowflake uses THE SAME
  internal IAM user for many of its customers at once ("Snowflake provisions
  a single IAM user for your entire Snowflake account") — the external ID protects
  against the confused deputy problem (another Snowflake customer who knows the same ARN
  couldn't assume THIS particular role without knowing THIS specific ID).

**Status:** catalog integration ready. The next step of the plan — the external volume
(S3, a second role with the same two-step trust policy pattern), then
`CREATE DATABASE ... LINKED_CATALOG` (auto-discovery of the 6 tables).

## 15. Phase 4 — external volume, catalog-linked database, verification on live data

**Done:** IAM policy `snowflake-s3-external-volume-read` (read-only:
`GetObject`/`ListBucket`/`GetBucketLocation`) → the role
`snowflake-s3-external-volume-role` with its own, explicitly chosen external ID
(not generated by Snowflake, unlike the catalog integration) →
`CREATE EXTERNAL VOLUME portfolio_ecommerce_ext_vol` (`ALLOW_WRITES = FALSE`,
in line with the decision: Snowflake only reads, Spark remains the only writer of these 6 tables)
→ `DESC EXTERNAL VOLUME` → trust policy finished →
`SYSTEM$VERIFY_EXTERNAL_VOLUME` → `PASSED` (the read/write/list/delete fields
`UNVERIFIED`, expected with `ALLOW_WRITES = FALSE` — the function can't
perform a round-trip write test; it is not an error).

**A bug encountered and fixed (real, empirical — the same nature as the
timestamp/Decimal in Phase 3):** the first attempt at `CREATE DATABASE ...
LINKED_CATALOG` returned the error `unable to load table 'dim_customer' metadata.
Verify the catalog integration has read access`, despite a correct Glue role.
The cause: loading a table's metadata through the Glue Iceberg REST endpoint is a
TWO-STEP operation — Glue returns only a POINTER to the `metadata.json` file
(`GetTable`), but the REST endpoint itself then has to physically OPEN that file in
S3 to return its content. The role `snowflake-glue-catalog-role` had only
Glue permissions; `s3:GetObject` on the bucket was missing. Fix: a second
statement (`SnowflakeGlueRestMetadataRead`) added to the policy
`snowflake-glue-catalog-read`, giving the same role S3 read access too — even though
`ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS` would suggest that ALL
S3 access goes through the external volume, in practice the catalog integration
(Glue) and the external volume (data) are two INDEPENDENT S3 access paths, both
required.

**`CREATE DATABASE portfolio_ecommerce_db LINKED_CATALOG = (...) EXTERNAL_VOLUME
= (...)`** — success after the fix. `SHOW ICEBERG TABLES` shows all 6
tables (auto-discovery worked, zero manual `CREATE ICEBERG TABLE`).
`SELECT COUNT(*) FROM dim_customer` → **51** — as expected (50
customers + 1 SCD2 history row from Phase 3). A third independent engine (after
Spark and Athena) reading the same Glue Catalog/S3 — confirming the
multi-engine thesis from ADR-002/cheat sheet 15.1.

**Questions/answers:**
- *Why `SHOW ICEBERG TABLES`, not an ordinary `SHOW TABLES`?* — an Iceberg table
  is a separate object type in Snowflake's model (like `VIEW`/`EXTERNAL TABLE`), it has
  properties that a native table doesn't have (`EXTERNAL_VOLUME`, `CATALOG`) and
  it lacks Snowflake's typical Time Travel (because history/versioning is already
  provided by the Iceberg format itself) — different enough to be a separate
  category, not a variant with a flag.
- *Is it correct that `customer_id=5` repeats in the result (2 rows)?* —
  yes, that is the essence of SCD Type 2, not an error. The business key DELIBERATELY repeats;
  the actual uniqueness (what `MERGE` works on) is the composite
  `(customer_id, valid_from)` — without a separate surrogate key column this composite
  IS the table's key. `is_current=true` guarantees one current version per
  customer, but by itself it is not unique (many different customers have
  `is_current=true` at the same time). The full explanation (including the role of a surrogate key
  in classic Kimball) moved to the cheat sheet, section 5.3.
- *Why does the catalog integration role need both Glue and S3 — Glue as the
  table of contents, S3 as the data+metadata+manifests?* — exactly the right intuition,
  confirmed: Glue holds ONLY a pointer to the current `metadata.json`
  (`GetTable`), it doesn't know/parse its content; the engine reads the content itself (and then the manifest
  list → manifest file → data) directly from S3. The full
  explanation with the analogy "table of contents vs the books themselves" moved to the cheat
  sheet, section 15.1.1.

**Status:** the Phase 4 read path is fully verified on live data. Remaining:
points 6-9 of the plan: RBAC, dynamic masking, row access policy, Query Profile.
Also noted (not implemented yet) a decision: the Gold layer/marts (a future
phase, dbt) will go the "School C" way — Snowflake-managed Iceberg tables
(`CATALOG = 'SNOWFLAKE'`, a separate external volume with `ALLOW_WRITES = TRUE`) —
instead of native Snowflake tables, to avoid vendor lock-in at the level of the
serving layer (the same motif as the "$2.5M savings through vendor independence"
from P&G in the cheat sheet), while keeping Snowflake's full performance/features
(masking/RLS confirmed to work directly on Iceberg tables, including
externally-managed ones). The decision goes into a formal ADR only at the start of the dbt phase.

## 16. Phase 4 — RBAC + Dynamic Data Masking (points 6-7 of the plan)

**Done (RBAC):** roles `portfolio_analyst`/`portfolio_engineer` with
inheritance (`GRANT ROLE portfolio_analyst TO ROLE portfolio_engineer`),
plugged into the built-in hierarchy (`GRANT ROLE portfolio_engineer TO ROLE
SYSADMIN`), grants on the database/schema/tables (including `ON FUTURE TABLES`) +
the warehouse. Switching test (`USE ROLE` + `SELECT COUNT(*)`) → both roles
return 51, inheritance works.

**Done (masking):** `CREATE MASKING POLICY email_mask` on `dim_customer.
email` (the full email for `portfolio_engineer`/`ACCOUNTADMIN`, `***@domain` for
`portfolio_analyst`) → `ALTER ICEBERG TABLE ... MODIFY COLUMN email SET
MASKING POLICY`. Live test: `portfolio_analyst` → `***@...` confirmed.

**Bugs encountered and fixed (two, both real/empirical):**
1. `CREATE MASKING POLICY` → `This session does not have a current database` —
   no `USE DATABASE`/`USE SCHEMA` before creating the object; fix: an explicit
   session context.
2. After fix #1 → `This operation is not supported in a catalog-linked
   database` — a catalog-linked database (`portfolio_ecommerce_db`) supports
   ONLY creating schemas/Iceberg tables/database roles, it can't host
   any other Snowflake objects (including policies). Fix: a new, ordinary
   (not linked) database `portfolio_governance.policies` only for governance
   objects; the policy is CREATED there but APPLIED to a column in
   `portfolio_ecommerce_db` through the fully qualified name — this is
   supported and in line with Snowflake's documentation.

**Questions/answers:**
- *Is the data still only in S3, is `portfolio_governance` a place for data?* —
  no, the data (6 tables) physically stays 100% in S3 unchanged;
  `portfolio_governance` holds only policy definitions (SQL logic, not
  rows) — the same kind of place as role/grant definitions, not comparable
  in size to real business data. Three independent layers: data
  (S3) / governance (Snowflake, `portfolio_governance`) / RBAC (Snowflake,
  account level) — each in a different place for a different reason.
- *Do I see the created roles in the database?* — no, roles (`portfolio_analyst`
  etc.) are objects at the level of the WHOLE ACCOUNT (`SHOW ROLES`), unrelated to
  any particular database — unlike a "database role" (a separate object type in
  Snowflake, enclosed within one database, and that one IS allowed in a
  catalog-linked database). The project deliberately uses ordinary account-level roles
  (simpler at this scale — 2 roles, one team); database roles noted
  as an alternative, not implemented.
- *When masking, when RLS?* — a decision test: "should this person even
  know that the row exists?" — yes, but not all details → masking
  (a column); no, the whole record is out of reach → RLS (the row disappears from the result).
  The full explanation with examples (multi-tenant SaaS, teacher/class,
  payment card number, salary) moved to the cheat sheet, section 14.4.

**Status:** points 6-7 of the plan closed and verified. Next: point 8 —
a row access policy on `dim_customer.country` (simulation: `portfolio_analyst`
sees only `country = 'DE'`, `portfolio_engineer`/`ACCOUNTADMIN` see
everything — symmetrical to the masking).

## 17. Phase 4 — RLS, Query Profile, closing the phase

**Done (RLS, point 8):** `CREATE ROW ACCESS POLICY country_rls` in
`portfolio_governance.policies` (the same pattern as the masking — a policy can't
be created in a catalog-linked database) → `ALTER ICEBERG TABLE dim_customer
ADD ROW ACCESS POLICY ... ON (country)`. Test: `portfolio_analyst` → 2 rows
(only `country='FR'`... no, `country` is filtered to `DE` in the definition, the result
depends on the actual data), `portfolio_engineer` → 51 (unchanged).
Confirmed to work.

**Done (Query Profile, point 9):** two real "zigzags" with the cache
encountered one after another before we reached a real scan:
1. The first execution of the query → the Query Profile showed `QUERY RESULT REUSE
   [0] 100%` instead of a real scan — Snowflake returned the result from the 24h persisted
   result cache, without touching S3/Iceberg at all.
2. `ALTER SESSION SET USE_CACHED_RESULT = FALSE` run as a SEPARATE
   execution (not "Run All" together with the `SELECT`) → it didn't take effect in time,
   and the second test also hit the cache.
3. Fix: `ALTER SESSION` + `SELECT ... AND 1=1` (a deliberate change of the query
   text as an extra safeguard) selected and run together →
   a real execution, `Total execution time: 121ms`, status "Success"/
   "Finished", NOT "Result Reuse".

**Final proof:** the `DynamicSecureView` node with `Object name: "DIM_CUSTOMER (+
RowAccessPolicy)"` in the Query Profile tree — the RLS is injected into the execution plan
(the table rewritten as a view with the policy), not attached to the result
after the fact. Detailed statistics (bytes/partitions scanned) are not available for
such a small table (0% of the time on the node) — expected at this scale, not an error;
`Query Details` (the tab next to Profile) shows a summary instead.

**An additional observation (outside the plan, worth noting):** RLS/masking
defined in Snowflake **do NOT work automatically in Athena** — Athena
reads directly from Glue+S3, bypassing the Snowflake engine, so it neither knows
nor enforces these policies. Governance is natively single-engine
(tied to Snowflake), unlike the data FORMAT (Iceberg —
genuinely multi-engine). There is a mechanism that fixes this (Snowflake
Horizon Iceberg REST Catalog + Scan Plan API, server-side policy enforcement
for external engines) — GA for Spark, maturity for other engines
(including Athena) unconfirmed/variable. Deliberately NOT implemented in this
project — noted as a gap to mention in an interview, not to fix
now.

**Questions/answers:**
- *What is the Query Profile, when is it used?* — a visual tree of the
  query's execution operators (the equivalent of `EXPLAIN ANALYZE` in Postgres,
  cheat sheet 3.1), used reactively (debugging slow queries — which node
  eats the time) or proactively on significant architecture changes
  (verifying that an optimisation/governance actually works, not just
  "seems" to work) — not routinely on every query.

**Status: PHASE 4 CLOSED.** All 9 points of the plan verified on
live data: catalog integration (REST/Glue), external volume, RBAC with
inheritance, masking, RLS, the Query Profile as proof. Two decisions for
future phases noted, not implemented: "School C" (Snowflake-managed
Iceberg for Gold in dbt) and the Horizon IRC mechanism for governance in Athena
(if ever justified). Next step: Phase 5 (dbt) — the first moment of
switching to the formal Claude Code + Codex workflow from ADR-003 (see also
ADR-003, the retrospective section).

## 18. Phase 5 — start (dbt), design decisions + a large block of theory moved to the cheat sheet

**Phase 5 plan agreed:** design decisions (here) → dbt scaffolding (Claude Code)
→ staging models → a second external volume with writes (`ALLOW_WRITES = TRUE`, for
"School C") → marts as Snowflake-managed Iceberg → tests + Codex review.

**Decisions made:**
- **SCD2 stays in Iceberg (Spark MERGE from Phase 3), dbt does NOT run `dbt
  snapshot` on the same data.** Justification: it avoids double/duplicated
  history and keeps the multi-engine value (SCD2 in Iceberg is visible to
  Spark/Athena/Snowflake equally; SCD2 in a dbt snapshot would live only in
  Snowflake as a native table), consistent with the rule already written in cheat sheet
  5.3 ("one layer responsible for history"). dbt only READS the ready
  SCD2 (`WHERE is_current = true` / a point-in-time join).
- **No separate `dim_geography`.** `country` is a flat, single attribute
  without a hierarchy (no city/region in `seed.py`) — extracting it would be
  excessive normalisation; it stays as a degenerate attribute in `dim_customer`.

**A large Q&A block on dbt/ELT/multi-engine fundamentals** (what dbt is and why,
orchestration vs transformation/ADF vs Airflow vs dbt, declarative vs
imperative, a correction of the Bronze/Silver/Gold simplification, Spark vs dbt at large
scale, native alternatives to dbt in Snowflake, DLT/Lakeflow and dbt-databricks,
Snowpark as a deliberately rejected alternative, the purpose of multi-engine in a company) —
**moved in full to the cheat sheet, new section 25** (7 subsections),
so as not to duplicate extensive, general theory in the project log. Here only
a note that the topic came up and where to look.

**Status:** Phase 5 decisions agreed, ready to prepare `CLAUDE.md` and
the first scaffolding task in Claude Code.

## 19. Phase 5 — the first switch to Claude Code, staging fully working

**Tooling setup:** VSCode + the Claude Code extension (a terminal inside
VSCode, not a separate window — the same shell/`.venv` as before, zero new
environment), logged in with a Claude.ai account. `CLAUDE.md` prepared in
English (see the file in the repo) — the project context, binding decisions (SCD2 in
Iceberg, no snapshot, Gold as Snowflake-managed Iceberg), code
conventions, guardrails (no `DROP`/secrets without confirmation). Hooks
(`.claude/settings.json`) deliberately NOT configured yet — to be done
later; until then the manual edit-approval mode plays that role.
A review of VSCode extensions for DE (dbt Power User, SQLFluff, Ruff, Better
Jinja, AWS Toolkit etc.) and the theory of AI-agent governance in a corporation (permission
mode vs data permissions, hooks as defense-in-depth, audit) —
**moved to the cheat sheet, new section 26**.

**Claude Code session 1 — scaffolding + staging (task: staging only, no
marts):** Plan Mode → two architectural questions to the user (where to
host staging — a new database `portfolio_analytics`, later renamed to
**`portfolio_dbt`** to avoid confusion with the `portfolio_analyst` role; how
to handle the case of identifiers from Glue — verify empirically,
don't guess) → the plan saved to a file, accepted in manual
edit-approval mode (not Auto — the first session writing files, no
hooks yet).

**Execution:** `uv add dbt-core dbt-snowflake` (they were already in `pyproject.toml`,
verified empirically anyway) → `dbt init --skip-profile-setup` →
removing the boilerplate, **deliberately removing `snapshots/`** with a comment in
`dbt_project.yml` pointing to `CLAUDE.md` decision 1 → `dbt_project.yml`
(`materialized: view`, `schema: staging`) → `profiles.yml.example` (a template,
key-pair auth, all values via `env_var()`, `profiles.yml` added
to `.gitignore`) → `schema.yml` (source `portfolio_ecommerce_db.
portfolio_ecommerce`, `quoting` with a justifying comment, tests
unique+not_null; a correct self-correction: `data_tests:` instead of
the deprecated `tests:` in dbt 1.12) → 6 models `stg_*.sql` (rename/cast,
`WHERE is_current = TRUE` for dim_*, a comment in every file pointing to
`CLAUDE.md decision 1`) → offline verification (`dbt parse`/`dbt ls`/`dbt
compile --no-introspect` with fake env vars in a scratchpad outside the repo,
`git check-ignore` on `profiles.yml`/`target/` — confirmed ignored).

**Real infrastructure added by the user (outside Claude Code):**
`CREATE DATABASE portfolio_dbt` + grants `USAGE`/`CREATE SCHEMA` for
`portfolio_engineer` → test `SELECT "customer_id" FROM ..."dim_customer"` →
**worked without changes** (the quoting from the plan was correct the first time,
the risk from the plan resolved empirically) → RSA keys (`openssl genrsa
2048` + conversion to PKCS8, Snowflake requires RSA, not Ed25519) → `ALTER USER
... SET RSA_PUBLIC_KEY` → `~/.dbt/profiles.yml` + environment variables
(`DBT_SNOWFLAKE_ACCOUNT` in the format `<ORGANIZATION>-<ACCOUNT>`, e.g.
`VIOWCJF-OO15773`) → `dbt debug` → **`All checks passed!`**.

**Final result:**
```
dbt run --select staging  → PASS=6  ERROR=0
dbt test --select staging → PASS=17 ERROR=0
```
All 6 views in `portfolio_dbt.dbt_dev_staging`, all tests
(unique+not_null on the PK, not_null on the FK) green the first time.

**Questions/answers (reusable theory — full explanation in cheat sheet
26):** Claude Code's permission mode vs data permissions in a corporation (two
separate layers), hooks as a mechanical guardrail independent of what the model
"decides", the VSCode setup (Python was already there, dbt is an ordinary package, not a separate
CLI, the terminal in VSCode = the same terminal as the system one, `.venv` as the
"Workspace" interpreter), why `VIEW` and not `TABLE`/materialized/temp for
staging (cheap, always current, staging is "thin"), Bronze/Silver/Gold
vs staging/marts (two parallel conventions, not contradictory), RSA vs Ed25519 and
the specific `openssl` commands.

**Status:** staging fully working and verified on live data.
Next step: Codex (an independent review of this session's diff), then the external
volume for Gold (`ALLOW_WRITES = TRUE`) and the marts as Snowflake-managed Iceberg.

## 20. Phase 5 — Codex review of session 1, both findings closed

**Codex (an agent in VSCode, a session independent of Claude Code)** got: the original
task + the instruction "don't reimplement, do an adversarial review
against the task and `CLAUDE.md`". It replied (in English) with two
findings, both taken seriously, not dismissed automatically:

- **P1 — timestamp conversion.** `CONVERT_TIMEZONE('UTC', col)::
  TIMESTAMP_NTZ` in 6 models assumes that the source columns are
  `TIMESTAMP_LTZ` (according to Claude Code's comment: "Spark timestamptz →
  Snowflake surfaces as LTZ"); Codex rightly noticed that this was an
  ASSUMPTION, not a verified fact — for `TIMESTAMP_NTZ` the same command
  would shift the values incorrectly. **Verified empirically:** `DESCRIBE
  TABLE dim_customer` → `valid_from`/`valid_to` are indeed `TIMESTAMP_LTZ(6)`.
  Claude Code's assumption was correct, the code unchanged. A good instinct from Codex
  as a reviewer (questioning an unverified assumption), even though the
  result confirmed the original implementation.
- **P2 — case sensitivity of identifiers.** Codex couldn't have known this
  (it had no access to the terminal output), but `dbt test` had already
  passed `PASS=17` with exactly this quoting — resolved empirically
  before the review, dismissed without code changes.

**Closing the loop:** the `DESCRIBE TABLE` result passed back to Codex;
it confirmed both findings as closed, zero SQL changes needed, and
`dbt test PASS=17` meets the `CLAUDE.md` requirement (`dbt run`+`dbt test` after changing
models — here: after the review). Codex didn't hit any real bug in its own/someone else's
implementation → nothing for `ai/failure-log/`.

**Codex's proposal (awaiting a decision):** add a one-line LTZ justification
directly in the model comments (today it is only in the plan, not in the code) and
replace the speculative comment about `CASE_INSENSITIVE` in `schema.yml` with a
verified fact. Documentation cosmetics, zero SQL changes — not done,
waiting for a "yes/no" in the next session.

**Status: Claude Code session 1 (staging) fully closed** — implemented,
tested live, verified by an independent review, both findings
resolved empirically. Next step: the external volume for Gold
(`ALLOW_WRITES = TRUE`), then the marts as Snowflake-managed Iceberg
(Claude Code session 2).

---

## 21. Phase 5 — layer naming and renaming the Iceberg tables (ADR-005)

**Trigger:** the questions "which stage am I at" (Bronze/Silver/Gold vs staging/marts)
and the remark that `dim_`/`fact_` on tables before dbt is confusing: in practice and in dbt convention
they denote the final Gold for Power BI (in Postgres the user kept separate databases
bronze/silver/gold). The criticism was right: the naming was unusual, and the marts would collide
by name with the source. The earlier recommendation "leave it, because of the cost of rewriting" was
withdrawn: the cost of a misleading name is paid at every onboarding, a rename only once.

**Second opinion (ChatGPT):** `dim_`/`fact_` before Gold is not a universal
anti-pattern (in an interview say "a convention, not a law"), but in this architecture the name
communicates the wrong boundary. The risk of a rename lies in the object's identity in the catalog, downstream
dependencies and governance, not in the data. Option C (a separate namespace) is good, but more expensive than
it seems (the Glue database name in IAM, catalog integration, catalog-linked DB, Spark, dbt).
The most valuable remark: why does the dimensional model exist both before dbt and, potentially,
again in Gold? That forced the rule "an object in Gold only if it adds logic".

**Decision (ADR-005):** option A. New names: `customer`, `product`, `orders`,
`order_items`, `payments`, `order_status_history`. `dim_`/`fct_` only for marts.

**Execution (Claude Code, Plan Mode, manual edit approval):** the plan reviewed
in advance (jar `iceberg-spark-runtime-3.5_2.12-1.9.1` with `GlueCatalog.renameTable`
and `RegisterTableProcedure`, PySpark 3.5.5). A snapshot before → a pilot on `dim_product`
(no policies) → the remaining 5 one by one with a check → a snapshot after. Result 6/6:
`customer` 51, `product` 30, `orders` 200, `order_items` 502, `payments` 160,
`order_status_history` 493 rows; `metadata_location` and S3 paths identical.
The `register_table` fallback not needed, no `DROP`. Code updated (only table
names): `merge_dimensions.py`, `merge_facts.py`, `schema.yml`, 6 × `stg_*.sql`; a grep for the old
names empty; `dbt ls` = 6 models / 6 sources / 17 tests.

**My own corrections along the way:** (1) my first prompt said to do the rename via
boto3/CLI, but Glue has no native rename (Iceberg does a create+drop of the catalog entry);
the right way is Spark; (2) I claimed that the masking/RLS policies would "follow" the table —
that was guessing: Snowflake sees a rename as a new object, the policies have to be applied
again (Claude Code reached the same conclusion independently).

**Confirmed by the user after the rename (output not preserved):** Snowflake
(syncing the catalog-linked DB, grants, re-applying masking/RLS on `customer`)
and `dbt run --select staging` and `dbt test --select staging` passed correctly.
The specific numbers (expected PASS=6 and PASS=17) and the role test result were not
preserved, so this record relies on the user's statement, not on a pasted result.

**A known cost:** the S3 directories kept the old names (`.../dim_customer/`); a future
table created under the old name in the default location would land in the same
directory. Deliberately accepted.

## 22. Phase 5 — external volume for Gold ready

**Done (the same two-step pattern as Phase 4, now with writes):** IAM policy
`snowflake-s3-gold-write` (`PutObject`/`GetObject`/`GetObjectVersion`/`DeleteObject`/
`DeleteObjectVersion` on `s3://tp-portfolio-ecommerce-raw/gold/*`, `ListBucket` with
the condition `s3:prefix=gold/*`, `GetBucketLocation` separately) → the role
`snowflake-s3-gold-write-role` with its own external ID (`portfolio_gold_ext_vol_eid`)
→ `CREATE EXTERNAL VOLUME portfolio_gold_ext_vol` (`ALLOW_WRITES = TRUE`,
`STORAGE_BASE_URL = s3://.../gold/`) → trust policy finished on
`STORAGE_AWS_IAM_USER_ARN` → `GRANT USAGE ... TO ROLE portfolio_engineer`.

**Verification:** `SYSTEM$VERIFY_EXTERNAL_VOLUME` → `writeResult`/`readResult`/
`listResult`/`deleteResult` all `PASSED` (unlike Phase 4, where with
`ALLOW_WRITES = FALSE` these fields were `UNVERIFIED` — now the function could perform a full
round-trip). `STORAGE_AWS_IAM_USER_ARN` identical to that of
`portfolio_ecommerce_ext_vol` from Phase 4 (`arn:...:user/g6702000-s`) — confirming that
Snowflake has one shared IAM user per account; the only distinction between the volumes
is the external ID, not a separate user.

**Error encountered:** `CREATE EXTERNAL VOLUME` → "SYSADMIN must have CREATE EXTERNAL
VOLUME granted" — the active role in the Worksheet was `SYSADMIN`, not `ACCOUNTADMIN`;
fix: `USE ROLE ACCOUNTADMIN`.

**Questions/answers:** why a separate IAM role per volume instead of one (the scope
of permissions differs — read-only on the whole bucket vs read-write only on `gold/`;
a shared role would have to add up both, breaking least privilege), what an ARN is.

**Status:** the infrastructure for Gold is complete. Next step: Claude Code session 2 —
the first mart (`fct_customer_revenue`, Snowflake-managed Iceberg, `CATALOG =
'SNOWFLAKE'`, this external volume), a point-in-time join to SCD2, then a Codex review.

---

**Correction (Great Expectations):** in the conversation I wrongly stated that Great Expectations
was done in Phase 2. In fact Phase 2 is our own `validate.py` script (code without a framework, written in the conversation with Claude, like the rest of Phases 1-4, see ADR-003)
(schema, not-null, PK uniqueness within the batch, accepted values, quarantine in
`quarantine/`), and GE is deliberately postponed as Advanced scope (section 8 of this
log and the docstring in `validate.py`). ADR-002 still lists GE as the plan, so when
changing its status from Proposed to Accepted, add that this stage was implemented as
`validate.py`. `CLAUDE.md` and the cheat sheet corrected.

**Questions/answers (theory moved to the cheat sheet, 15.1.2 and 25.9):** why
staging (DRY: "clean up once, use everywhere"), Bronze/Silver/Gold are labels of data
layers, while staging/marts is a division of work inside dbt, what a mart is
(an answer to a business question combining several staging models; "5-10 marts" = as many as there are
questions), generic vs custom tests, `target/compiled` vs `target/run`, a simple example
"how much did each customer spend", dbt in the pipeline's life cycle (not a one-off setup:
`dbt run`/`dbt test` run on every data cycle, eventually from Airflow and CI),
mapping the layers in the project (cheat sheet 25.10).

**Status:** the rename in Glue, in the code and in Snowflake completed and verified
(the Snowflake/dbt part: the user's statement). Next step: the second
external volume (`ALLOW_WRITES = TRUE`) and session 2 (marts, including the first
`fct_customer_revenue` with a point-in-time join to SCD2).

---

## 23. Phase 5 — repairing the SCD2 dates in `customer`, tidying up the repo

**Detection:** before the first mart, a control query (orders earlier
than the customer's first version) gave 200/200. Diagnosis: all customers' `valid_from`
values fell within one 15-minute window on 26.09, while products were spread over 24 days.
Cause: the customers were loaded with an early version of the script that took the processing
time instead of `source.updated_at`. A later code fix didn't touch the
existing versions, because the MERGE doesn't visit them. The same error was at the boundary between
versions of customer 5 (a ~6 min gap without a valid version). The error was invisible as long as
nothing read these dates. It only came out when planning the point-in-time join.

**Correction to section 10:** the single `dim_customer` partition in Athena did not
come from "SCD2 tests done today", but from this error.

**Fix (Claude Code, Spark):** a MERGE with UPDATE only: the first `valid_from` :=
`MIN(created_at)` from the raw S3 files, and the version boundary of customer 5 := the `updated_at`
of the change event. A pilot on customer 5, then the remaining 49. Verification: 51
rows / 50 customers / 50 current unchanged, attribute hashes unchanged,
contiguous versions, re-running changes nothing. Snapshot
`5434959617553012471` → `6395697598619178545` (rollback possible). The script and the states
before/after in `scripts/repairs/2026-09-29_customer_valid_from/`.

**Verification in Snowflake:** `valid_from` from 2026-06-22 to 2026-09-26.
Control query: 115 (the real effect of the random, independent dates in
`seed.py`; a rule in dbt will handle it). Masking and RLS survived the UPDATE (unlike
the rename): `portfolio_analyst` sees only `DE` and `***@`.
`dbt test --select staging` → PASS=17.

**Tidying up the repo:** it turned out that Phases 1–5 (Spark, dbt, ADRs, `validate.py`,
`github_extract.py`, `CLAUDE.md`, this log) had never been committed. 6
branches from `main` (5 of the 6 branches built on a temporary
index, without touching the working directory), a conflict test for all 15 pairs, PRs #1–#6 with squash merge.
The cheat sheet and a debug file stay local (`.gitignore`). `gh` logged in
separately, without the pipeline's token (`GITHUB_TOKEN` from `.env` had ended up in the
shell environment; a token for extraction shouldn't have write access to the repo).

**Status:** Silver correct, the repo complete on `main`. Next step: session 2 —
the first mart.

---

## 24. Phase 5 — session 2: the first mart `fct_orders` (Gold, Snowflake-managed Iceberg)

**Done (Claude Code, Plan Mode, manual approval):**
- `catalogs.yml` (v1, stable; v2 in dbt 1.12 is experimental) with the catalog
  `gold_iceberg` → `CATALOG = 'SNOWFLAKE'`, `portfolio_gold_ext_vol`. The table
  lands in `s3://.../gold/_dbt/dbt_dev_marts/fct_orders/`.
- Staging `stg_customers`/`stg_products` now returns ALL SCD2 versions
  (no `is_current` filter). Uniqueness on `(key, valid_from)` via
  `dbt_utils` 1.4.1 + a guard "at most one current version" (`unique` with
  `where: is_current`).
- `int_customer_versions`: a customer's first version is effective from `1900-01-01`
  (handling the 115 orders placed before the first version from `seed.py`), a half-
  open interval `[effective_from, effective_to)`, without `email` (no
  masked column reaches Gold).
- `fct_orders` (grain: order): `country_at_order` (point-in-time) and
  `current_country`, `gross_order_value`, `paid_amount` (only `completed`),
  `refunded_amount`, `is_before_first_customer_version`.
- Tests: among others version contiguity (boundary n = start of n+1, catches the class of error from
  §23), "exactly one customer version per order", reconciliation of the sum of
  `gross_order_value` with staging.

**Error encountered (anticipated in the plan):** Iceberg in Snowflake rejects
`TIMESTAMP_NTZ(9)` (Snowflake's default precision, nanoseconds) — the Iceberg format
stores at most microseconds. Fix: `::TIMESTAMP_NTZ(6)` on
`order_date`. No data loss, because the source has precision 6 anyway.

**Governance:** RLS (`country_rls` on `country_at_order`) and a mask via a
`post_hook` (the table is recreated on every `dbt run`, so a manual `ALTER`
would be lost), grants via the `grants` config. The order in the materialisation:
CREATE OR REPLACE → post_hooks → grants, so the analyst never gets
SELECT on a table without the policies. Deliberately without `copy_grants`.

**Codex review (3 findings):**
- P1, accepted: `current_country` revealed a country outside the RLS scope (the analyst
  sees an order from DE, but also the country the customer moved to). RLS
  filters rows, it doesn't protect other columns in those rows. Fix: a new
  mask `country_mask` on `current_country` (the analyst sees only `DE`, otherwise
  NULL).
- P2, accepted: `paid_amount` didn't distinguish a refund from no payment → added
  `refunded_amount`, corrected the description.
- P2, closed without changes: `SHOW GRANTS TO ROLE portfolio_analyst` — no
  access to staging or intermediate.

**Final result:** `dbt run` 200 rows, tests 34/34 (the whole chain) and 13/13
(the mart after the fixes). Engineer: 200 orders, 115 before the customer's first
version, 44 countries, 0 NULLs. Analyst: 10 orders, only DE. `POLICY_REFERENCES`
confirms both policies on the table. The mask path returning NULL has no
example in the data (no customer moved away from DE) — only the attachment of the
policy was verified.

**Manual objects in Snowflake (outside dbt):** `country_mask` + APPLY for
`portfolio_engineer`, APPLY on `country_rls`, USAGE on `portfolio_governance`
for the engineer, USAGE on `portfolio_dbt` and `dbt_dev_marts` for the analyst.
Recorded in `snowflake/phase5_gold_governance.sql`.

**Status:** PR #8 (`1add7cd`). The first mart in Gold works end to end.

---

## 25. Tidying up S3: Iceberg table paths matching the names

**Trigger:** after ADR-005 the tables were called `customer`, `orders` etc., but in S3
they sat in `dim_customer/`, `fact_orders/`. Considered a mess, fixed before
Phase 6, so that Airflow works on the final layout from the start. A change of the
decision from ADR-005 (an "Update" section added there).

**Method (Claude Code, Plan Mode):** `rewrite_table_path` + a copy of the S3 files +
`register_table` in a temporary Glue database `portfolio_ecommerce_relocation`
(invisible to Snowflake: the catalog integration role only has access to
`portfolio_ecommerce`, confirmed with `SHOW SCHEMAS`) + a swap via two renames.
CTAS rejected, because it loses the snapshot history. Pilot `product`, then
`order_items`, `order_status_history`, `payments`, `orders`, and finally
`customer` with the analyst's access cut off for the time of the swap.

**Verification:** for every table the rows, content hash, snapshots and time
travel identical, only the location changes. `customer`: 10 snapshots,
the rollback point from §23 still works. In Snowflake, after every swap
`ALTER ICEBERG TABLE ... REFRESH` (without it Snowflake kept reading the old
metadata), both roles, `dbt build` 51/51.

**Finding:** the policies on `customer` survived, because the name didn't change.
A rename (§21) lost the policies; moving the files with `REFRESH` doesn't lose them.
Clarified in `CLAUDE.md` (decision 6).

**Errors along the way:**
- `rewrite_table_path` writes the list of files through the Hadoop layer, and the
  container lacked `hadoop-aws`. Caught by the pilot before any
  write. The first entry in `ai/failure-log/` (ADR-003).
- Along the way it turned out that the `stg_products` view in Snowflake predated session 2:
  `dbt run --select +fct_orders` doesn't build models outside the mart's chain.
  Conclusion: after changes to several models, a full `dbt build`.

**Cleanup:** removed the `*_old_path` entries and the temporary database in Glue, 685
objects in S3 (the old directories and `migration-staging/`; the bucket has no versioning,
so irreversibly). Before deleting, checked that no table points
to the old directories. PR #10.

---

## 26. Phase 6 — start (Airflow): image and compose, extraction window, both extract scripts

**Time constraint:** the Snowflake trial ends around 27.10. Phase 6 uses almost no
credits (the warehouse only works in the dbt task), so time is the limit.
Steps 6.1–6.3 don't touch Snowflake.

**Phase 6 plan:** 6.1 Airflow image → 6.2 docker-compose (mounting `scripts/` and
`dbt_portfolio/`, secrets from `.env`, `docker.sock`, a named Spark image) → 6.3
refactoring the scripts (config from env vars, a `--start/--end` window) → 6.4 the DAG
`ecommerce_pipeline` (`[extract_postgres, extract_github]` → `validate` →
`merge_dimensions` → `merge_facts` → `dbt build`) → 6.5 retries with exponential
backoff, `execution_timeout`, `on_failure_callback` → 6.6 a backfill over 3 days →
6.7 documentation.

**Architectural decisions:**
- **Spark outside the Airflow image.** The `spark-iceberg` image already has Java and the
  Iceberg/AWS JARs. Airflow runs it as a temporary container through
  `DockerOperator` (dependency isolation), instead of copying the JVM and the JARs into the
  Airflow image. The original idea "extend the Airflow image with PySpark with
  Iceberg" was rejected after reading `spark-docker/Dockerfile`.
- **dbt in a separate venv in the Airflow image** (`/opt/airflow/dbt_venv`), because dbt and
  Airflow pin conflicting versions of shared libraries.
- **Extraction window = data interval with a 10-minute lookback, no watermark
  state → ADR-006.** It changes the plan from the `extract.py` docstring (§5:
  "the watermark will move to Airflow Variables"). Justification: idempotent runs,
  backfill without manually rewinding state, the history of windows in `dag_run`. The lookback
  closes the "late commit" gap (`now()` in Postgres = transaction start).
  Duplicates from the lookback are absorbed by the existing deduplication before `MERGE`.

**Done (6.1):** `airflow-docker/Dockerfile` (`apache/airflow:2.9.0-python3.12`,
providers with the official constraints file, dbt-core 1.12.5 + dbt-snowflake 1.12.1 in a
separate venv) and `requirements.txt` (the docker/amazon/postgres providers without
pins, versions from the constraints). Verification: build 51 s,
`apache-airflow-providers-docker 3.9.2`, `dbt --version` → core 1.12.5, snowflake
1.12.1. The `BrokenPipeError` in the output is an effect of `| head -2`, not an error.

**Questions from this step and what they teach** (theory: cheat sheet 31.1–31.4):
- **The plan said "we extend PySpark with Iceberg/AWS" — what does that mean?** Spark by
  itself can't read Iceberg or talk to Glue/S3; it needs additional
  JARs (`iceberg-spark-runtime`, `iceberg-aws-bundle`). "Extend" = build
  them into the image at build time, instead of making Spark download them from Maven on
  every run (`spark.jars.packages`). Our Spark image already has them,
  which is why Spark didn't end up in the Airflow image.
- **Will the extraction window and its history be in Airflow Variables?** No. Airflow
  computes each run's window itself from the `schedule` and `start_date`
  (`data_interval_start/end`), and the history of runs with their windows is in the
  metadata database (`dag_run`). Variables stay for configuration, not for state.
- **How do I do a backfill with a lookback — pass `-s` 5 minutes earlier?** No.
  `-s/-e` in `airflow dags backfill` choose WHICH days (runs) to execute;
  the lookback (10 min back) is added by the task itself in the command template, so every
  backfill run gets it automatically.

**Done (6.2, 8.10):** `docker-compose.yml`: shared Airflow variables
(`POSTGRES_SOURCE_HOST=postgres-source`, AWS keys from `.env`, `AWS_REGION` and
`AWS_DEFAULT_REGION`, `GITHUB_TOKEN`, `HOST_PROJECT_DIR=${PWD}` for the mount
paths in `DockerOperator`), mounting `scripts/` and `dbt_portfolio/` in the
webserver and the scheduler, `docker.sock` in the scheduler (LocalExecutor executes
tasks in it), a fixed Spark image name `portfolio-spark:latest`.

**Done (6.3, part 1, 8.10):** `extract.py` on `--start/--end` (window
`[start, end)`, dates without a time zone = UTC, validation start < end), config from env vars
with the current values as defaults, no state file. A test with the same window
(8.10) in two environments: from the Mac (`uv run`) and from the Airflow container (`docker
compose run`): both 0 rows without errors, `docker.from_env().ping()` → `True`.
Two runs on purpose: the Mac checks the code's logic, the container checks the surroundings
(network, secrets, packages from the image, mounts). Writing to S3 has not been
checked from the container yet (0 rows = no `put_object`).

**Discovered 20.10 (after a break in the work):** `docker compose ps -a` showed
`airflow-webserver` in the state `Exited (1)` since 29.09 and the scheduler on an image
labelled only with `sha256:...`.
- Webserver: the gunicorn workers died (`Some workers seem to have died`, `No
  response from gunicorn master within 120 seconds`), most likely lack of memory in
  Docker Desktop or the Mac going to sleep. The services have no `restart:`, so it lay dead for 3
  weeks unnoticed.
- Scheduler: the image rebuild on 7.10 moved the name to the new image, and the running
  container stayed on the old one (Docker doesn't swap the image in a running
  container). So it didn't see the mounts, variables or `docker.sock` from 6.2;
  the 8.10 test passed only because `docker compose run` starts a fresh
  copy.
- Fix: `docker compose up -d --build` (the Postgres volumes untouched).
  Verification on the running scheduler: it sees `scripts/` and `dbt_portfolio/`,
  `ping()` → `True`; the webserver stable after a few minutes.

**Questions from these steps and what they teach** (theory: cheat sheet 31.5–31.7):
- **Why `.env`, if `boto3` finds the keys by itself?** `boto3` looks for
  credentials in order: environment variables → `~/.aws/credentials` → the machine's IAM
  role. On the Mac it hits `~/.aws`, but the container doesn't see your `~/.aws`,
  so `.env` puts the keys into the container's variables. In a corporation there are no keys
  at all: the server/pod gets an IAM role and `boto3` hits that (step 3).
- **If the script only does `import`, where do the packages come from?** `import` loads
  a package from the Python that runs the script; it doesn't install it. On the Mac that's
  `.venv` (from `uv.lock`), in the container the Airflow image (`requirements.txt`, written
  by hand, versions from the Airflow constraints). The same code, two installations, hence
  the test in both places: the Mac checks the logic, the container the surroundings (network, secrets,
  packages, mounts). In a corporation there is one source of truth: an image built in CI.
- **`airflow-init` was meant to be one-off, but it runs on every `up` — is it
  needed?** It is short-lived, not one-off: on every start it
  checks/migrates the Airflow database schema and exits; the real work
  (creating the database, the user) it did once. It stays as a gate: the webserver and
  scheduler wait until it finishes successfully.
- **Why is `spark-iceberg` running all the time?** It has the command `tail -f /dev/null`
  (it does nothing, but never ends), and `up` starts all services. Since
  Spark is run by `DockerOperator` in temporary containers, the permanent container
  is only needed for manual work.
- **Why doesn't `docker compose` work in another folder, while `docker ps` does?**
  `docker compose` looks for `docker-compose.yml` in the current folder (and above), and
  the project name comes from the folder name; `docker ps` asks the Docker engine
  directly about all containers. From outside the repo: `-f <file>` or `-p
  portfolio-project`.
- **The lesson from 20.10:** rebuilding an image doesn't swap it in a running
  container (you need `up -d --build`), and a service without `restart:` lies dead after a failure
  unnoticed.

**Plan until the end of the Snowflake trial (27.10; state on 20.10: $368 of $400, so
time is the limit, not credits):** 20–21.10 `github_extract.py` + the DAG without dbt →
22–23.10 the dbt task and the first full run → 24–25.10 retries, alert, backfill → 26.10
collecting evidence (Airflow/Snowflake screenshots, dbt results, README), because after 27.10
the dbt step on Snowflake can no longer be shown live.

**Done 21.10:** PR #12 (`331777c`, squash merge in the UI): the Airflow image,
compose, `extract.py`, ADR-006, `CLAUDE.md`. After the merge, local cleanup:
`git switch main && git pull`, `git branch -D` (capital `-D`, because after a squash merge
git doesn't recognise the branch as merged), `git fetch --prune` removed 11
stale remote-tracking branches after PRs #1–#11, `git push origin --delete`
for the #12 branch. Only `main` and `origin/main` remained.

**Done (6.3, part 2, 21.10):** `github_extract.py` on `--start/--end`
according to ADR-006: `issues` with `since=start` server-side and `< end`
client-side (a window before 2008 skips `since`, because with the epoch GitHub returns
an empty result); `pull_requests` newest first: skips those newer than the window, collects the
window, stops at the first older one, and when the 20-page limit is exhausted before
reaching the start of the window it prints a warning instead of silently returning incomplete
data; `repositories` and `contributors` as a full snapshot; no state file.
The PR window logic checked on test data before running.
Live test from the running scheduler, window 20.10: repositories 1,
issues 1, pull_requests 177 (page 1 = PRs changed on 21.10, skipped;
pages 2–4 the window; no limit warning), contributors 410. **The first
write to S3 from the Airflow container** (the keys from `.env` work). Only 1 issue,
because the API returns the current state: an object changed on 20.10 and again on 21.10 lands in
the 21.10 window (a limitation described in ADR-006). The GitHub data stays in raw;
`merge_*` process only the Postgres tables (a simplification from phase 1).

**Documentation error (mine):** the updates from 20.10 (§26 and cheat sheet
31.5–31.7) I reported as saved in the repo, but the versions from 7.10 reached the repo.
I was saving the files under the same temporary path as on 7.10, and on saving the
old copy went in. Detected on 21.10 by the file sizes, fixed on 21.10 with
verification of the content in the repo. Conclusion: after every write check the content, not
just the success message.

**Questions from this step and what they teach** (theory: cheat sheet 12.12, 15.3.1):
- **I did a squash merge in the UI — what next in the terminal, and `switch` or
  `checkout`?** Local git doesn't know about the merge on GitHub: `git switch main && git
  pull` (fetches the squash commit), then you delete the branch with `git branch -D` (capital D,
  because after a squash git won't recognise it as merged). `switch` does the same as
  `checkout` for branches, but `checkout` can also overwrite files, so
  `switch` is safer.
- **`fetch`, `pull`, `prune` — how do they differ, and where do the old `origin/*`
  branches come from?** `fetch` downloads GitHub's state into the `origin/*` copies (remote-tracking
  branches) without touching your files; `pull` = `fetch` + merging into the current
  branch. Branches deleted on GitHub stay locally as `origin/*` until
  you run `git fetch --prune`; for us it removed 11 old ones after PRs #1–#11.
- **Usually a ready-made connector does this — why our own script, and what is dlt?**
  The choice is a calculation of the cost of MAINTENANCE, not of writing (AI only lowers the
  latter): Fivetran/Airbyte with many standard sources (you pay per
  row, you don't write code), dlt (a Python library: pagination, state,
  schema done for you) when you want code without writing infrastructure, your own script
  for an unusual source or when the goal is to show the mechanics, as here.
  dlt is not DLT (Delta Live Tables in Databricks, transformations).
- **Then why not dlt now?** It would hide the pagination and windows that the portfolio
  is meant to show, and there is no time before the end of the trial. After 27.10: GitHub on dlt next to
  the script and a comparison in ADR-007.

**Done (6.4, part 1, 21.10): DAG `dags/ecommerce_pipeline.py` without dbt.**
```
extract_postgres ──> has_new_rows ──> validate_raw ──> merge_dimensions ──> merge_facts
extract_github   (in parallel, ends in raw)
```
- `extract_*` and `validate_raw` via `BashOperator` (the dependencies are in the Airflow
  image), `merge_*` via `DockerOperator` from the image `portfolio-spark:latest`
  (Java/Spark/Iceberg only in the Spark image); mounting `scripts/` from the host
  path (`HOST_PROJECT_DIR`), AWS keys in `private_environment`,
  `mount_tmp_dir=False` (Docker Desktop), `auto_remove="success"`.
- Windows from Jinja templates: `extract_postgres` with a 10-minute lookback,
  `extract_github` without (the `now()` problem concerns only Postgres).
- `extract_github` without downstream dependencies: `validate.py` and `merge_*` handle
  only the Postgres tables, so wiring it into the validation would suggest a
  non-existent dependency.
- `@daily`, `start_date` 1.09 (originally 18.10, changed after the test, see
  below), `catchup=False`, `max_active_runs=1` (no overlapping MERGEs on the
  same Iceberg tables), `default_args.depends_on_past=True` (day N
  waits for the success or skip of day N-1; no silent gaps).
- **Quality gate:** `validate.py`, after checking all tables, exits
  with code 1 if anything went to quarantine, so the task fails and `merge_*`
  doesn't run. The bucket from an env var (decision 8).
- **The first TaskFlow task:** `@task.short_circuit has_new_rows` reads from XCom
  the last line of `extract.py` ("Done. N rows…", `BashOperator` stores it
  itself); 0 rows = everything downstream marked as skipped (no JVM, and after
  adding dbt also no Snowflake credits). When the line can't be read,
  it doesn't skip (safer to run than to lose data).
- **Known weaknesses of the validation (fix after 27.10):** `validate.py` checks
  the table's newest file, not the files of the current run (fix: paths from
  extract via XCom), and `merge_*` read the whole raw layer including the
  rejected rows (fix: skipping the quarantine). The gate stops a bad run,
  but it doesn't remove bad rows from later merges.

**Questions from the DAG review and what they teach** (theory: cheat sheet 31.8–31.12, 32):
- **Why extract via `BashOperator`, but merge via `DockerOperator`?**
  The operator is chosen by where the code can run: extract and validation
  need only libraries from the Airflow image, merge needs Java/Spark from the
  Spark image, so it gets its own temporary container. The scripts don't import
  Airflow, so they also work without it.
- **What do `SCRIPTS_DIR`, `HOST_PROJECT_DIR`, `SPARK_IMAGE` mean?** The first is the
  path to the scripts INSIDE the Airflow container (a mount from compose). The second is
  the repo path on your MAC: `DockerOperator` asks Docker on the Mac for a new
  container, and Docker only understands host paths. The third is the name of the Spark image.
- **Why `{{ data_interval_start }}` in braces?** It is a Jinja template. The scheduler
  reads the DAG file every ~30 s, when no run exists yet; the run's
  values are filled in only when the task executes. That is also why the code at the top
  level of the file must be light.
- **Why `max_active_runs=1` and `depends_on_past=True`?** The first = one run
  at a time (no parallel MERGEs on the same Iceberg tables and several JVMs on a
  laptop). The second = day N waits for the success of day N-1, so an error stops
  the following days instead of leaving a silent gap in the data.
- **Why `start_date` 1.09, and why did the test for 13.09 do nothing?** A run date
  before `start_date` produces a run with no tasks (marked as success), and the seed data
  ends on 15.09. Plus a trap: a manual run for date D processes day D-1.
- **Why does `extract_github` have nothing after it?** The GitHub data lands only
  in raw (validation and merge handle the Postgres tables), and wiring it before
  the validation would mean that a GitHub API failure blocks the whole pipeline.
- **TaskFlow, TaskGroup, XCom — which is which, and should the scripts move to
  `@task`?** TaskFlow is a way of writing a task (`@task` on a function), a TaskGroup is
  a folder in the UI, XCom is a small store in the Airflow database for values between tasks.
  The scripts stay scripts: Spark has to run in its own container anyway, data flows
  through S3, and `@task` stays for orchestration decisions (decision 9).
- **When does `has_new_rows` return `False`, and why does "I can't read it" give
  `True`?** `False` only with 0 rows (then the rest is skipped, not
  failed). When the line can't be read, it lets the run through: an unnecessary run costs
  a minute of Spark, while an unnecessary skip is a silent loss of data
  (fail-safe).
- **`pendulum` or `datetime`, why a helper function for `DockerOperator`, what is
  `GITHUB_REPO`?** Both date types work; what matters is the time zone (`pendulum`, because
  Airflow uses it internally). A function, because both Spark tasks share 6 of 8
  parameters. `GITHUB_REPO` is the public repo we fetch data from through the
  API, set by an environment variable with a default value.
- **How do I read the test commands (`2>&1 | grep`, `;`, `airflow dags test ...`)?**
  Airflow logs go to the error stream, and `|` passes only the normal output,
  so `2>&1` merges both before the filter; `;` always runs the next command,
  `&&` only after a success. The CLI is structured as program → group → command → arguments,
  like `git` or `docker compose`.
- **Merge commit, squash or rebase for a PR?** We stay with squash: one PR =
  one commit on `main`, easy to revert (cheat sheet 12.3).

**Test 21.10, the path without data:** `airflow dags list-import-errors` → no
errors. `airflow dags test ecommerce_pipeline 2026-10-20`: `extract_postgres`
0 rows, `extract_github` 580 (the same window a day earlier gave 589, because some
PRs were changed again and moved to the next window; the limitation from
ADR-006), `has_new_rows` → `False`, `validate_raw` and `merge_*` skipped.

**Two traps from the 21.10 test:**
- `dags test` with the date 13.09 created a run **without any task** and marked it
  as a success: the logical date was before `start_date` (18.10). My design
  error: `start_date` was chosen for a backfill of 18–20.10, while the data in
  `postgres-source` ends on 15.09 (a seed with past dates; most
  changes on 11–13.09). Fix: `start_date` = 1.09, the backfill in 6.6 on 11–13.09.
- **A manual run's window differs from a scheduled run's window.** Schedule and
  `backfill`: logical date D = window `[D, D+1)`. A manual run (`dags test`,
  Trigger in the UI): window `[D-1, D)`, i.e. the last completed one before D (in the log:
  `data_interval_start=2026-09-12, data_interval_end=2026-09-13` for D=13.09).
  So the test "for 20.10" in practice processed 19.10. To test 13.09
  manually, you pass 14.09.

**Test 21.10, a full run (window 13.09):** `airflow dags test
ecommerce_pipeline 2026-09-14` → `data_interval` 13.09–14.09.
`extract_postgres` 26 rows (customers 1, products 1, orders 4, order_items
10, payments 4, order_status_history 7), `has_new_rows` → `True`,
`validate_raw` 26/26 valid (the gate let it through), `merge_dimensions` and
`merge_facts` in Spark containers via `DockerOperator` (~40 s each). Iceberg
after the MERGE: customer 51, product 30, orders 200, order_items 502, payments 160,
order_status_history 493, **identical to before the run**: the rows from 13.09 were already
in Iceberg, and the MERGE didn't duplicate them (proof of idempotency). The DAG works
end to end Postgres → S3 → validation → Iceberg. Logs duplicated (logging to a file and
the terminal in `dags test`; `DockerOperator` passes the container's logs twice).

**Git 22.10:** PR #13 merged (the DAG). The commit with documentation landed on
the branch of an already merged PR (a push after the merge neither creates nor updates
a closed PR), so it was moved with `git cherry-pick` to a new branch from `main` →
PR #15. For the future: add to the PR before clicking merge.

**Done (6.4b, 22.10): the dbt task in the DAG.**
- The container connects to Snowflake without any secret in the repo: the template
  `dbt_portfolio/profiles.yml.example` (only `env_var()`) mounted as
  `profiles.yml`, the RSA key mounted read-only from the host path in
  `.env`; the `DBT_SNOWFLAKE_*` variables moved to `.env` (previously they were only in the
  terminal session, invisible to `docker compose` from a new terminal).
- The profile now has the targets `dev` (`dbt_dev_*`) and `prod` (`dbt_prod_*`);
  `PIPELINE_ENV` (default `dev`) chooses the target. TaskGroups `ingest`,
  `spark_merge`, `dbt`.
- `dbt debug` from the container: connection OK; `git [ERROR]` irrelevant (git is
  needed only for packages from git repositories; `dbt_utils` is from dbt Hub and
  already installed).

**Incident 22.10: since 7.10 Snowflake hadn't seen changes in 5 tables.**
- Symptom: the first run with dbt → 7 `unique` tests on staging failed
  (`orders`, `order_items`, `payments`, `customers`, `products`), dbt skipped the
  dependent models (`SKIP=22`, `fct_orders` not rebuilt), the task and the run
  failed. Spark at the same time: 200/502/160/493, no duplicates.
- Diagnosis step by step: (1) no delete files in Iceberg (only data
  files, copy-on-write), so not merge-on-read; (2) Snowflake: "parquet file
  … `fact_orders/` … inaccessible", i.e. the old path from before §25, deleted from
  S3; (3) Glue points to metadata `00005` (22.10), Snowflake to `00003` (7.10);
  (4) `SYSTEM$AUTO_REFRESH_STATUS`: `GENERALIZED_PIPE_STOPPED` since 7.10 11:38 for 5
  tables, `RUNNING` only for `order_status_history` (the MERGE only appends);
  the cause "invalid file … was found in the Iceberg metadata snapshot".
- Mechanism: `rewrite_table_path` kept the snapshot IDs but
  changed the paths; Snowflake refreshes incrementally by snapshot and rejected a
  file with a new path in a "known" snapshot. The verification in §25 covered Glue and
  Spark, not Snowflake.
- Fix: a manual `REFRESH` returned the same error, so the
  `portfolio_ecommerce_db` was recreated with the statement from `GET_DDL` (after an inventory of grants and
  policies), then the policies on `customer`, then the grants (the analyst had no
  access without the policies). The block with the policies was first run too early
  (the tables hadn't been discovered yet, `sync_interval_seconds = 30`); the error
  also stopped the grants, so there was no window without policies.
- Verification: Snowflake 51/30/200(200 unique)/502/160/493, metadata
  `00005`, refresh `RUNNING`; analyst: 2 rows, both DE, email
  `***@example.org`; a repeated `airflow dags test ecommerce_pipeline 2026-09-14`:
  all tasks SUCCESS, **dbt `PASS=51 ERROR=0`**.
- Lessons: (1) after physically moving a table, check every engine, not
  just the catalog, and do it before deleting the old files (ADR-005, update
  2026-10-22; `CLAUDE.md` decision 6); (2) the silent stop of the refresh gave
  no alert for two weeks, and only the dbt tests caught it: a candidate
  for monitoring in phase 9 (e.g. a daily check of
  `SYSTEM$AUTO_REFRESH_STATUS` for all tables).

**Questions from this day and what they teach** (theory: cheat sheet 12.13, 25.17, 27.3,
31.13, 31.14, 32.4, 32.5):
- **How does `portfolio_ecommerce_db` (a link icon) differ from `portfolio_dbt`, and
  a view from a table?** The first is a catalog-linked database: it stores nothing,
  it shows the Iceberg tables from Glue and refreshes their metadata. The second is an ordinary database in
  which dbt builds its objects. A view is a saved query computed on
  every read (a recipe), a table is a saved result (a ready dish); that is why the
  staging views immediately showed duplicates when Snowflake was reading two snapshots.
- **What stopped the pipeline, and is `dags test` a dry run?** dbt, as part of
  `build`, skipped the models depending on the bad test, and Airflow sees only the
  task's exit code, so it marked the task and the run as failed. `dags test` executes
  everything for real (S3, MERGE, Snowflake); safe only thanks to
  idempotency.
- **What does a test do, and does it write anything? What are RLS and `int_`?** A test is a SELECT
  looking for bad rows (0 = PASS), and by default it writes nothing. RLS (a row
  access policy) filters rows depending on the role, e.g. the analyst sees only
  DE. `int_` = intermediate, the layer between staging and a mart.
- **Where does `PIPELINE_ENV` come from, and how do I do dev/prod "like in a corporation"?** A variable from
  `.env` → compose → the container; the DAG passes it as `--target` to dbt
  (`dbt_dev_*` / `dbt_prod_*`). In a corporation there is one codebase, and environments differ
  by configuration files and separate deployments. A separate S3 bucket for
  prod postponed until Terraform (phase 8): the prod Iceberg can't be rebuilt from
  raw, because a merge reading everything at once would collapse the SCD2 history.
- **Why didn't `git push` create a PR?** A push only sends commits; a PR is created by
  `gh pr create` or the button in the UI. A push to the branch of an already merged PR goes
  nowhere, hence a new branch from `main` and `git cherry-pick <hash>` (copies one
  commit).
- **Why md5 checksums when saving files?** A checksum is a fingerprint of the file's
  content: before saving it confirms that nobody changed the file, after saving that
  the repo really has the new version (the lesson from 20.10).
- **The commands `env`, `sed`, `printf >>`, `grep -l`, `diff`, `2>/dev/null`** —
  used to move the `DBT_SNOWFLAKE_*` variables into `.env` without displaying the
  values (cheat sheet 32.4).

**Done (22.10): dbt tests extended** (inspiration: a LinkedIn post about
dbt tests; cheat sheet 25.18–25.19).
- Staging: `relationships` ×5 (every foreign key has a parent),
  `accepted_values` ×4 (order statuses, payment methods and statuses),
  `dbt_utils.expression_is_true` ×4 (`valid_to > valid_from` in SCD2,
  `quantity > 0 AND unit_price >= 0`, `amount >= 0`), freshness on the sources
  `orders` and `payments` (`warn_after` 2 days, without `error_after`).
- `fct_orders`: non-negative amounts (error) and "paid + refunded ≤ order
  value" as `severity: warn` (a customer can pay again after a refund);
  `store_failures: true` on the business rules.
- Unit tests: `int_customer_versions` (the validity windows of versions) and
  `fct_orders` (payment statuses, an order without line items → 0, the country at the time of
  the order LY → FR, the flag `is_before_first_customer_version`).
- DAG: `dbt.dbt_source_freshness` before `dbt_build` (only warns); in `prod`
  `dbt build --exclude-resource-type unit_test` (a dict `DBT_BUILD_FLAGS`,
  to be moved to `config/<env>.yaml`). The whole flow in one line of
  dependencies.
- Result: `dbt build --target dev` → **`PASS=68 ERROR=0`**, after adding the unit
  test for `fct_orders`, `--select fct_orders` → `PASS=18`; freshness `WARN` for both
  sources (the data ends on 15.09, expected); `list-import-errors` empty.
- My mistake: I assumed that the rule "≤ order value" would give a `WARN` on the
  seed data; it passed, because the seed generates payments consistent with the order
  values. The comment in `schema.yml` corrected.
- Known risk (until 6.5): dbt starts right after `merge_facts`, and Snowflake
  refreshes the Iceberg metadata every 30 s, so it may read the previous snapshot.
  To consider: a sensor on the metadata version or a `REFRESH` before dbt.
- Tidying up the cheat sheet: a duplicated number 25.16 → "Databases in Snowflake" is
  now 25.17.

**Questions about the tests and the DAG and what they teach** (theory: cheat sheet 25.18, 25.19, 31.15, 32.6):
- **Why dependencies in one line, and how do I read a DAG?** `a >> b` = b starts
  after a succeeds; it reads left to right, and the whole flow in one place is
  more readable than dependencies scattered around the file. Tasks with nothing before them
  (roots) start in parallel.
- **Why a TaskGroup, if `extract_github` blocks nothing?** A TaskGroup is a
  folder in the UI grouping tasks by stage (ingest / spark_merge / dbt), not a
  dependency; `extract_github` is in ingest, because it is the fetching stage.
- **Why tests in dbt, not in Airflow, and are they part of the DAG?** dbt knows the
  models and their dependencies, a test sits next to the model, and `build` skips only what
  depends on a bad model; Airflow sees only the exit code. The tests are in the DAG
  through the `dbt_build` task; runs from the terminal are checks while
  writing.
- **Why is the data in a unit test hard-coded, is it only SCD2, dev
  or prod?** A unit test checks SQL logic on hand-picked edge
  cases (like pytest), so the data changes only together with the logic. It concerns
  one model (for us SCD2 and `fct_orders`) and runs in dev/CI. Data tests
  check real data, so they run everywhere, including prod.
- **Why "18" for one table, and why is the command so long?** One
  command, and `--select fct_orders` takes the model with its tests: the build, the unit
  test and 16 data tests. The length of the command: `docker exec` (dbt and the key are in the
  container), `cd` + the full path to dbt (a separate venv), `grep` to filter the
  log. Day to day: `docker exec -w <directory> ...`, and in a corporation dbt locally
  from a venv or through a Makefile.

**Questions about CI and the state of the project** (theory: cheat sheet 31.16):
- **Which "manual tests", and why doesn't a PR run `dbt build`?** Manual tests are
  commands you run and paste (`dbt build`, `list-import-errors`, `dags
  test`). The repo has no CI: GitHub without `.github/workflows/` runs nothing.
  In a corporation every PR automatically goes through lint, a DAG import test and
  `dbt build` of the changed models in a CI schema, and the merge is blocked until
  the checks are green. For us that is phase 7.
- **How much is left?** State on 22.10: phases 0–5 done, phase 6 about 70%; then 7
  CI/CD, 8 Terraform, 9 observability. After 27.10 without Snowflake, so phases 7–9
  on AWS/Spark/Airflow.

**Done (22.10): dev and prod as configuration, two deployments.**
- `config/dev.yaml` and `config/prod.yaml`: only what differs (the schedule:
  dev 00:00, prod 03:00 UTC, so that two MERGEs into the same Iceberg tables don't run
  at once; prod `--exclude-resource-type unit_test`). The dbt target = simply
  `PIPELINE_ENV`. The DAG, when
  loaded, reads `config/<PIPELINE_ENV>.yaml`; a missing file = an import error, not
  silent defaults. There is no `if prod` anywhere in the code.
- `docker-compose.yml`: `./config` mounted read-only into Airflow;
  `PIPELINE_ENV: dev` hard-coded (no longer from `.env`); a second Airflow in
  the `prod` profile (its own metadata database, init, UI on 8081 labelled "PROD",
  scheduler, logs in `logs/prod`), started only with `docker compose
  --profile prod up -d`.
- Test: dev `No data found`, schedule "At 00:00"; prod `No data found`,
  "At 03:00" — the same DAG file, different behaviour purely from configuration.
  The DAG in prod paused until its first run.
- A trap (23.10): after editing the files in `config/`, both schedulers reported
  `FileNotFoundError: .../pipeline_config/dev.yaml`, although the files were on the Mac.
  `ls` in the container showed an empty folder (`total 0`): Docker Desktop lost
  its view of the mounted directory. Fix: `docker compose --profile prod up -d
  --force-recreate <Airflow services>`; the databases untouched. A good sign: the missing
  config gave a loud import error, not silent defaults. Rule: after
  changing a mounted config, check the import, and on an error recreate the
  containers (in a company a config change means a new deployment anyway).
- To improve: compose built a separate image for every service (identical,
  from the cache). In a company one image with one version goes to all
  environments; to be done with a shared `image:` in the Airflow services.

**Questions about dev/prod and what they teach** (theory: cheat sheet 31.17):
- **What did I agree to in the successive changes, and what did `docker compose up
  -d` do?** Three small steps instead of one big command (you rejected the earlier one, with 100
  lines of configuration at once, because it couldn't be reviewed). `up -d`
  recreated only the containers whose mounts changed (`Started`),
  and left the rest (`Running`): mounts are set only when a container is
  created.
- **How will prod override `PIPELINE_ENV: dev`?** The shared variables are in compose once,
  under the YAML anchor `&airflow-common-env`. Prod pastes them in (`<<:
  *airflow-common-env`) and adds its own; with a repeated key the later one
  wins. Only `PIPELINE_ENV` and the metadata database address change.
- **Where do environment variables come from?** The route is always the same: the source
  (`.env` for secrets, a constant in compose, `${PWD}` from the terminal) → the
  `environment:` section in compose → the env of the process in the container → the reader (Airflow itself,
  the scripts through `os.environ`/`boto3`, dbt through `env_var()`, the DAG, which passes the AWS
  keys on to the Spark container). The full table is in the cheat sheet.
- **How does `PIPELINE_ENV` change from dev to prod?** It never changes. There are
  two sets of containers, each with the value hard-coded from the start; the same
  DAG does something different because it reads a different configuration file. "Moving to prod" is
  starting the second set, and in a company: the same code version deployed by
  CI to a separate Airflow.
- **Why didn't the search find `PIPELINE_ENV=prod`?** Before prod was added,
  that value didn't exist, and the syntax depends on the place: in compose (YAML)
  `PIPELINE_ENV: prod`, in `.env` and in a running container `PIPELINE_ENV=...`;
  `.env` is in `.gitignore`, so VS Code doesn't search it.
- **Can this be done like in a company?** Closer: one compose template for
  Airflow and a values file per environment (`env/prod.env`), each deployment as a
  separate project (`docker compose -p portfolio-prod --env-file ...`), which requires
  moving the source Postgres and Spark into a separate file (a few hours). Fully:
  two Airflows in the cloud on separate accounts (phase 8). The profile variant was chosen
  because the trial ends on 27.10.
- **Why separate `PIPELINE_ENV` and `DBT_TARGET`?** The first version read the
  dbt target from the config (`dbt.target`), "just in case" the names of environments and
  targets diverged (e.g. a separate CI environment writing to the `dev` target,
  a target per developer). Here both always have the same values, so it was an
  unnecessary complication: withdrawn, `--target {PIPELINE_ENV}`. The rule from the ADRs:
  don't add complexity without a real need. By the way: `target: dev` in
  `profiles.yml` is only the default target (when a command has no `--target`), and
  `outputs` is the list of available targets.
- **Do dev and prod have a separate Snowflake / Databricks?** Snowflake: usually one
  account, environments separated by databases, roles and warehouses (separate accounts
  mainly under strict regulation). Databricks: the standard is separate
  workspaces for dev and prod within one account, plus catalogs per environment in
  Unity Catalog. For us one database `PORTFOLIO_DBT`, schemas `dbt_dev_*` and
  `dbt_prod_*`.
- **Does this mean I didn't share the folder with the container?** No: the mount
  is in compose, Docker Desktop shares the whole `/Users` by default, and at 22:58
  the DAG read `dev.yaml` without an error. What failed was the view during operation: I changed the files
  from a different layer than VS Code, and Docker Desktop didn't notice.
  Diagnosis: `ls` in the container shows something different than on the Mac.
- **What does `--force-recreate` do?** It removes the given containers and creates new ones from the
  same image and configuration, even when nothing changed (a plain `up -d`
  does so only on a change). Everything outside the container survives (volumes with
  databases, mounted folders); only what was written inside disappears. In a company:
  restarting a deployment (`kubectl rollout restart`); containers are stateless.
- **Do I have a separate scheduler for dev and prod?** Yes, each environment has its own
  set: a metadata database, init, webserver, scheduler (dev without a suffix, because it
  came first; prod with `-prod`). Shared are `postgres-source` and
  `spark-iceberg`. In a company the names don't mix, because they are separate clusters.
- **What is Helm?** A deployment manager for Kubernetes: a chart (a template of the whole
  Airflow) + `values-dev.yaml`/`values-prod.yaml`; CI runs `helm upgrade` after a
  merge. The same idea as our compose block + the prod overrides.

**Questions about data quality and the Advanced part of the plan (23.10):**
- **Where is Great Expectations?** In the plan, in the Advanced part (item 13),
  as an extension on top of `validate.py` in the raw layer; in phase 2 a
  lightweight custom validation was deliberately chosen. In companies GE is rarer today: where there is dbt,
  dbt tests are usually enough (+ `dbt-expectations`, Elementary), Soda as
  rules in YAML, GE mainly in Python/Spark pipelines before the warehouse.
- **What else is in Advanced?** 14 Elementary/observability (overlaps with
  phase 9), 15 a comparison with Databricks in `/experiments/databricks` (the Free Edition
  doesn't expire), 16 extended governance (partly done: RLS + masks on `customer`
  and `fct_orders`; further only until 27.10).
- **GE or dbt tests, or several at once?** Decision: several tools on the same
  rules (key, allowed statuses, amount ≥ 0, volume), so that the differences are
  visible directly: our own `validate.py` (the reference point), GE and Soda Core on raw
  (after 27.10, instead of just fixing `validate.py`), 2–3 `dbt-expectations` tests
  in Snowflake (before 27.10, in 6.7). A comparison in an ADR, like dlt.
- The control questions from the project plan ("what if the pipeline runs
  twice?", "how do you backfill 3 months?", "how do you detect silent data
  corruption?") will serve as the skeleton of the README in 6.7.

**Git 23.10:** PR #18 merged too early (without removing `target`), the second
commit went to the branch after the merge (the same lesson as 22.10) → PR #19; documentation
→ PR #20. In #20 a conflict in `PROGRESS_LOG.md`: the same fragment changed differently
on the branch and on `main`, and git doesn't guess which version to take. Resolved in the UI
("Accept current change": on the `main` side the fragment was empty, so nothing was
lost), check `grep -c "<<<<<<<" PROGRESS_LOG.md` → 0.

**Questions about Asana and what they teach** (theory: cheat sheet 31.18):
- **Can Asana be used in the project?** Yes, in two places. (1) In
  6.5 as the alert target: `on_failure_callback` creates a task in the project "Data
  incidents" through the Asana API. (2) After 27.10 as a third data source (tasks,
  projects): the `modified_since` filter and the events API with a sync token are
  a different incremental fetching pattern than Postgres and GitHub; dlt has a ready
  Asana source, so it is a natural candidate for the comparison in ADR-007 (GitHub
  with our own script, Asana through dlt).
- **Will the alerts appear in Asana immediately?** Yes, after the task's final failure
  (after all retries) the callback sends one `POST /tasks` request and the task
  is in Asana within a few seconds. It is a one-way flow (Airflow → Asana),
  not a synchronisation. The first version doesn't close the task after a successful run and doesn't
  merge repeats; next steps: an `on_success_callback` closing the open
  task and a check "does an open task for this task already exist"
  (then a comment instead of a new task). In a company: Slack/PagerDuty for
  notifications, a ticket (Jira/Asana) for incidents to resolve.
- For you to do: a free Asana account (the API is in the Personal plan),
  the project "Data incidents", a Personal Access Token; in `.env` `ASANA_TOKEN` and
  `ASANA_PROJECT_ID` (the number from the project's address). The token never in the repo.

**Done (6.5, 23.10): retries with a growing delay, `execution_timeout`, an alert to Asana.**
- `config/dev.yaml` and `prod.yaml`: a `retries` block (dev: 2 retries, delay from
  30 s, limit 5 min; prod: 3, from 2 min, limit 30 min, because at 03:00 nobody is
  watching) and `execution_timeout_minutes` per stage (extract 10, validate 10,
  spark_merge 15, dbt 20; about 5–10× the normal time; the same in both environments,
  because the data is shared — in a company prod's volume would decide).
- DAG: `retries`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`
  and `on_failure_callback` in `default_args` (they cover every task),
  `execution_timeout` per task. `validate_raw` and `has_new_rows` have
  `retries=0`: their failure is deterministic (quarantine, an XCom read),
  a retry would give the same result. Retries are safe because every task is
  idempotent.
- `dags/alerting/asana.py`: the callback is called once, after the LAST failed attempt
  (also after a timeout), and sends one `POST /tasks` to the project "Data
  incidents": the title `[env] dag.task failed (window …)`, in the notes the run, the window,
  a link to the log and the error. It never raises an exception (that would hide the original error in
  the log); a missing token = a loud log entry, without failing the task.
  `dags/.airflowignore`: the scheduler doesn't parse the module as a DAG.
- Secrets: `ASANA_TOKEN` (a PAT from a private account) and `ASANA_PROJECT_ID` (the number after
  `/project/` in the address) in `.env`, passed by compose to both Airflows.

**Live test 23.10 (dev, a scheduled run, window 22.10):**
`docker compose stop postgres-source` as a realistic transient error (the source
database doesn't respond), without changing code.

| Attempt | Start (UTC) | Delay | Result |
|---|---|---|---|
| 1 | 18:40:10 | — | `UP_FOR_RETRY` |
| 2 | 18:40:43 | 33 s (expected 30–60) | `UP_FOR_RETRY` |
| 3 | 18:42:32 | 109 s (expected 60–120) | `FAILED` → a task in Asana after 1 s |

One task in Asana (not three), landed in the project's first section;
`extract_github` succeeded despite the Postgres failure, the rest `upstream_failed` (no
Spark and dbt). The window in the log `2026-10-21T23:50` → `2026-10-23T00:00` (day 22.10
with the lookback). The second half: `docker compose start postgres-source` + `airflow
tasks clear ... -t '^ingest\.extract_postgres$' -d -s 2026-10-22 -e 2026-10-22
-y` → the run green, `has_new_rows` = False (0 rows in the 22.10 window, the seed ends
on 15.09), the rest `skipped`. The Asana task closed manually (the first
version works one way only).

**Discovered during the test: a `depends_on_past` stall after the TaskGroups.**
- Symptom: the run `scheduled__2026-10-22` in the `running` state since 18:27, all
  tasks without a status, and through `max_active_runs=1` it blocked the following runs. `airflow
  tasks failed-deps`: "depends_on_past is true …, but the previous task instance
  has not run yet".
- Cause: a TaskGroup changes the `task_id` (`extract_postgres` →
  `ingest.extract_postgres`). With `catchup=False` the "previous run" is
  `manual__2026-10-20` (the `dags test` from 21.10, from before the TaskGroups), which has no
  instance with the new ID, so Airflow waited forever. On the Grid you can see it in
  the 13.09 run: green `has_new_rows`/`validate_raw` (outside the groups), empty `extract_*`.
- Fix: deleting the test run `manual__2026-10-20` from the metadata (Browse →
  DAG Runs → Delete; not `airflow dags delete`, which wipes the DAG's whole history).
  The data in S3/Iceberg/Snowflake and the log files untouched. The previous run became
  `manual__2026-09-14` (re-run on 22.10 with the new IDs), and the scheduler immediately
  started the task.
- Lessons: (1) changing a `task_id`, including through a TaskGroup, breaks the task's history
  (`depends_on_past`, durations); in a company: don't change IDs in a running
  DAG (`prefix_group_id=False`) or deliberately cut the history at deployment
  (a one-off run with "Ignore Task Deps"); run history in prod is never
  deleted (audit). (2) A stall is silence, not a failure, so the Asana alert doesn't
  catch it; a candidate: `dagrun_timeout` + a DAG-level callback.
- My mistakes: commands listed with commas in a sentence (pasted into the terminal they didn't
  work; from now on commands only in a code block, in a pasteable form) and the date
  from the UI (23.10 = the end of the window) instead of the logical date (22.10) in `failed-deps`.

**Git 23.10:** `git push origin --delete` for `feat/dbt-tests`,
`feat/dev-prod-config`, `feat/airflow-dbt-task` → "remote ref does not exist":
they were no longer on GitHub, only the local `origin/*` copies remained, which
only `git fetch --prune` cleans up. The local `feat/dbt-tests` deleted (checked
beforehand that nothing from it was missing on `main`).

**Questions from this step and what they teach:**
- **Should the Asana account be created with a fake email (e.g. `PG_ASANA@PG.com`)?** No:
  Asana requires confirming the email, and a real company's domain can attach the account
  to that company's organisation in Asana and looks like impersonation. For a portfolio
  a PAT from a private account is enough; a separate "bot" identity = a Gmail alias
  (`mail+asana-bot@gmail.com`).
- **How is this done in a company?** A technical account is created by IT/an admin on request (a ticket,
  an owner, approval, a name like `svc-data-alerts`). Two options: an Asana service
  account (Enterprise plans, created by a super admin, API only, but with full
  permissions it sees all of the organisation's data, so rarely for a narrow
  integration) or, more often, an integration user: a real functional mailbox
  in the company's domain, a licence, an invitation to only one project, a PAT from its
  account (least privilege). The token in a vault (Key Vault / Secrets Manager /
  HashiCorp Vault), Airflow reads it as a Connection through a secrets backend; rotation
  and access reviews. Often instead of Asana: an alert on Slack/Teams/PagerDuty,
  a ticket in Jira/ServiceNow.
- **`,` vs `&&` vs `;`?** A comma is not shell syntax (git got the whole line
  as its arguments). `&&` = the next command only after the previous one succeeds,
  `;` = always. In a company one-off steps are typed one by one; `&&` is in
  scripts/Makefiles/CI.
- **What did the git commands do, and what fixed the old branches?** A branch lives in three
  places: locally (`feat/x`), as a local copy of GitHub's state (`origin/feat/x`,
  updated only by `fetch`/`pull`) and on GitHub. A plain `fetch` deletes
  nothing; `--prune` removes the copies of branches that no longer exist on GitHub. In a company
  once: `git config --global fetch.prune true` + in the repo "Automatically delete head
  branches".
- **Why do we stop `postgres-source`?** Retries and the alert only work on a
  failure, so it has to be caused deliberately: the most common real reason for retries (the
  source database temporarily unavailable), one reversible command, the data in a volume,
  Airflow has a separate metadata database, the code unchanged. In a company: a test on dev/staging
  (a wrong password in the connection, chaos testing), never on a shared database.
- **Why `tasks clear`, and not running the whole DAG?** We fix this
  particular run (window 22.10) rather than adding a new one: a red run would stay in the
  history and, through `depends_on_past`, block the following days. `clear` resets the state
  of the chosen instances (`-t` a name regex, `-d` with downstream, `-s/-e` which runs,
  `-y` without confirmation), the scheduler runs them again (a new attempt =
  `attempt=4`), `extract_github` (success) untouched. Safe thanks to
  idempotency. In a company: the "Clear" button with "Downstream" in the UI.
- **Why is the rest `skipped`?** The 22.10 window has 0 rows, `has_new_rows` (the
  task itself = success) decided "nothing to do" and the short-circuit skipped the rest: no
  JVM and no Snowflake credits, the run green.
- **Where does dbt in Airflow come from, and do I have two dbts?** Airflow doesn't contain dbt; we added it
  ourselves in 6.1 to our image (`airflow-docker/Dockerfile`, a separate venv
  `/opt/airflow/dbt_venv`), because `BashOperator` with `LocalExecutor` runs the command
  in the scheduler's container, so the `dbt` program has to be there. There are two installations, in
  the same versions: `.venv` on the Mac (work from the terminal, `~/.dbt/profiles.yml`) and
  the venv in the image (the DAG, the profile from the repo + `.env`); the model code is shared. "dbt from Airflow"
  and "dbt from Docker" are the same installation. In a company: one image from CI used
  everywhere, or dbt in a separate image (`DockerOperator`/`KubernetesPodOperator`),
  or Astronomer Cosmos (every dbt model as a separate Airflow task).
- **How does `DockerOperator` work, and do Java/the JARs force it?** Code in the scheduler,
  through `docker.sock`, asks the Docker engine on the Mac for a NEW, temporary container from the
  image `portfolio-spark:latest` (mounting `scripts/` from the Mac's path, the AWS
  keys, the command `python3 merge_*.py`), started next to the scheduler, not inside it
  (Docker-out-of-Docker); the operator passes on the logs and waits for the exit code, and after
  a success the container disappears. The permanent `spark-iceberg` is a different container from the same
  image; the DAG doesn't use it. Java doesn't force `DockerOperator`, it is a choice: Java +
  Spark + JARs in the Airflow image = a heavy image, version conflicts, rebuilding
  Airflow on every Spark change. In a company Airflow doesn't compute at all, it only
  delegates to a cluster (`SparkSubmitOperator`, EMR, Glue, Databricks,
  `KubernetesPodOperator`), like ADF starting a job in Databricks.
- **Why do I close the Asana task manually?** The first version only sends a `POST`
  on a failure. Auto-closing requires an `on_success_callback`, finding the
  task (a stored ID or searching for an open one by name), a `PUT` with
  `completed: true` and a condition so as not to query Asana on every success; plus
  deduplication (a comment instead of a new task). In a company an alert (Slack,
  PagerDuty) closes itself when the problem goes away, while an incident ticket is closed by a
  person after establishing the cause (whether there is a gap in the data, why the database went down).
- **Where is the project ID in Asana, and how do I make a PAT?** (when wiring up the
  alert) The ID is the number after `/project/` in the project's address
  (`app.asana.com/1/<workspace>/project/<ID>/list/...`; the first number is the
  workspace, not the project); in the desktop app without an address bar: the arrow
  next to the project name → "Copy project link". PAT: avatar → Settings → Apps →
  View developer console → Personal access tokens → "+ Create new token"; the token
  is shown only once. A PAT works like a password to the whole account, tasks are created "as
  you"; in a company a technical account's token from a vault.
- **Why does VS Code show my changes "on main", if I didn't
  commit anything?** (after editing files, before creating a branch) Uncommitted
  changes are in the working tree, not on any branch; `git switch -c <branch>` takes
  them along, and they reach the branch only with a commit. That's why it is enough to
  create the branch before the commit. The numbers next to files in VS Code (`8`, `1`) are
  Pylance warnings: the local `.venv` doesn't have Airflow, so the editor doesn't recognise the
  imports; the real test is `airflow dags list-import-errors` in the container.
- **Do I have to run anything during the failure test?** (after stopping the database) No:
  the scheduler starts the task by itself, retries by itself and calls the callback by itself; you watch
  the Grid, the logs of the successive attempts and Asana.
- **Why does the UI show the run as "23.10", while the CLI wants 22.10?** (with `failed-deps`)
  The Grid in Airflow 2.9 shows the end of the window (the moment from which the run could
  start), while the `execution_date` in the `run_id` is the logical date = the start of the window.
  CLI commands take the logical date or the `run_id`; the safest is to pass the `run_id`
  (e.g. `scheduled__2026-10-22T00:00:00+00:00`) from `airflow dags list-runs`.
- **Why two dbts (the Mac and the Airflow image), how is it in a company, and what is CI?**
  (a follow-up after explaining the installation) One model codebase, one program
  installed in two places where this code has to run: on the Mac for
  fast work (`dbt run --select`, the dbt extension in VS Code) and in the
  Airflow image, because that's where the scheduler runs `dbt build` by itself at 00:00. A single installation
  is possible (working through `docker exec` or a devcontainer, i.e. VS Code
  opening the repo inside a container). CI (Continuous Integration) is an automaton
  (for us it would be GitHub Actions, `.github/workflows/`), which on every PR
  runs lint, a DAG import test and `dbt build` of the changed models in a CI schema
  (the merge blocked until the checks are green), and after the merge builds the image, gives it a
  version (the commit hash) and pushes it to a registry (ECR/ACR); CD (Continuous
  Deployment) deploys THE SAME image to dev, then to prod. In a company the image is built
  once, in CI, never on a laptop, so dev and prod run bit-for-bit the same
  dbt; on the laptop: a venv from a lockfile checked in CI or a devcontainer. We don't have this,
  because there is no CI yet (phase 7) and no server or registry (dev and prod are
  two sets of containers on the Mac; building the image in CI only makes sense with
  Airflow in the cloud, phase 8). Phase 7 will start with checks on PRs and, along the way,
  one image for all Airflow services instead of a separate build per service.

**Git 23.10:** the PR from 6.5 merged (squash `15118df`), the local branch deleted;
`git pull` itself removed `origin/feat/retries-alerting` (the branch on GitHub
deleted at merge, `fetch.prune` already on).

**Done (6.6, 23.10): backfill of 11–13.09.**
- Preparation: deleted the test run `manual__2026-09-13` (the `extract_*` tasks
  under the old IDs from before the TaskGroups; the backfill would have entered this existing run).
  `manual__2026-09-14` stays (see the questions below).
- Command: `docker compose exec airflow-scheduler airflow dags backfill
  ecommerce_pipeline -s 2026-09-11 -e 2026-09-13 2>&1 | tail -n 25` (`-s/-e` =
  which runs, both bounds inclusive; Airflow computes the windows and the lookback). Result: 3/3
  runs, 24/24 tasks successful, about 4.5 min per day, days in order
  (`depends_on_past` + `max_active_runs=1`).

| Day | Window (with lookback) | Rows from Postgres | Validation | Iceberg after MERGE | dbt build |
|---|---|---|---|---|---|
| 11.09 | 10.09 23:50 → 12.09 00:00 | 18 | 0 quarantined | 51 / 30 / 200 / 502 / 160 / 493 | `PASS=69 ERROR=0` |
| 12.09 | 11.09 23:50 → 13.09 00:00 | 21 | 0 | unchanged | `PASS=69 ERROR=0` |
| 13.09 | 12.09 23:50 → 14.09 00:00 | 26 (as in the test of 21.10) | 0 | unchanged | `PASS=69 ERROR=0` |

- Freshness `WARN` every day (the data ends on 15.09, expected). No
  new task in Asana. The 13.09 run processed the same window as
  `manual__2026-09-14` (a manual run with date D = window D-1): the same day a second time,
  without duplicates.
- **What the backfill proves, and what it doesn't.** It proves: per-day windows, the lookback,
  the quality gate, the order of days, and no duplicates or false SCD2 versions
  on reprocessing (`no changed rows -- nothing to insert`). It doesn't
  prove processing "its own day": `merge_*` read the WHOLE raw layer and
  deduplicate (`orders: incoming batch (deduped) = 200 rows` for 18 rows from the
  extract), a weakness known since 6.4. The cost for a 3-month backfill: every day
  processes the whole history so far (time grows quadratically), and raw keeps growing,
  because every run appends its files (append-only with `run_id`). Fix after 27.10:
  `merge_*` read only the current run's files (paths from the extract via XCom
  or a filter on `run_id`). This is also the answer to the control question from the plan "how
  do you backfill 3 months?".
- Evidence taken on 23.10: a screenshot of the Grid (3 green `backfill__` runs) and of the closed
  Asana task.

**Questions from this step and what they teach:**
- **Why don't we delete the 14.09 run as well?** (before the backfill, after deciding to
  delete 13.09) It already has the new task IDs (re-run on 22.10 with the TaskGroups; it lacks
  only `dbt_source_freshness`, added later), it lies outside the backfill's
  range (for `depends_on_past`, 11.09 has nothing before it, 12.09 looks at
  11.09, 13.09 at 12.09, nobody looks "forward") and it is the evidence of the first
  full run with dbt `PASS=51`. You delete only what really gets in the way.
- **Where does the "new" data in Postgres come from, if nobody writes to it?** (after the
  backfill result) There is no new data. `postgres-source` contains seed data
  generated once (`seed.py`) with dates in the past up to 15.09. The extract takes the
  rows whose timestamp falls into the run's window, so the backfill only slices the
  ready history into days; 18/21/26 is the number of rows with that day across all
  6 tables. In a company the source is alive (an application keeps writing), and a backfill
  reconstructs past days with the same window rules.
- **Why does the row count differ between days, while Iceberg doesn't grow?** (as above)
  These rows have been in Iceberg since phase 3 (the first full load). The MERGE
  compares by key: facts with an existing key add nothing, SCD2
  dimensions create a new version only when an attribute changes. Iceberg will grow with a
  real change: a new order (a new key) or a customer changing country
  (a new SCD2 version). Hence the idea for the demonstration in 6.7.

**Done (6.7, part 1, 23.10): a traffic simulator in the source.**
- Decision (yours): one file `scripts/seed.py`, not a separate script; once a day
  is enough (the pipeline has a daily window, two batches would end up in one
  run anyway).
- `seed.py`: `--mode init` (the existing one, logic unchanged; history, once, on an
  empty database) and `--mode daily`: one day of activity with timestamps between
  today's 00:00 UTC and now — 1–3 new customers (Faker), 1–2 country
  or email changes for existing customers (new SCD2 versions), with a 50% chance a price change
  of a product by 5–20% (SCD2 + a new price snapshot in today's orders),
  5–15 new orders with line items, status history and in about 60% a payment,
  3–8 old open orders moved one step on (pending → paid with a payment or
  cancelled, paid → shipped, shipped → delivered). It doesn't change history, it only
  appends. Random seed: `init` 42, `daily` the date (different data every day,
  repeatable within a day). The day guard: an order created today = the
  batch is done → it does nothing (idempotency; both Airflows see the DAG). One
  transaction for everything. `DB_CONFIG` from environment variables as in
  `extract.py` (it was hard-coded `localhost`, decision 8).
- `dags/source_simulator.py`: one task `generate_daily_activity`
  (`BashOperator`), retries and the Asana alert as in the pipeline, `catchup=False`
  (events get the "now" timestamp), the schedule from the config: dev `30 23 * * *`
  (30 min before the dev run at 00:00, whose window [D, D+1) contains the new rows),
  prod `null` (manual only; both environments read the same `postgres-source`).
  It is not a task of `ecommerce_pipeline`: generating the source is not part of the
  pipeline (a backfill of the past must not append "today's" data).
- `airflow-docker/requirements.txt`: `faker` (the version from the Airflow constraints;
  locally 40.39.0 from `uv.lock`).
- Test: from the Mac `--mode daily` twice → "1 new customers, 1 customer changes,
  1 price changes, 6 new orders, 8 order status transitions", then "already
  generated -- nothing to do". Image rebuild (`--profile prod up -d
  --build`, 49 s, dev and prod recreated), `list-import-errors` empty, `dags test
  source_simulator` on the dev scheduler → "already generated" + SUCCESS (`faker`
  in the image, the connection through environment variables, the guard works from the
  container). Both DAGs in dev `is_paused = False` (`unpause` printed `True`, i.e.
  the state before the change; checked with `airflow dags list`).
- The first real increment: the dev run at 00:00 UTC on 24.10 (window 23.10) will process
  the rows generated on 23.10 from the Mac — the result to be recorded on 24.10.
- Ruff SIM117 (nested `with`) fixed in `main()`: `with connection,
  connection.cursor() as cursor:`.
- My mistake: I started editing `seed.py` on `main` (in the working tree, without a commit);
  the branch `feat/source-simulator` was created only after your remark. Rule:
  a branch before the first edit.

**Questions from this step and what they teach:**
- **Does the seed have to be historical and in a separate file?** (the idea of generating
  new records from combinations of first names, last names and countries) No: one file with two
  modes. The history already sits in the Postgres volume, `init` is only for
  setting up an environment from scratch; the daily mode only appends, so a backfill
  of past days remains a valid test. In a company production data (PII) is not
  copied to dev: synthetic data (Faker, SDV) or an anonymised copy;
  nobody writes to the source system (someone else's application) — for us `postgres-source`
  plays that role.
- **Once or twice a day?** Once: the pipeline takes the window of the whole day, so two
  batches would end up in one run. Two runs make sense only with a
  pipeline every few hours (then the guard "this day" → "this half of the
  day"). A local limitation: Airflow works while the Mac is awake; after waking up
  the simulator will generate data with the current date.
- **What exactly did you add to `seed.py`?** Two modes (`argparse`), a seed per mode,
  `DB_CONFIG` from env, the `DAILY_*` "knobs", the existing `main()` as
  `seed_initial()`, the day's functions: drawing event times without the future,
  a free email (`UNIQUE`), the day guard, new customers, customer changes,
  a price change, new orders (a customer can order only after creating an account),
  the progress of open orders, `simulate_daily_activity()` putting the day together.
- **Why `--profile prod` only in the first command, and did dev also get
  `faker`?** A profile is a flag of one command, not state. `up --build` creates and
  rebuilds, so it has to include prod; `exec` points at a running container by
  name (`airflow-scheduler` = dev, `airflow-scheduler-prod` = prod). Dev was also
  recreated (`Started` for the services without `-prod`, `Running` = untouched),
  proof: `dags test` on the dev scheduler imported `faker`.
- **Why does `--profile prod` start dev and prod, and how do I start prod alone?**
  A service without `profiles:` is always active; a profile only ADDS services. Prod
  alone: service names in the command (`... up -d --build airflow-init-prod
  airflow-webserver-prod airflow-scheduler-prod`; `depends_on` pulls in prod's
  database). Cleaner: a `dev` profile for the dev services (phase 7). In a company dev and
  prod are separate deployments.
- **Where does "Env var required but not provided: DBT_SNOWFLAKE_ACCOUNT" in VS Code come from?**
  The dbt Power User extension parses the project with a profile using `env_var()`,
  and since 22.10 the variables are only in `.env`. dbt 1.12 loads `.env` by itself, but only
  from the current directory, and the dbt project is in `dbt_portfolio/`. A solution without
  copying secrets: `ln -s ../.env dbt_portfolio/.env` + reloading the window
  (`.env` in `.gitignore` also covers subdirectories). In a company: a secrets manager
  or direnv. The pipeline is not affected.
- **What does the Ruff warning SIM117 mean?** Two nested `with` statements can be written
  as one with several contexts; the behaviour is identical. In a company such remarks are
  stopped by a pre-commit hook and a CI check (phase 7).

**Git 23.10:** the simulator PR merged (squash `84ece27`, #22). It also included
a `.gitignore` change: the pattern `DE_Interview_Cheatsheet*.docx` instead of only
`-2`, because the new copy of the cheat sheet `-3` was not ignored.

**Done (6.7, part 2, 23–24.10): the first real increment through the whole pipeline.**
- A manual run `airflow dags test ecommerce_pipeline 2026-10-24` (window 23.10, 22:00
  UTC): 57 rows from Postgres (2 customers, 1 product, 14 orders = 6 new +
  8 changed statuses, 15 line items, 7 payments, 18 history entries),
  validation without quarantine, dbt `PASS=69 ERROR=0`.

| Iceberg table | Before | After | Where the difference comes from |
|---|---|---|---|
| customer (all versions) | 51 | 53 | +1 new customer, +1 SCD2 version (email change) |
| product (all versions) | 30 | 31 | +1 SCD2 version (price change) |
| orders | 200 | 206 | +6 new (8 status transitions update existing rows) |
| order_items | 502 | 517 | +15 line items |
| payments | 160 | 167 | 4 paid the same day + 3 transitions pending → paid |
| order_status_history | 493 | 511 | 6 × pending + 4 × paid + 8 transitions |

- Snowflake: `fct_orders` has 6 new orders (201–206: 4 `paid` with
  `paid_amount` = the value, 2 `pending` with 0). SCD2 in the Iceberg table: customer 39
  (email change) has two versions, the old one's `valid_to` = the new one's `valid_from`, the new one
  `is_current = TRUE`; customer 51 (new) has one. The analyst sees 0 of the 6 new
  orders — none is from DE (WS, IT, CA, KG, LV, ID), i.e. RLS works on
  fresh rows. The `COUNT(*)` counters per role to be repeated one by one
  (with several selected queries, the worksheet shows only the last result).
- The nightly scheduled run `scheduled__2026-10-23` (00:00–00:01:29 UTC): the same
  window a second time → again 57 rows, the tables unchanged (53 / 31 / 206 / 517 / 167 /
  511), SCD2 `no changed rows -- nothing to insert`, dbt `PASS=69`.
  Idempotency confirmed on a real increment.

**Discovered along the way:**
- **`dags test` next to a running scheduler.** The run lands in the shared metadata
  database, and the scheduler (DAG enabled) also starts executing its tasks:
  `extract_github` ran 3 times ("Executor reports task instance …
  finished (success) although the task says it's queued"). No harm thanks to
  idempotency (the GitHub raw got three copies). Rule: `dags test` only on a
  paused DAG or in a separate environment.
- **Date collision.** A manual run with the logical date 24.10 occupies that date; the scheduler would
  not create its own run for 24.10 → a silent gap in the data (no failure, so
  no alert). `manual__2026-10-24` deleted after collecting the results (Browse →
  DAG Runs). My mistake: I proposed this command without checking the consequences for
  the following runs.
- **An incomplete window.** GitHub: 627 rows in the run at 22:00 UTC (day 23.10 was still
  going on), 635 at night. That is why the run for day D starts only at D+1 00:00.
- **The `set_updated_at` trigger in Postgres** (`ddl.sql`) on every `UPDATE` sets
  `updated_at = now()`, so the change times given by the simulator are
  overwritten with the real time of the change: the new version of customer 39 has `valid_from` =
  20:59:56 UTC (the moment of the run). Realistic (that is how a real
  application works); to fix: the comment in `seed.py` that suggests random times.
- **`Duration` of about a month for `manual__2026-09-14`:** an artefact of `dags test` (Start
  Date = the logical date 14.09, End Date = the actual end on 22.10); in reality about
  5 min. The scheduled run has a normal 1 min 29 s.
- **`-0700` in Snowflake's results:** `TIMESTAMP_LTZ` displayed in the session's time zone
  (Los Angeles by default); 13:59:56 -0700 = 20:59:56 UTC. dbt staging
  converts to UTC, so the models don't depend on it.

**Questions from this step and what they teach:**
- **So now I have two DAGs?** (after adding the simulator) Yes, every Airflow loads
  both files: `source_simulator` (dev 23:30 UTC, prod manual only) and
  `ecommerce_pipeline` (dev 00:00, prod 03:00). They are linked only by time, without an explicit
  dependency — on purpose, because the simulator plays someone else's application. In a company, with a dependency
  between your own DAGs: Datasets (data-aware scheduling, Airflow 2.4+),
  `TriggerDagRunOperator`, `ExternalTaskSensor`; with someone else's source: a window with a
  lookback + freshness checks.
- **Can we check the increment right away, without waiting for the night?** Technically yes
  (`dags test` with the date 24.10), but with the two side effects described above
  (double execution by the scheduler, an occupied date). The right way: wait for the
  scheduled run or reprocess a closed day with a backfill/Clear.
- **Why is it a "test"?** The name is misleading: `dags test` = run the whole DAG once,
  now, in the terminal, without the scheduler; the tasks run for real (S3, Iceberg,
  Snowflake). No retries with a delay, no `max_active_runs`. In a company: a developer's tool
  locally and in CI; in prod runs are started only by the scheduler.
- **Why does `dags test` take the date of the next day?** In Airflow 2 the date of a
  scheduled/backfill run = the day of data (the start of the window), and the run starts after its
  end. In a manual run the given date = the moment of the run, and Airflow assigns
  the last completed day before it: 24.10 → window 23.10. A trigger with a date in the
  future is blocked by default (`allow_trigger_in_future = False`).
- **Why does one run in DAG Runs last a month?** An artefact of `dags test` (above).

**Done (6.7, part 3a, 24.10): `dbt-expectations` tests on `fct_orders`.**
- `packages.yml`: `metaplane/dbt_expectations` 0.10.10 (the maintained continuation of
  the archived `calogica/dbt_expectations`; dbt >=1.7 <3.0), the version
  pinned like `dbt_utils`; `dbt deps` pulled in the dependency `dbt_date` 0.21.0,
  `package-lock.yml` updated. `dbt_packages/` in `.gitignore`; Airflow
  sees the package without rebuilding the image, because `dbt_portfolio/` is mounted (in a
  company `dbt deps` in CI/when building the image — phase 7).
- Three tests in `models/marts/schema.yml`, each catching something the existing ones
  didn't see:

| Test | Column | What it catches | Level |
|---|---|---|---|
| `expect_row_values_to_have_recent_data` (≤ 2 days) | `order_date` | a silent stall: the mart stopped receiving data although no task failed (the lesson from 6.5) | `warn` |
| `expect_column_values_to_match_regex` (`^[A-Z]{2}$`) | `country_at_order` | a broken format contract from the source; RLS filters on this value, so a wrong format = wrong permissions | `error` |
| `expect_column_values_to_be_between` (0–10,000) | `gross_order_value` | a value anomaly (duplicated line items, a wrong price); the ceiling from the shape of the data: prices 5–500, max 4 line items × 3 units ≈ 6000 | `warn` + `store_failures` |

- First run: the freshness test `ERROR` (`Required var
  'dbt_date:time_zone' not found`). Fix: `vars: 'dbt_date:time_zone': 'UTC'`
  in `dbt_project.yml` (UTC, because staging converts all timestamps to UTC).
  Second run in the dev container (`dbt test --select fct_orders --target
  dev`): `PASS=20 WARN=0 ERROR=0`. The last work requiring Snowflake before
  the end of the trial is done.
- `seed.py`: a comment about the `set_updated_at` trigger (all tables except
  `order_status_history`): the times given in an UPDATE are overwritten by
  `now()`; random times take effect only on INSERT and in the history's `changed_at`.

**Questions from this step and what they teach:**
- **What does "the length of the command is the price of running dbt in a container" mean?** (with the
  test command) The test itself is `dbt test --select fct_orders --target dev`;
  the rest is getting to dbt in the container: `docker compose exec airflow-scheduler`
  (there the profile and variables are ready: `DBT_PROFILES_DIR` pointing to the mounted profile,
  variables from `.env`, the key read-only), `bash -c` (several commands at once),
  `cd` into the mounted project, the full path to dbt (a separate venv outside
  `PATH`). On the Mac the short version doesn't work, because dbt reads `.env` only from the
  current directory (`dbt_portfolio/`), and `.env` sits in the repo root; a
  symlink `ln -s ../.env dbt_portfolio/.env` helps. In a company a developer has the profile and
  access configured locally (or the dbt Cloud IDE / a devcontainer) and types
  `dbt test -s fct_orders`; the long path is the scheduler's way, not a human's.
- **How do you know the test failed at compilation, before checking anything?** (after
  the first `ERROR`) Three signals: the text `Compilation Error` (dbt first
  compiles Jinja/YAML into SQL, then runs the SQL in the warehouse — here the SQL was never
  produced); the time `0.02s` vs 0.7–1.1 s for the others (too little for a query to
  Snowflake); the result `ERROR`, not `FAIL`. `FAIL n` = the test ran and found
  n bad rows (a problem in the data); `WARN` = the same with `severity: warn`;
  `ERROR` = the test couldn't be run (compilation, a missing table, an SQL error —
  a problem in the code/configuration). In a company this decides who reacts: `FAIL` —
  the data team/the source owner, `ERROR` — the author of the latest code change.
  Lesson: the `grep` filter swallowed the line with the reason for the error; the full reason is in
  `dbt_portfolio/logs/dbt.log`.
- **Why `dbt_date:time_zone` = UTC?** The freshness test compares `order_date` with
  "now"; both must be in the same time zone, and staging keeps everything in UTC.
  A business time zone (e.g. `Europe/Warsaw`) is set when a "day" in reports should
  be counted by the customer's time.
- **Are dbt tests run in Snowflake at all?** (after a green `dbt
  test`) Yes, entirely. dbt is a compiler and conductor of SQL: it turns a
  test from YAML into a `SELECT` returning BAD rows, Snowflake runs it on the
  warehouse (credits — which is why dbt tests will stop after 27.10), and dbt only counts the
  returned rows (0 = `PASS`, n = `FAIL n`/`WARN n`). The compiled SQL of every
  test sits in `dbt_portfolio/target/compiled/`. Tests with `store_failures` first
  write the bad rows into the schema `dbt_dev_dbt_test__audit` (`create or replace
  transient table …`), then count `count(*)` from that table; a unit test creates from
  the fixtures an empty temporary table (`where false limit 0`) to learn the column
  types. In a company it is identical — data tests run where the data lives; unlike
  `validate.py`, which reads the raw files from S3 in Python (a gate
  before the warehouse).
- **Where in Snowflake can you see that a query was sent by dbt?** (while browsing
  the Query History) Three traces: (1) the comment dbt appends at the end of every
  SQL statement: `/* {"app": "dbt", "dbt_version": …, "target_name": "dev", "node_id":
  "test.dbt_portfolio.…"} */` — the `node_id` points to the specific test/model; (2)
  CLIENT DRIVER `PythonConnector` (dbt-snowflake) vs `Go` (Snowsight, i.e.
  manual queries from a worksheet); (3) a shared SESSION ID for the queries of one
  run. For us USER is your user (dbt logs in with your key); in a
  company dbt has a service user (e.g. `SVC_DBT`) and a `query_tag` in the profile
  (e.g. `dbt_prod`), so the Query History and the credit billing immediately
  show what dbt used and what people used (`query_tag` — phase 7). The results of the
  `COUNT(*)` queries per role aren't visible in the history — for evidence, run them
  one at a time in the worksheet.

**Git 24.10:** the `dbt_expectations` tests merged (#23). A second commit
(documentation) was made on the same, already merged branch → `git push`
recreated it on GitHub (`* [new branch]`, "2 commits ahead, 1 behind main").
Fix: a new branch `docs/6-7-notes` from a fresh `main`, `git cherry-pick` of only the
documentation commit, deleting the recreated branch locally and on GitHub → PR
#24. My mistake: I assumed the PR wasn't merged instead of asking.

**Done (6.7, part 3b, 24.10): evidence from Snowflake and the README.**
- RLS: engineer `COUNT(*) = 206`, `COUNT_IF(country_at_order = 'DE') = 10`;
  analyst `COUNT(*) = 10` — the sets match (the analyst sees exactly DE).
- Screenshots in `docs/images/` (6 files, about 1.5 MB): the list of runs, a run after
  all attempts, the Asana task, RLS engineer/analyst, SCD2 of customers 39 and 51.
- `README.md` (297 lines): architecture, stack, three control questions with
  evidence (two runs of the same window, the backfill of 11–13.09 with the conclusion about
  the cost of `MERGE` on the whole raw layer, the layers detecting silent corruption), a
  "Design questions" section with the remaining 7 control questions from the plan (Iceberg vs
  Delta, append-only S3, no Bronze in Snowflake, `MERGE`, the Glue REST catalog,
  SCD2 in Spark instead of dbt snapshots, "what would you remove in a €5M/year system" — the
  last one as a working answer for you to correct), the increment from the simulator,
  governance, running locally, the ADRs, known limitations.
- `.env.example`: the names of 11 variables with descriptions, without values (missing since phase 0).
- ADRs moved from the repo root to `docs/adr/` (a plain move, git
  will show it as a rename) + an index `docs/adr/README.md` (ADR, decision, status).
  ADR-001 (technology stack) written retrospectively: the decision from the plan of
  17.09 (AWS + Iceberg + Snowflake + dbt + Airflow instead of the Azure/Databricks work
  stack, what was deliberately left out, the phase order), marked as recorded on 24.10. References corrected in
  `README.md` and `CLAUDE.md`; older log entries stay unchanged (history).
- `CLAUDE.md` pointed to `ADR-002` and `project-summary-for-consultation.md`,
  which weren't in the repo — both lived only in the project knowledge on claude.ai
  (17.09), so Claude Code didn't see them. ADR-002 added to the repo with an
  "Update 2026-10-24" section (implemented: append-only S3 + `MERGE` into Iceberg, no
  native Bronze; changed: `validate.py` instead of GE, SCD2 in Spark instead of dbt
  snapshots, windows instead of a watermark; missing: retention policies; known limitation:
  `MERGE` on the whole raw layer). The planning summary (a PDF from before the build, outdated)
  stays outside the repo; in `CLAUDE.md` the reference was replaced with `README.md`. The README lists
  ADR-002.

**Questions from this step and what they teach:**
- **What does `git cherry-pick` do?** (fixing the branch after a merge) It takes the changes
  introduced by ONE given commit (the added/removed lines relative to its
  parent) and applies them to the current branch as a new commit (the same content,
  a new identifier). It doesn't copy a branch or whole files. The standard way
  to move a single commit from a branch that has "drifted away".
- **Why `git push origin --delete`, if `git branch -D` deletes the branch?** `-D`
  deletes only the local one; the branch on GitHub (here recreated by accident) is deleted
  only by `push --delete`. Without it, it would hang around with a misleading history, GitHub
  would offer a PR from it, and `fetch` would bring back `origin/*`.
- **What does `docs/6-7-notes` mean?** A branch name following a convention: a change-type prefix
  (as in conventional commits: `feat/`, `fix/`, `docs/`, `test/`, `chore/`) + a
  short description. In a company usually with a ticket number, e.g. `docs/DE-142-...`; then
  Jira links the branch and the PR by itself.
- **What does `-c` in `git switch -c` do?** `--create`: create a branch from the current
  commit and switch to it (the older form: `git checkout -b`). Without `-c`
  `switch` only moves to an existing branch.
- **How is it possible that uncommitted changes "move" to another branch?**
  A branch is a pointer to a commit, not a folder. The files sit in one shared working
  tree; `switch` replaces only the files that differ between the two branches' commits and
  leaves the uncommitted changes. With `switch -c` both branches point to the same
  commit, so the changes always carry over; when moving to a branch where the
  changed file looks different, git refuses ("would be overwritten"). In a company:
  `git stash` → switch → `git stash pop`, or `git worktree` (a second folder
  with another branch).
- **Did we delete the run that failed?** No: only test runs were deleted
  (`manual__2026-10-20`, `manual__2026-10-24`). The run `scheduled__2026-10-22` was
  cleared after the test (`tasks clear`) and passed, hence `success` today.
  The trace of the failure: the Asana task, the screenshot with the red task, the logs of attempts 1–4.
- **Why does customer 51 have only one version, and why is it in the screenshot?** It is a new
  customer from 23.10 — a first version, with nothing to close. Together with customer 39
  (an email change: closing the old version + a new one) it shows both SCD2 paths:
  a new key vs a changed attribute; the third path (no change → `nothing to
  insert`) follows from the second pass over the window.
- **Is `project-summary-for-consultation` important?** (when tidying up the
  references in `CLAUDE.md`) Yes, as the project plan, not as a repo file: a PDF from
  17.09 in the project knowledge on claude.ai (a consultation before the build). Three
  things followed from it: there are 10 control questions, not 3 (the README extended);
  the plan's "wow moment" (a customer changes country → SCD2) we now have on real
  data (customer 39); `.env.example` was missing (added). It doesn't go into the repo, because
  it is outdated (dbt snapshots, GE, a different folder structure) and written for the
  consultation. In a company: planning documents in the team wiki, in the repo the README +
  ADRs.
- **Do ADRs have to be in separate files?** (when tidying up the repo) The standard
  is one file per decision: its own status (`Proposed` → `Accepted` →
  `Superseded by …`), stable references, git history per decision, every new
  ADR is a separate PR without conflicts, and old ones are never rewritten. One file
  (`DECISIONS.md`) appears in small projects as a lightweight decision log.
  What got in the way was not the format but the clutter in the repo root → `docs/adr/` with an index
  (that's also what `adr-tools` does and what the plan of 17.09 assumed).
- **Shouldn't the choice of the technology stack be described somewhere?** (after the
  README and ADRs were in place) Yes: the README had a stack table (what and for what), but
  not WHY this stack — the reasoning lived only in the plan outside the repo. That is
  classic ADR material, so ADR-001 was written retrospectively, with an explicit note
  that the decision was made earlier and recorded later. In a company this happens
  whenever a decision exists only in someone's head or an old document; the honest form
  is a dated ADR, not a back-dated one.
- **Could we have a diagram of which technology depends on / points to which?** (after
  ADR-001) Yes: the ASCII sketch in the README replaced with a Mermaid flowchart — six groups
  (sources, Airflow, processing, AWS, Snowflake, plus dbt and Asana), solid arrows = data
  flow, dashed = metadata and control (catalog pointer, quality gate, policies, grants,
  alert), thick = what Airflow runs. Mermaid is "diagram as code": GitHub renders it from
  text in the Markdown, a change is a few lines in a diff, and there is no PNG that silently
  goes out of date. In a company the same idea: Mermaid or C4/Structurizr in the repo, or
  draw.io files versioned next to the code.
- **Could there also be a picture of the whole architecture with technology logos?** (after
  the Mermaid diagram) Yes: `docs/diagrams/architecture.py` with the `diagrams` library
  ("diagram as code" with official vendor icons: PostgreSQL, GitHub, Airflow, Spark, dbt,
  S3, Glue, Athena, Snowflake) generates `docs/images/architecture.png`, shown at the top of
  the README's Architecture section; the Mermaid map stays below as the detailed view. Iceberg
  is drawn as S3 (its tables are files in S3); Asana uses the author's logo file
  (`docs/diagrams/icons/asana_logo.webp`, a PNG copy because Graphviz cannot read WebP).
  Regenerated locally (`brew install graphviz`, `uv add --dev diagrams`), so the picture
  always comes from the script in the repo. In a company the same: diagrams as code
  (diagrams, Structurizr/C4, Mermaid) next to the code, rendered in CI or by hand.
  Final layout after several review rounds: layers left to right — Sources → Ingestion →
  AWS (Raw/bronze, Lakehouse/silver) → Snowflake (source tables → staging + intermediate →
  marts) → Gold (S3); Orchestration (Airflow + dbt) as a bottom row, Alerting (Asana) below
  it, CI/CD and IaC (Terraform, GitHub Actions) on the left — the picture shows the finished
  project, README "Known limitations" says phases 7–8 are next. Graphviz lessons: every edge
  `label` gets its own invisible column (it pushed the Snowflake icons apart unevenly), so the
  final picture has no arrow labels; `constraint=false` draws an arrow without letting it
  move the layout; invisible edges and `group` position and align nodes; `splines=line`
  keeps every arrow one straight segment.
- **Why is dbt drawn outside Snowflake, if the models live there?** dbt Core is a program in
  the Airflow container: it generates SQL (`CREATE VIEW`, `CREATE TABLE AS SELECT`) and sends
  it to Snowflake, which builds and stores the objects. Staging = thin 1:1 views over the
  source tables (renames, types, UTC); intermediate = reusable logic not exposed to analysts
  (`int_customer_versions`); marts = tables answering business questions, with grants and
  policies (`fct_orders`, so far the only mart).
- **Spark points to both Glue and S3 — why?** On `MERGE` Spark writes everything to S3: data
  files (Parquet) and Iceberg metadata files (`metadata.json`, manifests). The commit is an
  atomic swap of one pointer in Glue ("the current version is this `metadata.json`"). Glue
  holds only that pointer; readers ask Glue where the latest version is and read the data
  from S3.
- **Shouldn't Athena read gold too?** Athena finds Iceberg tables through the Glue catalog.
  Silver tables are written by Spark and registered in Glue, so Athena sees them. The marts
  are Snowflake-managed Iceberg (catalog = Snowflake): their files are in S3, but Glue does
  not know them. Reading gold from Athena would need the Snowflake catalog exposed (Snowflake
  Open Catalog / Horizon via Iceberg REST) or regular registration in Glue — a deliberate
  trade-off, not implemented.
- **Is anything missing from the picture?** S3 quarantine (where `validate.py` puts rejected
  rows) — added. Observability (phase 9) is added once its tool is chosen. Left out on
  purpose: IAM roles per consumer (security detail, ADR-004), Docker as an icon (shown in the
  labels), uv and Ruff (developer tooling, not architecture).
- **Does the README go into `docs/` too?** No: the main `README.md` stays in the root,
  because GitHub shows it on the repo page. `docs/adr/README.md` is a separate index
  that GitHub displays when you open the folder — two files with the same name in
  two places is standard.
- By the way (the Asana screenshot): the error description in the task is a generic "Bash command
  failed … exit code 1", without the cause from the log — the callback should attach
  the last lines of the task log (after 27.10).

**Next step:** commit + PR from `docs/readme` (README, `docs/images/`, `docs/adr/`,
`.env.example`, `CLAUDE.md`, this entry).
After 27.10: `merge_*` only on the current run's files, auto-closing and
deduplication of Asana tasks (with the last log lines in the description), `dagrun_timeout`
with a DAG-level alert, phase 7 (CI, Ruff in pre-commit, `dbt deps` in
CI, `query_tag` and a service user for dbt, one Airflow image, a `dev` profile
in compose).

---

*Update this file after every bigger step — in chronological order; questions from the conversation recorded with context (where they came from) and with the knowledge they teach, so that they are understandable a month later; general theory goes in the cheat sheet.*
