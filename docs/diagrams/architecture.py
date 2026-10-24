"""
Architecture overview with technology icons, generated as code (the `diagrams` library).

Regenerate after a change, from the repository root:
    uv run python docs/diagrams/architecture.py
Requires Graphviz (macOS: `brew install graphviz`) and the `diagrams` package
(a dev dependency: `uv add --dev diagrams`).

Output: docs/images/architecture.png — the at-a-glance view. The detailed dependency map,
with every arrow labelled, is the Mermaid diagram in README.md.

Layout rule: data flows strictly left to right, one cluster per layer, so Graphviz can lay
the picture out flat. Orchestration is a bottom row (Airflow under the raw layer, dbt under Snowflake);
its arrows use constraint=false, so they are drawn but do not pull the layout around.
Apache Iceberg tables are drawn with the S3 icon (they are files in S3). Asana uses
docs/diagrams/icons/asana_logo.png (a PNG copy of asana_logo.webp, margins trimmed) when
that file exists, otherwise a plain labelled node.

Athena reads only the silver tables: it finds Iceberg tables through the Glue catalog, and
the marts are Snowflake-managed Iceberg (catalog = Snowflake), so Glue does not know them.
"""

from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Athena, GlueDataCatalog
from diagrams.aws.storage import SimpleStorageServiceS3 as S3
from diagrams.custom import Custom
from diagrams.generic.blank import Blank
from diagrams.onprem.analytics import Dbt, Spark
from diagrams.onprem.ci import GithubActions
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.iac import Terraform
from diagrams.onprem.vcs import Github
from diagrams.onprem.workflow import Airflow
from diagrams.programming.language import Python
from diagrams.saas.analytics import Snowflake

DIAGRAMS_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = DIAGRAMS_DIR.parent / "images" / "architecture"  # .png is added by the library
ASANA_LOGO = DIAGRAMS_DIR / "icons" / "asana_logo.png"  # PNG copy of asana_logo.webp (Graphviz cannot read WebP)

GRAPH_ATTR = {"forcelabels": "true", "fontsize": "22", "pad": "0.4", "splines": "line", "nodesep": "0.5", "ranksep": "0.8"}
DATA_FLOW = {"color": "#444441"}
METADATA = {"color": "#888780", "style": "dashed"}
ORCHESTRATION = {"color": "#534AB7", "style": "bold"}
CLUSTER_ATTR = {"margin": "28", "fontsize": "14"}  # padding inside every group, so labels stay inside the frame
# For nodes whose arrows leave upwards: icon at the top of a shorter node box, so the
# arrows start at the icon instead of in the empty space above it.
ICON_AT_TOP = {"imagepos": "tc", "height": "1.9"}

with Diagram(
    "E-commerce data pipeline",
    filename=str(OUTPUT_FILE),
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=GRAPH_ATTR,
):
    # ---- main row: data flows left to right (bronze -> silver -> gold) ----
    with Cluster("Sources", graph_attr=CLUSTER_ATTR):
        postgres = PostgreSQL("PostgreSQL\n+ simulator")
        github = Github("GitHub API")

    with Cluster("Ingestion", graph_attr=CLUSTER_ATTR):
        ingestion = Python("extract +\nvalidate")

    # Two layers, one cloud: raw files of every run vs Iceberg tables after MERGE.
    with Cluster("AWS", graph_attr=CLUSTER_ATTR):
        with Cluster("Raw (bronze)", graph_attr=CLUSTER_ATTR):
            raw = S3("S3 raw\nappend-only")
            quarantine = S3("S3 quarantine\nrejected rows")
        with Cluster("Lakehouse (silver)", graph_attr=CLUSTER_ATTR):
            spark = Spark("Spark MERGE\n(Docker)")
            iceberg_tables = S3("S3 silver\nIceberg tables")
            glue_catalog = GlueDataCatalog("Glue Catalog\n(Iceberg REST)")
            athena = Athena("Athena")

    # Everything dbt builds lives in Snowflake; governance applies to the whole account.
    with Cluster("Snowflake: warehouse (RBAC, RLS, masking)", graph_attr=CLUSTER_ATTR):
        source_tables = Snowflake("source tables", group="warehouse_line")
        staging_views = Snowflake("staging +\nintermediate", group="warehouse_line")
        marts = Snowflake("marts", group="warehouse_line")

    # Marts are Snowflake-managed Iceberg tables: Snowflake writes them to S3 (gold prefix).
    with Cluster("Gold (AWS)", graph_attr=CLUSTER_ATTR):
        gold = S3("S3 gold\nIceberg tables", group="warehouse_line")

    # ---- bottom row: what runs things (declared last so Graphviz places it below) ----
    with Cluster("Orchestration (Docker)", graph_attr=CLUSTER_ATTR):
        airflow = Airflow("Airflow 2.9\ndev + prod", group="orchestration_line", **ICON_AT_TOP)
        dbt = Dbt("dbt Core\nmodels + tests", group="orchestration_line", **ICON_AT_TOP)

    with Cluster("Alerting", graph_attr={**CLUSTER_ATTR, "margin": "12"}):
        if ASANA_LOGO.exists():
            # wide logo (mark + wordmark) needs a wider box; the logo itself names the tool
            asana = Custom("", str(ASANA_LOGO), width="2.2", height="0.8", fixedsize="true")
        else:
            asana = Blank("Asana")

    # CI/CD and infrastructure as code (phases 7-8): the picture shows the finished project.
    with Cluster("CI/CD and IaC", graph_attr=CLUSTER_ATTR):
        # both arrows below are layout-constrained, so Graphviz orders these two nodes to avoid
        # crossing them (Terraform above, its arrow goes up to AWS)
        terraform = Terraform("Terraform\nAWS + Snowflake")
        github_actions = GithubActions("GitHub Actions\nCI/CD")

    # data flow
    postgres >> Edge(**DATA_FLOW) >> ingestion
    github >> Edge(**DATA_FLOW) >> ingestion
    ingestion >> Edge(**DATA_FLOW) >> raw
    ingestion >> Edge(**METADATA) >> quarantine  # validate.py: rows that fail the quality gate
    raw >> Edge(**DATA_FLOW) >> spark
    spark >> Edge(**DATA_FLOW) >> iceberg_tables
    spark >> Edge(**METADATA) >> glue_catalog
    iceberg_tables >> Edge(**DATA_FLOW) >> athena
    iceberg_tables >> Edge(**DATA_FLOW) >> source_tables
    glue_catalog >> Edge(**METADATA) >> source_tables
    source_tables >> Edge(**DATA_FLOW) >> staging_views
    staging_views >> Edge(**DATA_FLOW) >> marts
    marts >> Edge(**DATA_FLOW) >> gold

    # invisible edges only position the bottom row: Airflow under the lakehouse,
    # dbt under the staging models, so the orchestration arrows are short and point up
    raw >> Edge(style="invis") >> airflow  # anchored on raw, so ingestion stays centred between the sources
    source_tables >> Edge(style="invis") >> dbt  # dbt under staging: short arrows up

    # orchestration and control (constraint=false: drawn, but they don't move the layout)
    # xlabel instead of label on every arrow below the main row: Graphviz gives each `label`
    # its own invisible column, which pushed the Snowflake icons apart unevenly; an xlabel is
    # attached to the arrow without taking a column
    airflow >> Edge(**ORCHESTRATION) >> dbt
    airflow >> Edge(constraint="false", **ORCHESTRATION) >> ingestion
    airflow >> Edge(constraint="false", **ORCHESTRATION) >> spark
    airflow >> Edge(**METADATA) >> asana
    # dbt sends SQL to Snowflake, which builds the staging/intermediate views and the marts
    dbt >> Edge(constraint="false", **METADATA) >> staging_views
    dbt >> Edge(constraint="false", **METADATA) >> marts
    github_actions >> Edge(**METADATA) >> airflow
    terraform >> Edge(**METADATA) >> raw
