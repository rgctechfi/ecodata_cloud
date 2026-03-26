"""@bruin

name: gold.gold_obt

type: python

image: python:3.11

@bruin"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from google.cloud import bigquery


def resolve_project_root() -> Path:
    """Resolve the repository root from the different locations Bruin may use."""
    candidates = [
        Path.cwd(),
        Path.cwd().parent,
        Path(__file__).resolve().parents[4],
    ]
    for candidate in candidates:
        if (candidate / "bruin").exists():
            return candidate
    return candidates[-1]


def load_bruin_vars() -> dict[str, Any]:
    """Deserialize JSON runtime overrides passed by Bruin or Make."""
    # Bruin passes runtime overrides through environment variables, which keeps
    # the asset reusable across local runs, Make targets, and CI-like executions.
    raw = os.environ.get("ECODATA_VARS") or os.environ.get("BRUIN_VARS", "")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("ECODATA_VARS/BRUIN_VARS is not valid JSON.") from exc


def load_query(project_root: Path) -> str:
    """Load the SQL query that defines the One Big Table business logic."""
    query_path = project_root / "bruin" / "pipeline" / "assets" / "gold" / "gold_obt.sql"
    return query_path.read_text()


def ensure_obt_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
) -> str:
    """Recreate the destination OBT table with the expected schema and storage settings."""
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    # The OBT schema is declared explicitly because it is the business-facing table
    # consumed by analysis and reporting tools.
    schema = [
        bigquery.SchemaField("country_label", "STRING"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("gdp_per_capita_usd_usd_per_capita", "FLOAT"),
        bigquery.SchemaField("gdp_per_capita_ppp_international_dollars_per_capita", "FLOAT"),
        bigquery.SchemaField("gdp_ppp_world_share_percent_world", "FLOAT"),
        bigquery.SchemaField("unemployment_rate_percent_labor_force", "FLOAT"),
        bigquery.SchemaField("public_debt_gdp_percent_gdp", "FLOAT"),
        bigquery.SchemaField("inflation_avg_consumer_percent_change", "FLOAT"),
    ]

    # Recreate the table on each run so the SQL definition remains the single source of truth.
    client.delete_table(table_id, not_found_ok=True)

    table = bigquery.Table(table_id, schema=schema)
    # Range partitioning on year matches the analytical grain and keeps scans cheaper.
    table.range_partitioning = bigquery.RangePartitioning(
        field="year",
        range_=bigquery.PartitionRange(start=1980, end=2030, interval=1),
    )
    table.clustering_fields = ["country_label"]
    client.create_table(table)
    return table_id


def run_gold_obt() -> None:
    """Execute the SQL OBT query and materialize its result into the final Gold table."""
    bruin_vars = load_bruin_vars()
    project_root = resolve_project_root()

    bq_project = bruin_vars.get("bq_project", "ecodatacloud")
    bq_dataset = bruin_vars.get("bq_dataset", "ecodatacloud_bq_gold")
    bq_location = bruin_vars.get("bq_location", "EU")
    table_prefix = bruin_vars.get("table_prefix", "gold__")
    obt_table_name = bruin_vars.get("gold_obt_table", f"{table_prefix}obt")

    client = bigquery.Client(project=bq_project)
    destination_table = ensure_obt_table(client, bq_project, bq_dataset, obt_table_name)
    # The SQL file contains the business logic; the Python wrapper only manages
    # table lifecycle and execution settings.
    select_query = load_query(project_root).strip().rstrip(";")

    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.query(select_query, location=bq_location, job_config=job_config)
    job.result()

    print(f"Created or replaced table: {bq_project}.{bq_dataset}.{obt_table_name}")
    print(f"BigQuery location: {bq_location}")


run_gold_obt()
