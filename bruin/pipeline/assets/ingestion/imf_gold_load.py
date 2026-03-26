"""@bruin

name: ingestion.imf_gold_load

type: python

image: python:3.11

@bruin"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from google.cloud import bigquery
from google.cloud import storage


def resolve_project_root() -> Path:
    """Resolve the repository root from local or Bruin-managed execution contexts."""
    # Support running the asset from the repo root or from the bruin pipeline folder.
    candidates = [
        Path.cwd(),
        Path.cwd().parent,
        Path(__file__).resolve().parents[4],
    ]
    for candidate in candidates:
        if (candidate / "data").exists():
            return candidate
    return candidates[-1]


def load_bruin_vars() -> dict[str, Any]:
    """Deserialize runtime parameters controlling the BigQuery Gold load."""
    raw = os.environ.get("ECODATA_VARS") or os.environ.get("BRUIN_VARS", "")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("ECODATA_VARS/BRUIN_VARS is not valid JSON.") from exc


def parse_bool(value: Any, default: bool = False) -> bool:
    """Convert runtime flags into booleans with a configurable default."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "t"}
    return default


def parse_int(value: Any) -> int | None:
    """Parse optional numeric settings such as partition bounds."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def default_gold_config() -> dict[str, Any]:
    """Return default BigQuery tuning settings for Gold tables."""
    # Partitioning and clustering are kept declarative so table tuning can evolve
    # without rewriting the loading logic for every dataset.
    return {
        "default": {
            "partition_field": "year",
            "partition_range": {"start": 1960, "end": 2035, "interval": 1},
            "cluster_fields": ["country"],
        },
        "datasets": {
            "countries": {
                "partition_field": None,
                "cluster_fields": ["country"],
            }
        },
    }


def load_gold_config(project_root: Path, override_path: str | None) -> dict[str, Any]:
    """Load the Gold table config and validate its top-level JSON structure."""
    if override_path:
        config_path = Path(override_path).expanduser()
    else:
        config_path = project_root / "bruin" / "pipeline" / "config" / "gold_tables.json"

    if not config_path.exists():
        return default_gold_config()

    payload = json.loads(config_path.read_text())
    if not isinstance(payload, dict):
        raise ValueError("Gold config must be a JSON object.")

    payload.setdefault("default", {})
    payload.setdefault("datasets", {})
    if not isinstance(payload["default"], dict) or not isinstance(payload["datasets"], dict):
        raise ValueError("Gold config keys 'default' and 'datasets' must be objects.")

    return payload


def merge_gold_config(config: dict[str, Any], dataset_name: str) -> dict[str, Any]:
    """Combine the default Gold settings with one dataset-specific override block."""
    defaults = config.get("default", {})
    dataset_cfg = config.get("datasets", {}).get(dataset_name, {})

    merged = dict(defaults)
    merged.update(dataset_cfg)
    return merged


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str | None) -> None:
    """Create the target BigQuery dataset on demand if it does not exist yet."""
    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        if location:
            dataset.location = location
        client.create_dataset(dataset, exists_ok=True)


def table_name_from_blob(blob_name: str, prefix: str, table_prefix: str) -> str:
    """Derive a deterministic Gold table name from the parquet object name."""
    # Only the parquet filename matters: folders are used for storage organization,
    # while the table name stays stable and predictable in BigQuery.
    stem = Path(blob_name).stem
    return f"{table_prefix}{stem}"


def build_partitioning(config: dict[str, Any], columns: set[str]) -> bigquery.RangePartitioning | None:
    """Build range partitioning only when the configured field exists in the parquet schema."""
    partition_field = config.get("partition_field")
    if not partition_field or partition_field not in columns:
        return None

    range_cfg = config.get("partition_range", {})
    start = parse_int(range_cfg.get("start")) or 1960
    end = parse_int(range_cfg.get("end")) or 2035
    interval = parse_int(range_cfg.get("interval")) or 1

    return bigquery.RangePartitioning(
        field=partition_field,
        range_=bigquery.PartitionRange(start=start, end=end, interval=interval),
    )


def build_cluster_fields(config: dict[str, Any], columns: set[str]) -> list[str] | None:
    """Keep only valid clustering fields so the loader stays generic across datasets."""
    cluster_fields = config.get("cluster_fields", [])
    if not isinstance(cluster_fields, list):
        return None
    filtered = [field for field in cluster_fields if field in columns]
    return filtered or None


def load_gold_tables() -> None:
    """Load Silver parquet datasets into BigQuery Gold tables with configurable tuning."""
    bruin_vars = load_bruin_vars()

    silver_bucket_name = bruin_vars.get("silver_bucket", "ecodatacloud-ds-silver")
    prefix = bruin_vars.get("prefix", "parquet/")
    table_prefix = bruin_vars.get("table_prefix", "gold__")
    max_objects = parse_int(bruin_vars.get("max_objects"))
    dry_run = parse_bool(bruin_vars.get("dry_run"))
    overwrite = parse_bool(bruin_vars.get("overwrite"))
    write_disposition = bruin_vars.get("write_disposition", "WRITE_TRUNCATE")
    create_disposition = bruin_vars.get("create_disposition", "CREATE_IF_NEEDED")
    config_path = bruin_vars.get("gold_config")

    bq_project = bruin_vars.get("bq_project")
    bq_dataset = bruin_vars.get("bq_dataset", "ecodatacloud_bq_gold")
    bq_location = bruin_vars.get("bq_location", "EU")

    project_root = resolve_project_root()
    gold_config = load_gold_config(project_root, config_path)

    storage_client = storage.Client()
    bq_client = bigquery.Client(project=bq_project)
    dataset_project = bq_project or bq_client.project
    dataset_id = f"{dataset_project}.{bq_dataset}"
    ensure_dataset(bq_client, dataset_id, bq_location)

    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logs: list[dict[str, Any]] = []
    loaded = 0
    skipped = 0
    errored = 0

    for blob in storage_client.list_blobs(silver_bucket_name, prefix=prefix):
        if max_objects is not None and loaded + skipped + errored >= max_objects:
            break

        if not blob.name.endswith(".parquet") or "/_logs/" in blob.name:
            continue

        dataset_name = Path(blob.name).stem
        table_name = table_name_from_blob(blob.name, prefix, table_prefix)
        table_id = f"{dataset_id}.{table_name}"
        config = merge_gold_config(gold_config, dataset_name)

        try:
            if not overwrite:
                try:
                    bq_client.get_table(table_id)
                    skipped += 1
                    logs.append(
                        {
                            "run_at": run_at,
                            "dataset": dataset_name,
                            "table_id": table_id,
                            "status": "skip_exists",
                            "rows": None,
                            "partition_field": config.get("partition_field"),
                            "cluster_fields": ",".join(config.get("cluster_fields", [])),
                            "error": None,
                        }
                    )
                    continue
                except Exception:
                    pass

            # Downloading to a temporary file lets us inspect parquet metadata before loading.
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir) / "silver.parquet"
                blob.download_to_filename(tmp_path)

                parquet = pq.ParquetFile(tmp_path)
                columns = set(parquet.schema.names)
                row_count = parquet.metadata.num_rows if parquet.metadata else None

                # Partitioning/clustering is applied only when the expected columns exist,
                # which keeps the loader generic across heterogeneous datasets.
                partitioning = build_partitioning(config, columns)
                clustering = build_cluster_fields(config, columns)

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    autodetect=True,
                    write_disposition=write_disposition,
                    create_disposition=create_disposition,
                )
                if partitioning:
                    job_config.range_partitioning = partitioning
                if clustering:
                    job_config.clustering_fields = clustering

                if dry_run:
                    status = "dry_run"
                else:
                    # BigQuery infers the schema from parquet while this asset controls
                    # the warehouse-side table behavior (naming, partitioning, clustering).
                    with tmp_path.open("rb") as handle:
                        load_job = bq_client.load_table_from_file(
                            handle, table_id, job_config=job_config
                        )
                    load_job.result()
                    status = "ok"
                    loaded += 1

            logs.append(
                {
                    "run_at": run_at,
                    "dataset": dataset_name,
                    "table_id": table_id,
                    "status": status,
                    "rows": row_count,
                    "partition_field": config.get("partition_field"),
                    "cluster_fields": ",".join(clustering or []),
                    "error": None,
                }
            )
        except Exception as exc:
            errored += 1
            logs.append(
                {
                    "run_at": run_at,
                    "dataset": dataset_name,
                    "table_id": table_id,
                    "status": "error",
                    "rows": None,
                    "partition_field": config.get("partition_field"),
                    "cluster_fields": ",".join(config.get("cluster_fields", [])),
                    "error": str(exc),
                }
            )

    log_dir = project_root / "data" / "gold" / "_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "imf_gold_load_log.csv"

    columns = [
        "run_at",
        "dataset",
        "table_id",
        "status",
        "rows",
        "partition_field",
        "cluster_fields",
        "error",
    ]
    with log_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(logs)

    print(f"Gold load summary: loaded={loaded}, skipped={skipped}, errors={errored}")
    print(f"Wrote gold load log to: {log_path}")


load_gold_tables()
