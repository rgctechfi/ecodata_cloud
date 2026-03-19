"""@bruin

name: ingestion.imf_bronze_to_silver

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

import pandas as pd

from google.cloud import storage


def resolve_project_root() -> Path:
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
    raw = os.environ.get("BRUIN_VARS", "")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("BRUIN_VARS is not valid JSON.") from exc


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "t"}
    return False


def parse_int(value: Any) -> int | None:
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


def ensure_bucket(client: storage.Client, name: str) -> storage.Bucket:
    try:
        return client.get_bucket(name)
    except Exception as exc:  # pragma: no cover - surface a clear message
        raise RuntimeError(f"Unable to access bucket '{name}': {exc}") from exc


def default_transform_config() -> dict[str, Any]:
    return {"default": {"drop_columns": [], "rename_columns": {}}, "datasets": {}}


def load_transform_config(project_root: Path, override_path: str | None) -> dict[str, Any]:
    if override_path:
        config_path = Path(override_path).expanduser()
    else:
        config_path = project_root / "bruin" / "pipeline" / "config" / "silver_transforms.json"

    if not config_path.exists():
        return default_transform_config()

    payload = json.loads(config_path.read_text())
    if not isinstance(payload, dict):
        raise ValueError("Transform config must be a JSON object.")

    payload.setdefault("default", {})
    payload.setdefault("datasets", {})
    if not isinstance(payload["default"], dict) or not isinstance(payload["datasets"], dict):
        raise ValueError("Transform config keys 'default' and 'datasets' must be objects.")

    payload["default"].setdefault("drop_columns", [])
    payload["default"].setdefault("rename_columns", {})
    return payload


def merge_transforms(config: dict[str, Any], dataset_name: str) -> tuple[list[str], dict[str, str]]:
    defaults = config.get("default", {})
    dataset_cfg = config.get("datasets", {}).get(dataset_name, {})

    drop_columns: list[str] = []
    rename_columns: dict[str, str] = {}

    for candidate in (defaults, dataset_cfg):
        if isinstance(candidate.get("drop_columns"), list):
            drop_columns.extend([str(col) for col in candidate["drop_columns"]])
        if isinstance(candidate.get("rename_columns"), dict):
            rename_columns.update({str(k): str(v) for k, v in candidate["rename_columns"].items()})

    # de-duplicate while keeping order
    seen: set[str] = set()
    drop_columns = [col for col in drop_columns if not (col in seen or seen.add(col))]
    return drop_columns, rename_columns


def apply_transforms(
    frame: pd.DataFrame, dataset_name: str, config: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    drop_columns, rename_columns = merge_transforms(config, dataset_name)
    dropped = [col for col in drop_columns if col in frame.columns]
    rename_columns = {k: v for k, v in rename_columns.items() if k in frame.columns}

    if dropped:
        frame = frame.drop(columns=dropped)
    if rename_columns:
        frame = frame.rename(columns=rename_columns)

    return frame, {"dropped": dropped, "renamed": rename_columns}


def promote_bronze_to_silver() -> None:
    bruin_vars = load_bruin_vars()

    bronze_bucket_name = bruin_vars.get("bronze_bucket", "ecodatacloud-ds-bronze")
    silver_bucket_name = bruin_vars.get("silver_bucket", "ecodatacloud-ds-silver")
    prefix = bruin_vars.get("prefix", "parquet/")
    overwrite = parse_bool(bruin_vars.get("overwrite"))
    dry_run = parse_bool(bruin_vars.get("dry_run"))
    max_objects = parse_int(bruin_vars.get("max_objects"))
    transform_config_path = bruin_vars.get("transform_config")

    project_root = resolve_project_root()
    transform_config = load_transform_config(project_root, transform_config_path)

    client = storage.Client()
    bronze_bucket = ensure_bucket(client, bronze_bucket_name)
    silver_bucket = ensure_bucket(client, silver_bucket_name)

    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logs: list[dict[str, Any]] = []
    copied = 0
    skipped = 0
    errored = 0

    for blob in client.list_blobs(bronze_bucket, prefix=prefix):
        if max_objects is not None and copied + skipped + errored >= max_objects:
            break

        if not blob.name.endswith(".parquet") or "/_logs/" in blob.name:
            skipped += 1
            logs.append(
                {
                    "run_at": run_at,
                    "dataset": None,
                    "source_bucket": bronze_bucket_name,
                    "dest_bucket": silver_bucket_name,
                    "source_object": blob.name,
                    "dest_object": None,
                    "status": "skip_non_parquet",
                    "bytes": blob.size,
                    "row_count_before": None,
                    "row_count_after": None,
                    "column_count_before": None,
                    "column_count_after": None,
                    "error": None,
                }
            )
            continue

        dest_name = blob.name
        dest_blob = silver_bucket.blob(dest_name)
        dataset_name = Path(blob.name).stem

        try:
            if not overwrite and dest_blob.exists():
                logs.append(
                    {
                        "run_at": run_at,
                        "dataset": dataset_name,
                        "source_bucket": bronze_bucket_name,
                        "dest_bucket": silver_bucket_name,
                        "source_object": blob.name,
                        "dest_object": dest_name,
                        "status": "skip_exists",
                        "bytes": blob.size,
                        "row_count_before": None,
                        "row_count_after": None,
                        "column_count_before": None,
                        "column_count_after": None,
                        "error": None,
                    }
                )
                skipped += 1
                continue

            if dry_run:
                status = "dry_run"
                row_count_before = None
                row_count_after = None
                column_count_before = None
                column_count_after = None
                bytes_written = blob.size
            else:
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmpdir_path = Path(tmpdir)
                    source_path = tmpdir_path / "bronze.parquet"
                    dest_path = tmpdir_path / "silver.parquet"

                    blob.download_to_filename(source_path)
                    frame = pd.read_parquet(source_path)
                    row_count_before = int(frame.shape[0])
                    column_count_before = int(frame.shape[1])

                    frame, _ = apply_transforms(frame, dataset_name, transform_config)
                    row_count_after = int(frame.shape[0])
                    column_count_after = int(frame.shape[1])

                    frame.to_parquet(dest_path, index=False)
                    bytes_written = dest_path.stat().st_size
                    dest_blob.upload_from_filename(dest_path)

                status = "ok"
                copied += 1

            logs.append(
                {
                    "run_at": run_at,
                    "dataset": dataset_name,
                    "source_bucket": bronze_bucket_name,
                    "dest_bucket": silver_bucket_name,
                    "source_object": blob.name,
                    "dest_object": dest_name,
                    "status": status,
                    "bytes": bytes_written,
                    "row_count_before": row_count_before,
                    "row_count_after": row_count_after,
                    "column_count_before": column_count_before,
                    "column_count_after": column_count_after,
                    "error": None,
                }
            )
        except Exception as exc:
            logs.append(
                {
                    "run_at": run_at,
                    "dataset": dataset_name,
                    "source_bucket": bronze_bucket_name,
                    "dest_bucket": silver_bucket_name,
                    "source_object": blob.name,
                    "dest_object": dest_name,
                    "status": "error",
                    "bytes": blob.size,
                    "row_count_before": None,
                    "row_count_after": None,
                    "column_count_before": None,
                    "column_count_after": None,
                    "error": str(exc),
                }
            )
            errored += 1

    log_dir = project_root / "data" / "silver" / "_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "imf_bronze_to_silver_log.csv"

    columns = [
        "run_at",
        "dataset",
        "source_bucket",
        "dest_bucket",
        "source_object",
        "dest_object",
        "status",
        "bytes",
        "row_count_before",
        "row_count_after",
        "column_count_before",
        "column_count_after",
        "error",
    ]

    with log_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(logs)

    print(
        f"Bronze->Silver summary: copied={copied}, skipped={skipped}, "
        f"errors={errored}, dry_run={dry_run}"
    )
    print(f"Wrote promotion log to: {log_path}")


promote_bronze_to_silver()
