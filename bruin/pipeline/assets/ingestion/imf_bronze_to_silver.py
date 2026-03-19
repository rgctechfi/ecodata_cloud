"""@bruin

name: ingestion.imf_bronze_to_silver

type: python

image: python:3.11

@bruin"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def promote_bronze_to_silver() -> None:
    bruin_vars = load_bruin_vars()

    bronze_bucket_name = bruin_vars.get("bronze_bucket", "ecodatacloud-ds-bronze")
    silver_bucket_name = bruin_vars.get("silver_bucket", "ecodatacloud-ds-silver")
    prefix = bruin_vars.get("prefix", "parquet/")
    overwrite = parse_bool(bruin_vars.get("overwrite"))
    dry_run = parse_bool(bruin_vars.get("dry_run"))
    max_objects = parse_int(bruin_vars.get("max_objects"))

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

        dest_name = blob.name
        dest_blob = silver_bucket.blob(dest_name)

        try:
            if not overwrite and dest_blob.exists():
                logs.append(
                    {
                        "run_at": run_at,
                        "source_bucket": bronze_bucket_name,
                        "dest_bucket": silver_bucket_name,
                        "source_object": blob.name,
                        "dest_object": dest_name,
                        "status": "skip_exists",
                        "bytes": blob.size,
                        "error": None,
                    }
                )
                skipped += 1
                continue

            if dry_run:
                status = "dry_run"
            else:
                bronze_bucket.copy_blob(blob, silver_bucket, new_name=dest_name)
                status = "ok"
                copied += 1

            logs.append(
                {
                    "run_at": run_at,
                    "source_bucket": bronze_bucket_name,
                    "dest_bucket": silver_bucket_name,
                    "source_object": blob.name,
                    "dest_object": dest_name,
                    "status": status,
                    "bytes": blob.size,
                    "error": None,
                }
            )
        except Exception as exc:
            logs.append(
                {
                    "run_at": run_at,
                    "source_bucket": bronze_bucket_name,
                    "dest_bucket": silver_bucket_name,
                    "source_object": blob.name,
                    "dest_object": dest_name,
                    "status": "error",
                    "bytes": blob.size,
                    "error": str(exc),
                }
            )
            errored += 1

    project_root = resolve_project_root()
    log_dir = project_root / "data" / "silver" / "_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "imf_bronze_to_silver_log.csv"

    columns = [
        "run_at",
        "source_bucket",
        "dest_bucket",
        "source_object",
        "dest_object",
        "status",
        "bytes",
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
