"""@bruin

name: ingestion.imf_bronze_upload

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
    raw = os.environ.get("ECODATA_VARS", "")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("ECODATA_VARS is not valid JSON.") from exc


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "t"}
    return default


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


def build_object_name(prefix: str, relative_path: Path) -> str:
    prefix_clean = prefix.strip("/")
    relative_posix = relative_path.as_posix()
    if prefix_clean:
        return f"{prefix_clean}/{relative_posix}"
    return relative_posix


def upload_bronze() -> None:
    bruin_vars = load_bruin_vars()

    bronze_bucket_name = bruin_vars.get("bronze_bucket", "ecodatacloud-ds-bronze")
    prefix = bruin_vars.get("prefix", "parquet/")
    overwrite = parse_bool(bruin_vars.get("overwrite"))
    dry_run = parse_bool(bruin_vars.get("dry_run"))
    max_files = parse_int(bruin_vars.get("max_files"))
    local_dir = bruin_vars.get("local_parquet_dir")

    project_root = resolve_project_root()
    parquet_root = Path(local_dir) if local_dir else project_root / "data" / "parquet"
    if not parquet_root.exists():
        raise FileNotFoundError(f"Parquet directory not found: {parquet_root}")

    client = storage.Client()
    bronze_bucket = ensure_bucket(client, bronze_bucket_name)

    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logs: list[dict[str, Any]] = []
    uploaded = 0
    skipped = 0
    errored = 0

    parquet_files = sorted(parquet_root.rglob("*.parquet"))
    for parquet_path in parquet_files:
        if max_files is not None and uploaded + skipped + errored >= max_files:
            break

        if "_logs" in parquet_path.parts:
            skipped += 1
            continue

        relative_path = parquet_path.relative_to(parquet_root)
        object_name = build_object_name(prefix, relative_path)
        blob = bronze_bucket.blob(object_name)

        try:
            if not overwrite and blob.exists():
                logs.append(
                    {
                        "run_at": run_at,
                        "source_path": str(parquet_path),
                        "dest_object": object_name,
                        "status": "skip_exists",
                        "bytes": parquet_path.stat().st_size,
                        "error": None,
                    }
                )
                skipped += 1
                continue

            if dry_run:
                status = "dry_run"
            else:
                blob.upload_from_filename(parquet_path)
                status = "ok"
                uploaded += 1

            logs.append(
                {
                    "run_at": run_at,
                    "source_path": str(parquet_path),
                    "dest_object": object_name,
                    "status": status,
                    "bytes": parquet_path.stat().st_size,
                    "error": None,
                }
            )
        except Exception as exc:
            logs.append(
                {
                    "run_at": run_at,
                    "source_path": str(parquet_path),
                    "dest_object": object_name,
                    "status": "error",
                    "bytes": parquet_path.stat().st_size,
                    "error": str(exc),
                }
            )
            errored += 1

    log_dir = project_root / "data" / "bronze" / "_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "imf_bronze_upload_log.csv"

    columns = ["run_at", "source_path", "dest_object", "status", "bytes", "error"]
    with log_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(logs)

    print(f"Bronze upload summary: uploaded={uploaded}, skipped={skipped}, errors={errored}")
    print(f"Wrote bronze upload log to: {log_path}")


upload_bronze()
