"""@bruin

name: ingestion.imf_quality_checks

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


def default_quality_config() -> dict[str, Any]:
    return {
        "default": {
            "required_columns": ["country"],
            "not_null_columns": ["country"],
            "numeric_columns": ["year", "value"],
            "min_year": 1960,
            "max_year": 2035,
        },
        "datasets": {
            "countries": {
                "required_columns": ["country", "label"],
                "not_null_columns": ["country", "label"],
                "numeric_columns": [],
            }
        },
    }


def load_quality_config(project_root: Path, override_path: str | None) -> dict[str, Any]:
    if override_path:
        config_path = Path(override_path).expanduser()
    else:
        config_path = project_root / "bruin" / "pipeline" / "config" / "quality_checks.json"

    if not config_path.exists():
        return default_quality_config()

    payload = json.loads(config_path.read_text())
    if not isinstance(payload, dict):
        raise ValueError("Quality config must be a JSON object.")

    payload.setdefault("default", {})
    payload.setdefault("datasets", {})
    if not isinstance(payload["default"], dict) or not isinstance(payload["datasets"], dict):
        raise ValueError("Quality config keys 'default' and 'datasets' must be objects.")

    return payload


def pick_list(config: dict[str, Any], key: str, fallback: list[str]) -> list[str]:
    if key in config:
        value = config.get(key)
        if isinstance(value, list):
            return [str(item) for item in value]
    return fallback


def pick_int(config: dict[str, Any], key: str, fallback: int | None) -> int | None:
    if key in config:
        value = config.get(key)
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return int(value)
            except ValueError:
                return fallback
    return fallback


def merge_checks(config: dict[str, Any], dataset_name: str) -> dict[str, Any]:
    defaults = config.get("default", {})
    dataset_cfg = config.get("datasets", {}).get(dataset_name, {})

    merged = {
        "required_columns": pick_list(dataset_cfg, "required_columns", pick_list(defaults, "required_columns", [])),
        "not_null_columns": pick_list(dataset_cfg, "not_null_columns", pick_list(defaults, "not_null_columns", [])),
        "numeric_columns": pick_list(dataset_cfg, "numeric_columns", pick_list(defaults, "numeric_columns", [])),
        "min_year": pick_int(dataset_cfg, "min_year", pick_int(defaults, "min_year", None)),
        "max_year": pick_int(dataset_cfg, "max_year", pick_int(defaults, "max_year", None)),
    }
    return merged


def run_checks(frame: pd.DataFrame, checks: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    columns = set(frame.columns)

    missing = [col for col in checks["required_columns"] if col not in columns]
    if missing:
        issues.append(f"missing_columns={missing}")

    for col in checks["not_null_columns"]:
        if col not in columns:
            continue
        null_count = int(frame[col].isna().sum())
        if null_count > 0:
            issues.append(f"nulls_{col}={null_count}")

    for col in checks["numeric_columns"]:
        if col not in columns:
            continue
        series = pd.to_numeric(frame[col], errors="coerce")
        invalid = int(series.isna().sum() - frame[col].isna().sum())
        if invalid > 0:
            issues.append(f"non_numeric_{col}={invalid}")

    if "year" in columns and (checks["min_year"] is not None or checks["max_year"] is not None):
        year_series = pd.to_numeric(frame["year"], errors="coerce")
        invalid_years = year_series.isna().sum() - frame["year"].isna().sum()
        if invalid_years > 0:
            issues.append(f"invalid_year={int(invalid_years)}")
        if checks["min_year"] is not None:
            below = int((year_series < checks["min_year"]).sum())
            if below > 0:
                issues.append(f"year_below_{checks['min_year']}={below}")
        if checks["max_year"] is not None:
            above = int((year_series > checks["max_year"]).sum())
            if above > 0:
                issues.append(f"year_above_{checks['max_year']}={above}")

    return issues


def run_quality_checks() -> None:
    bruin_vars = load_bruin_vars()

    silver_bucket_name = bruin_vars.get("silver_bucket", "ecodatacloud-ds-silver")
    prefix = bruin_vars.get("prefix", "parquet/")
    max_objects = parse_int(bruin_vars.get("max_objects"))
    fail_on_error = parse_bool(bruin_vars.get("fail_on_error"), default=True)
    config_path = bruin_vars.get("quality_config")

    project_root = resolve_project_root()
    config = load_quality_config(project_root, config_path)

    client = storage.Client()
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logs: list[dict[str, Any]] = []
    errored = 0
    checked = 0

    for blob in client.list_blobs(silver_bucket_name, prefix=prefix):
        if max_objects is not None and checked + errored >= max_objects:
            break

        if not blob.name.endswith(".parquet") or "/_logs/" in blob.name:
            continue

        dataset_name = Path(blob.name).stem
        checks = merge_checks(config, dataset_name)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "silver.parquet"
            blob.download_to_filename(tmp_path)
            frame = pd.read_parquet(tmp_path)

        issues = run_checks(frame, checks)
        status = "ok" if not issues else "error"
        if issues:
            errored += 1
        else:
            checked += 1

        logs.append(
            {
                "run_at": run_at,
                "dataset": dataset_name,
                "status": status,
                "row_count": int(frame.shape[0]),
                "column_count": int(frame.shape[1]),
                "issues": ";".join(issues) if issues else None,
            }
        )

    log_dir = project_root / "data" / "silver" / "_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "imf_quality_checks_log.csv"

    columns = ["run_at", "dataset", "status", "row_count", "column_count", "issues"]
    with log_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(logs)

    print(f"Quality checks summary: ok={checked}, errors={errored}")
    print(f"Wrote quality checks log to: {log_path}")

    if errored > 0 and fail_on_error:
        raise RuntimeError("Quality checks failed. See log for details.")


run_quality_checks()
