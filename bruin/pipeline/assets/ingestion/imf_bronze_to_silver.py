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
    raw = os.environ.get("ECODATA_VARS", "")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("ECODATA_VARS is not valid JSON.") from exc


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
    return {
        "default": {"drop_columns": [], "rename_columns": {}},
        "datasets": {},
        "enrichments": {},
    }


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
    payload.setdefault("enrichments", {})
    if not isinstance(payload["default"], dict) or not isinstance(payload["datasets"], dict):
        raise ValueError("Transform config keys 'default' and 'datasets' must be objects.")
    if not isinstance(payload["enrichments"], dict):
        raise ValueError("Transform config key 'enrichments' must be an object.")

    payload["default"].setdefault("drop_columns", [])
    payload["default"].setdefault("rename_columns", {})
    payload["enrichments"].setdefault("country_labels", {})
    payload["enrichments"].setdefault("id_countryear", {})
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


def get_enrichment(config: dict[str, Any], key: str, defaults: dict[str, Any]) -> dict[str, Any]:
    enrichments = config.get("enrichments", {})
    if isinstance(enrichments.get(key), dict):
        merged = defaults.copy()
        merged.update(enrichments[key])
        return merged
    return defaults


def build_id_countryear(
    frame: pd.DataFrame, country_col: str, year_col: str, target_col: str, separator: str
) -> pd.DataFrame:
    if country_col not in frame.columns or year_col not in frame.columns:
        return frame

    year_series = pd.to_numeric(frame[year_col], errors="coerce").astype("Int64")
    valid = frame[country_col].notna() & year_series.notna()
    id_series = pd.Series([pd.NA] * len(frame), index=frame.index, dtype="object")
    id_series.loc[valid] = (
        frame.loc[valid, country_col].astype(str).str.strip()
        + separator
        + year_series.loc[valid].astype(str)
    )
    frame[target_col] = id_series
    return frame


def expand_countries_years(
    frame: pd.DataFrame,
    country_col: str,
    label_col: str,
    year_col: str,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    if country_col not in frame.columns:
        return frame

    if label_col not in frame.columns:
        frame[label_col] = pd.NA

    base = frame[[country_col, label_col]].dropna(subset=[country_col]).drop_duplicates()
    years = pd.DataFrame({year_col: list(range(start_year, end_year + 1))})
    base["_key"] = 1
    years["_key"] = 1
    expanded = base.merge(years, on="_key").drop(columns="_key")
    return expanded


def load_countries_lookup(
    client: storage.Client,
    bronze_bucket: storage.Bucket,
    prefix: str,
    transform_config: dict[str, Any],
    label_cfg: dict[str, Any],
) -> dict[str, str]:
    countries_dataset = str(label_cfg.get("source_dataset", "countries"))
    country_col = str(label_cfg.get("country_column", "country"))
    label_col = str(label_cfg.get("label_column", "label"))
    target_col = str(label_cfg.get("target_column", "country_label"))

    for blob in client.list_blobs(bronze_bucket, prefix=prefix):
        if not blob.name.endswith(".parquet") or "/_logs/" in blob.name:
            continue
        if Path(blob.name).stem != countries_dataset:
            continue

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "countries.parquet"
            blob.download_to_filename(tmp_path)
            frame = pd.read_parquet(tmp_path)

        frame, _ = apply_transforms(frame, countries_dataset, transform_config)

        label_candidates = [target_col, label_col, "country_label", "label"]
        label_column = next((col for col in label_candidates if col in frame.columns), None)
        if label_column is None or country_col not in frame.columns:
            return {}

        mapping_frame = (
            frame[[country_col, label_column]]
            .dropna(subset=[country_col, label_column])
            .drop_duplicates()
        )
        return dict(zip(mapping_frame[country_col], mapping_frame[label_column]))

    return {}


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

    label_cfg = get_enrichment(
        transform_config,
        "country_labels",
        {
            "enabled": True,
            "source_dataset": "countries",
            "country_column": "country",
            "label_column": "label",
            "target_column": "country_label",
        },
    )
    id_cfg = get_enrichment(
        transform_config,
        "id_countryear",
        {
            "enabled": True,
            "country_column": "country",
            "year_column": "year",
            "target_column": "id_countryear",
            "separator": "_",
        },
    )

    countries_lookup: dict[str, str] = {}
    if parse_bool(label_cfg.get("enabled", True)):
        countries_lookup = load_countries_lookup(
            client,
            bronze_bucket,
            prefix,
            transform_config,
            label_cfg,
        )

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

                    # Apply enrichment logic based on transform config.
                    if dataset_name == str(label_cfg.get("source_dataset", "countries")):
                        # Drop rows with missing country labels (e.g., ATI/ATL) before expansion.
                        label_col = str(label_cfg.get("target_column", "country_label"))
                        if label_col in frame.columns:
                            frame = frame[frame[label_col].notna()].copy()

                        # Expand countries across a fixed year range when configured.
                        dataset_cfg = transform_config.get("datasets", {}).get(dataset_name, {})
                        year_range = dataset_cfg.get("expand_year_range")
                        if isinstance(year_range, dict):
                            start_year = parse_int(year_range.get("start"))
                            end_year = parse_int(year_range.get("end"))
                            if start_year is not None and end_year is not None:
                                frame = expand_countries_years(
                                    frame,
                                    str(label_cfg.get("country_column", "country")),
                                    label_col,
                                    str(id_cfg.get("year_column", "year")),
                                    start_year,
                                    end_year,
                                )

                        frame = build_id_countryear(
                            frame,
                            str(id_cfg.get("country_column", "country")),
                            str(id_cfg.get("year_column", "year")),
                            str(id_cfg.get("target_column", "id_countryear")),
                            str(id_cfg.get("separator", "_")),
                        )
                    else:
                        if parse_bool(label_cfg.get("enabled", True)):
                            target_label = str(label_cfg.get("target_column", "country_label"))
                            country_col = str(label_cfg.get("country_column", "country"))
                            if target_label in frame.columns:
                                if countries_lookup:
                                    frame[target_label] = frame[target_label].fillna(
                                        frame[country_col].map(countries_lookup)
                                    )
                            else:
                                frame[target_label] = (
                                    frame[country_col].map(countries_lookup)
                                    if countries_lookup
                                    else pd.NA
                                )

                        if parse_bool(id_cfg.get("enabled", True)):
                            frame = build_id_countryear(
                                frame,
                                str(id_cfg.get("country_column", "country")),
                                str(id_cfg.get("year_column", "year")),
                                str(id_cfg.get("target_column", "id_countryear")),
                                str(id_cfg.get("separator", "_")),
                            )
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
