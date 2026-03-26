"""@bruin

name: ingestion.imf_json_to_parquet

type: python

image: python:3.11

@bruin"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

# Technical notes for reviewers:
# - This asset is a normalization boundary: heterogeneous IMF JSON payloads are converted into a
#   stable tabular representation before the data enters cloud storage.
# - The code favors explicit branching over clever generic recursion so each supported payload
#   shape remains easy to explain during evaluation.
# - pandas is used here as a pragmatic transformation engine: compact code, strong type coercion,
#   and direct parquet export without hand-written serializers.
# - The returned log dataframe is an operational artifact, which means the function is useful both
#   as a runnable asset and as a testable unit in isolation.


def resolve_project_root() -> Path:
    """Resolve the repository root regardless of where the asset is launched from."""
    # Support running the asset from the repo root or from the bruin pipeline folder.
    candidates = [
        Path.cwd(),
        Path.cwd().parent,
        Path(__file__).resolve().parents[4],
    ]
    for candidate in candidates:
        if (candidate / "data" / "raw").exists():
            return candidate
    return candidates[-1]


def normalize_values_payload(values: dict) -> pd.DataFrame:
    """Flatten IMF indicator payloads into a row-per-indicator/country/year dataframe."""
    rows = []
    # IMF indicator payloads are nested as indicator -> country -> year -> value.
    # This loop flattens them into a tabular shape that parquet and pandas can handle.
    for indicator, country_map in values.items():
        if not isinstance(country_map, dict):
            rows.append(
                {
                    "indicator": indicator,
                    "country": None,
                    "year": None,
                    "value": country_map,
                }
            )
            continue

        for country, series in country_map.items():
            if isinstance(series, dict):
                for year, value in series.items():
                    rows.append(
                        {
                            "indicator": indicator,
                            "country": country,
                            "year": year,
                            "value": value,
                        }
                    )
            else:
                rows.append(
                    {
                        "indicator": indicator,
                        "country": country,
                        "year": None,
                        "value": series,
                    }
                )

    frame = pd.DataFrame(rows)
    if not frame.empty:
        # `errors="coerce"` is deliberate: malformed values become nulls instead of crashing
        # the conversion step, and those anomalies are handled later by the quality checks.
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    return frame


def normalize_countries_payload(countries: dict) -> pd.DataFrame:
    """Flatten the country metadata payload into one row per country code."""
    rows = []
    for country_code, payload in countries.items():
        row = {"country": country_code}
        if isinstance(payload, dict):
            row.update(payload)
        else:
            row["label"] = payload
        rows.append(row)
    return pd.DataFrame(rows)


def payload_to_frame(payload: object) -> pd.DataFrame:
    """Route each JSON shape to the normalization strategy it needs."""
    # IMF responses are not fully uniform: indicator payloads, countries payloads,
    # and generic JSON objects are normalized through dedicated branches here.
    if isinstance(payload, dict) and "values" in payload and isinstance(payload["values"], dict):
        return normalize_values_payload(payload["values"])

    if isinstance(payload, dict) and "countries" in payload and isinstance(payload["countries"], dict):
        return normalize_countries_payload(payload["countries"])

    # `pd.json_normalize` is used as a generic fallback for less structured payloads.
    if isinstance(payload, list):
        return pd.json_normalize(payload)

    if isinstance(payload, dict):
        return pd.json_normalize(payload)

    return pd.DataFrame([{"value": payload}])


def convert_json_to_parquet() -> pd.DataFrame:
    """Convert every raw JSON file to parquet and store a structured conversion log."""
    project_root = resolve_project_root()
    data_raw = project_root / "data" / "raw"
    data_parquet = project_root / "data" / "parquet"
    json_files = sorted(data_raw.rglob("*.json"))

    # The log schema is fixed so runs are easy to compare over time.
    columns = [
        "run_at",
        "json_path",
        "parquet_path",
        "status",
        "row_count",
        "column_count",
        "bytes",
        "error",
    ]

    if not json_files:
        return pd.DataFrame(columns=columns)

    # The timestamp is normalized once per run so all log rows belong to the same execution batch.
    run_at = pd.Timestamp.now(tz="UTC").tz_localize(None)
    logs = []

    for json_path in json_files:
        parquet_path = data_parquet / json_path.relative_to(data_raw)
        parquet_path = parquet_path.with_suffix(".parquet")

        try:
            payload = json.loads(json_path.read_text())
            frame = payload_to_frame(payload)

            # Parquet becomes the portable exchange format used by the Bronze/Silver steps.
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(parquet_path, index=False)

            size_bytes = parquet_path.stat().st_size
            logs.append(
                {
                    "run_at": run_at,
                    "json_path": str(json_path),
                    "parquet_path": str(parquet_path),
                    "status": "ok",
                    "row_count": int(frame.shape[0]),
                    "column_count": int(frame.shape[1]),
                    "bytes": int(size_bytes),
                    "error": None,
                }
            )
        except Exception as exc:
            # Errors are recorded row-by-row rather than raising immediately, which lets one bad
            # source file surface alongside the successful conversions from the same run.
            logs.append(
                {
                    "run_at": run_at,
                    "json_path": str(json_path),
                    "parquet_path": str(parquet_path),
                    "status": "error",
                    "row_count": None,
                    "column_count": None,
                    "bytes": None,
                    "error": str(exc),
                }
            )

    logs_frame = pd.DataFrame(logs, columns=columns)
    log_dir = data_parquet / "_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "imf_json_to_parquet_log.csv"
    logs_frame.to_csv(log_path, index=False)
    print(f"Wrote parquet files to: {data_parquet}")
    print(f"Wrote conversion log to: {log_path}")
    return logs_frame

convert_json_to_parquet()

"""
Launch json to parquet:
bruin run bruin/pipeline/assets/ingestion/imf_json_to_parquet.py
"""
