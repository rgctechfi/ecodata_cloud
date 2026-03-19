"""@bruin

name: ingestion.imf_api_extract

type: python

image: python:3.11

@bruin"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests


def resolve_project_root() -> Path:
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


def fetch_json(url: str) -> dict:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def format_bytes(num: int) -> str:
    value = float(num)
    for unit in ["B", "KiB", "MiB", "GiB"]:
        if value < 1024 or unit == "GiB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GiB"


def build_url(base_url: str, periods: list[str] | None) -> str:
    if not periods:
        return base_url
    separator = "&" if "?" in base_url else "?"
    period_str = ",".join(periods)
    return f"{base_url}{separator}periods={period_str}"


def run_extract() -> None:
    project_root = resolve_project_root()
    data_raw = project_root / "data" / "raw"

    path_gdp = data_raw / "gdp"
    path_inflation = data_raw / "inflation"
    path_employment = data_raw / "employment"
    path_debt = data_raw / "debt"
    path_countries = data_raw / "countries"

    for p in [path_gdp, path_inflation, path_employment, path_debt, path_countries]:
        p.mkdir(parents=True, exist_ok=True)

    # IMF DataMapper endpoints
    url_gdp_pc = "https://www.imf.org/external/datamapper/api/v1/NGDPDPC"
    url_gdp_ppp_world = "https://www.imf.org/external/datamapper/api/v1/PPPSH"
    url_gdp_pc_ppp = "https://www.imf.org/external/datamapper/api/v1/PPPPC"
    url_unemployment = "https://www.imf.org/external/datamapper/api/v1/LUR"
    url_debt = "https://www.imf.org/external/datamapper/api/v1/GGXWDG_NGDP"
    url_inflation = "https://www.imf.org/external/datamapper/api/v1/PCPIECH"
    url_countries = "https://www.imf.org/external/datamapper/api/v1/countries"

    endpoints = {
        "gdp_per_capita_usd": (path_gdp, url_gdp_pc),
        "gdp_ppp_world_share": (path_gdp, url_gdp_ppp_world),
        "gdp_per_capita_ppp": (path_gdp, url_gdp_pc_ppp),
        "unemployment_rate": (path_employment, url_unemployment),
        "public_debt_gdp": (path_debt, url_debt),
        "inflation_avg_consumer": (path_inflation, url_inflation),
        "countries": (path_countries, url_countries),
    }

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    datasets_filter = bruin_vars.get("datasets")
    periods = bruin_vars.get("periods")

    if isinstance(periods, str):
        periods = [p.strip() for p in periods.split(",") if p.strip()]

    log_lines = []
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for name, (folder, url) in endpoints.items():
        if datasets_filter and name not in datasets_filter:
            continue

        try:
            final_url = url if name == "countries" else build_url(url, periods)
            data = fetch_json(final_url)
            file_path = folder / f"{name}.json"
            write_json(file_path, data)
            size_bytes = file_path.stat().st_size
            file_type = file_path.suffix.lstrip(".")
            log_lines.append(
                f"[{timestamp}] {name} | url={final_url} | file={file_path} | "
                f"type={file_type} | size={size_bytes} bytes ({format_bytes(size_bytes)})"
            )
        except Exception as exc:
            log_lines.append(f"[{timestamp}] {name} | url={url} | ERROR: {exc}")

    log_path = data_raw / "api_download_log.txt"
    log_path.write_text("\n".join(log_lines) + "\n")
    print(f"Wrote API log to: {log_path}")


run_extract()
