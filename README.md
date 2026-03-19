**Overview**
This project builds a reproducible data pipeline around IMF DataMapper indicators. The goal is to collect economic data, store it in a cloud data lake, and prepare it for analysis and dashboards.

**Problem**
Provide a clean, repeatable pipeline that aggregates macroeconomic indicators across countries and years, and makes them available for downstream analytics.

**Stack**
- Cloud: Google Cloud Platform (GCP)
- IaC: Terraform
- Orchestration: Bruin (CLI-driven batch runs)
- Data lake: Google Cloud Storage (bronze + silver)
- Data warehouse: BigQuery (gold dataset)
- Transformations: dbt or SQL in BigQuery (planned)
- Dashboard: Looker Studio or similar (planned)
- Languages: Python, SQL



**Architecture (Batch)**
1. Extract IMF API data into JSON: `data/raw`
2. Convert JSON to Parquet: `data/parquet`
3. Upload Parquet to GCS bronze: `gs://ecodatacloud-ds-bronze/parquet`
4. Promote Parquet to GCS silver: `gs://ecodatacloud-ds-silver/parquet`
5. Transform into BigQuery tables (planned)
6. Build a dashboard with at least two tiles (planned)

**Evaluation Criteria Mapping**
- Problem description: The project target and data scope are defined in this README.
- Cloud: GCP is used, and all infrastructure is created with Terraform.
- Batch / orchestration: Bruin orchestrates batch assets; runs are triggered via CLI or Makefile.
- Data warehouse: BigQuery dataset is provisioned; partitioning/clustering is planned in transformation step.
- Transformations: To be implemented with dbt or BigQuery SQL models.
- Dashboard: To be implemented with two tiles after warehouse modeling.
- Reproducibility: Makefile and step-by-step instructions are provided below.

**Prerequisites**
1. macOS/Linux
2. Google Cloud CLI (`gcloud` + `gsutil`)
3. Terraform
4. Bruin CLI
5. Python (optional, only if you want to open the notebook)

**GCP Auth**
1. `gcloud auth application-default login`
2. `gcloud config set project ecodatacloud`
3. `gcloud auth application-default print-access-token`

**Billing Requirement**
GCS buckets require an active billing account. If you see:
`Error 403: The billing account for the owning project is disabled`
then link a billing account and re-run Terraform.

**Provision Infrastructure (Terraform)**
1. `cd terraform`
2. `terraform init`
3. `terraform plan`
4. `terraform apply`

This creates:
1. Service account `bruin-ingestor`
2. IAM roles: Storage Object Admin + BigQuery Data Editor
3. Buckets: bronze + silver
4. BigQuery dataset

**Data Ingestion**
1. Extract IMF data into JSON with Bruin:
   `bruin run bruin/pipeline/assets/ingestion/imf_api_extract.py`
2. Convert JSON to Parquet with Bruin:
   `bruin run bruin/pipeline/assets/ingestion/imf_json_to_parquet.py`
3. Upload Parquet to bronze:
   `gsutil -m rsync -r data/parquet gs://ecodatacloud-ds-bronze/parquet`
4. Promote Parquet from bronze to silver:
   `bruin run bruin/pipeline/assets/ingestion/imf_bronze_to_silver.py`

**Orchestration (End-to-End)**
1. First run: provision infrastructure with `make provision`.
2. Run the full batch with `make full` (extract → convert → upload → promote).
3. Subsequent runs can use `make full` directly without reprovisioning.
4. For scheduling, run `make full` from a cron job or a managed scheduler (Cloud Scheduler / GitHub Actions) with the same environment variables.

**Makefile Targets**
- `make auth-check`: verify gcloud and ADC authentication
- `make provision`: Terraform init + apply
- `make bruin-extract`: IMF API extraction to JSON
- `make bruin-convert`: JSON to Parquet conversion
- `make ingest-bronze`: upload Parquet to bronze bucket
- `make promote-silver`: copy bronze parquet objects to the silver bucket
- `make full`: provision + extract + convert + upload + promote to silver
- `make all`: same as `make full`

**Tool Equivalents**
- `make auth-check`
  - `gcloud auth application-default print-access-token`
  - `gcloud config get-value project`
- `make provision`
  - `terraform -chdir=terraform init`
  - `terraform -chdir=terraform apply`
- `make bruin-convert`
  - `bruin run bruin/pipeline/assets/ingestion/imf_json_to_parquet.py`
- `make bruin-extract`
  - `bruin run bruin/pipeline/assets/ingestion/imf_api_extract.py`
- `make ingest-bronze`
  - `gsutil -m rsync -r data/parquet gs://ecodatacloud-ds-bronze/parquet`
- `make promote-silver`
  - `bruin run bruin/pipeline/assets/ingestion/imf_bronze_to_silver.py`

**Notes**
- The notebook `scripts/api_data.ipynb` is kept for exploration; the automated pipeline uses the Bruin asset instead.
- For project context, read `data/raw/context.md`.

**Batch Details (Bruin)**
Batch orchestration is CLI-driven and fully automated via Bruin assets plus a Makefile target:
1. `bruin/pipeline/assets/ingestion/imf_api_extract.py` downloads IMF DataMapper JSON into `data/raw/*` and writes a log at `data/raw/api_download_log.txt`.
2. `bruin/pipeline/assets/ingestion/imf_json_to_parquet.py` converts every JSON file to Parquet under `data/parquet/*` and writes a log at `data/parquet/_logs/imf_json_to_parquet_log.csv`.
3. `bruin/pipeline/assets/ingestion/imf_bronze_to_silver.py` copies parquet objects from bronze to silver and writes a log at `data/silver/_logs/imf_bronze_to_silver_log.csv`.
4. `make full` runs the end-to-end batch: provision infra, extract, convert, upload, then promote to silver.

**Batch Configuration**
Batch parameters are passed via `BRUIN_VARS` as JSON. Example:
`BRUIN_VARS='{"datasets":["gdp_per_capita_usd"],"periods":["2019","2020"],"dry_run":true,"max_objects":5}'`

Extraction parameters:
1. `datasets`: list of dataset keys to download.
2. `periods`: list of years to request from IMF (ignored for `countries`).
Available dataset keys: `gdp_per_capita_usd`, `gdp_ppp_world_share`, `gdp_per_capita_ppp`, `unemployment_rate`, `public_debt_gdp`, `inflation_avg_consumer`, `countries`.

Promotion (bronze → silver) parameters:
1. `bronze_bucket`: source bucket name. Default: `ecodatacloud-ds-bronze`.
2. `silver_bucket`: destination bucket name. Default: `ecodatacloud-ds-silver`.
3. `prefix`: object prefix to copy. Default: `parquet/`.
4. `overwrite`: overwrite existing objects in silver. Default: `false`.
5. `dry_run`: log actions without copying. Default: `false`.
6. `max_objects`: limit the number of objects processed (useful for testing).
