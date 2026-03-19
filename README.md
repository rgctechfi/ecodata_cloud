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
4. Transform into BigQuery tables (planned)
5. Build a dashboard with at least two tiles (planned)

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
5. Python (for the notebook extraction)

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
1. Extract IMF data into JSON: run `scripts/api_data.ipynb`
2. Convert JSON to Parquet with Bruin:
   `bruin run bruin/pipeline/assets/ingestion/imf_json_to_parquet.py`
3. Upload Parquet to bronze:
   `gsutil -m rsync -r data/parquet gs://ecodatacloud-ds-bronze/parquet`

**Makefile Targets**
- `make auth-check`: verify gcloud and ADC authentication
- `make provision`: Terraform init + apply
- `make bruin-convert`: JSON to Parquet conversion
- `make ingest-bronze`: upload Parquet to bronze bucket
- `make full`: provision + convert + upload
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
- `make ingest-bronze`
  - `gsutil -m rsync -r data/parquet gs://ecodatacloud-ds-bronze/parquet`

**Notes**
- The extraction step is currently manual via notebook. You can later automate it into a Bruin asset.
- For project context, read `data/raw/context.md`.
