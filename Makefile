SHELL := /bin/bash
BRUIN_VARS ?=
export BRUIN_VARS
# BRUIN_VARS is a JSON string passed to Bruin assets.
# Examples:
#   BRUIN_VARS='{"datasets":["gdp_per_capita_usd"],"periods":["2020"]}' make full
#   BRUIN_VARS='{"dry_run":true,"max_objects":3}' make quality-checks
#   BRUIN_VARS='{"overwrite":true}' make promote-silver

.PHONY: help auth-check terraform-init terraform-fmt terraform-validate terraform-plan terraform-apply provision bruin-extract bruin-convert ingest-bronze promote-silver quality-checks gold-load full init-to-bronze

help:
	@echo "Targets:"
	@echo "  make auth-check  # verify gcloud + ADC authentication"
	@echo "  make provision   # terraform init + apply (creates GCP resources)"
	@echo "  make bruin-extract # extract IMF API data into JSON"
	@echo "  make bruin-convert # run the Bruin asset to convert JSON to Parquet"
	@echo "  make ingest-bronze # upload Parquet files to the bronze bucket"
	@echo "  make promote-silver # copy bronze parquet objects to the silver bucket"
	@echo "  make quality-checks # run Bruin data quality checks on silver"
	@echo "  make gold-load     # load partitioned + clustered gold tables"
	@echo "  make full         # provision infra, extract, convert, upload, promote to silver"
	@echo "  make init-to-bronze # provision + extract + convert + upload (no silver promotion)"
	@echo "  make terraform-plan/terraform-apply/terraform-validate"

auth-check:
	@command -v gcloud >/dev/null 2>&1 || (echo "gcloud is not installed. Install Google Cloud CLI first." && exit 1)
	@gcloud auth application-default print-access-token >/dev/null 2>&1 || (echo "ADC not configured. Run: gcloud auth application-default login" && exit 1)
	@echo "GCP auth OK. Active project: $$(gcloud config get-value project 2>/dev/null)"

terraform-init:
	terraform -chdir=terraform init

terraform-fmt:
	terraform -chdir=terraform fmt

terraform-validate: terraform-init
	terraform -chdir=terraform validate

terraform-plan: terraform-init
	terraform -chdir=terraform plan

terraform-apply: terraform-init
	terraform -chdir=terraform apply

provision: auth-check terraform-apply

bruin-extract:
	bruin run bruin/pipeline/assets/ingestion/imf_api_extract.py

bruin-convert:
	bruin run bruin/pipeline/assets/ingestion/imf_json_to_parquet.py

ingest-bronze: auth-check
	bruin run bruin/pipeline/assets/ingestion/imf_bronze_upload.py

promote-silver: auth-check
	bruin run bruin/pipeline/assets/ingestion/imf_bronze_to_silver.py

quality-checks: auth-check
	bruin run bruin/pipeline/assets/ingestion/imf_quality_checks.py

gold-load: auth-check
	bruin run bruin/pipeline/assets/ingestion/imf_gold_load.py

full: provision bruin-extract bruin-convert ingest-bronze promote-silver

init-to-bronze: provision bruin-extract bruin-convert ingest-bronze
