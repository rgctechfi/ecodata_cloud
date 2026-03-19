SHELL := /bin/bash

.PHONY: help auth-check terraform-init terraform-fmt terraform-validate terraform-plan terraform-apply provision bruin-convert ingest-bronze full all

help:
	@echo "Targets:"
	@echo "  make auth-check  # verify gcloud + ADC authentication"
	@echo "  make provision   # terraform init + apply (creates GCP resources)"
	@echo "  make bruin-convert # run the Bruin asset to convert JSON to Parquet"
	@echo "  make ingest-bronze # upload Parquet files to the bronze bucket"
	@echo "  make full         # provision infra, run Bruin, upload to bronze"
	@echo "  make all          # full project run (provision + convert + upload)"
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

bruin-convert:
	bruin run bruin/pipeline/assets/ingestion/imf_json_to_parquet.py

ingest-bronze: auth-check
	@command -v gsutil >/dev/null 2>&1 || (echo "gsutil is not installed. Install Google Cloud CLI first." && exit 1)
	gsutil -m rsync -r data/parquet gs://ecodatacloud-ds-bronze/parquet

full: provision bruin-convert ingest-bronze

all: full
