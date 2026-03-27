<h1 align="center" style="color:#0B2D5C; font-size: 40px; margin-bottom: 8px;">
  𝙎𝙚𝙩𝙪𝙥
</h1>

This guide covers the full environment setup, GCP/IAM, and infrastructure provisioning.

## <span style="color:#0B2D5C;">**𝙋𝙧𝙚𝙧𝙚𝙦𝙪𝙞𝙨𝙞𝙩𝙚𝙨**</span>
1. 𝙢𝙖𝙘𝙊𝙎/𝙇𝙞𝙣𝙪𝙭
2. 𝙂𝙤𝙤𝙜𝙡𝙚 𝘾𝙡𝙤𝙪𝙙 𝘾𝙇𝙄 (`gcloud`)
3. 𝙏𝙚𝙧𝙧𝙖𝙛𝙤𝙧𝙢
4. 𝘽𝙧𝙪𝙞𝙣 𝘾𝙇𝙄
5. `make`
6. 𝙋𝙮𝙩𝙝𝙤𝙣 (optional, only if you want to open the notebook)

## <span style="color:#0B2D5C;">**𝙐𝙑 𝙀𝙣𝙫𝙞𝙧𝙤𝙣𝙢𝙚𝙣𝙩 (𝙊𝙥𝙩𝙞𝙤𝙣𝙖𝙡)**</span>
Use `uv` for local development only (Bruin installs its own dependencies per asset):
```bash
uv venv
source .venv/bin/activate
uv pip install -r bruin/pipeline/assets/ingestion/requirements.txt
```

## <span style="color:#0B2D5C;">**𝙂𝘾𝙋 𝘼𝙪𝙩𝙝**</span>
Supported local auth modes:
1. User ADC:
   `gcloud auth application-default login`
   `gcloud config set project ecodatacloud`
   `gcloud auth application-default print-access-token`
2. Service account key:
   `export CLOUDSDK_CONFIG="$(pwd)/.gcloud"`
   `export GOOGLE_APPLICATION_CREDENTIALS="$(find "$(pwd)/.gcloud/credentials" -maxdepth 1 -name '*.json' | head -n 1)"`

In the current repository setup:
- `make` targets always use the repo-local GCP config in `.gcloud`.
- If a key file exists under `.gcloud/credentials/*.json`, `make` exports `GOOGLE_APPLICATION_CREDENTIALS` automatically.
- Otherwise, `make` falls back to your user ADC session.
- Direct `bruin`, `bq`, `gsutil`, and `terraform` commands use whatever auth is active in your current shell.

## <span style="color:#0B2D5C;">**𝙄𝘼𝙈 𝙍𝙚𝙦𝙪𝙞𝙧𝙚𝙢𝙚𝙣𝙩𝙨**</span>
User/Owner account (used to run Terraform and manage project resources): `roles/owner` (or equivalent admin permissions).

Service account `bruin-ingestor@ecodatacloud.iam.gserviceaccount.com` is provisioned by Terraform for the project, but it is **not** the credential used by local Bruin runs unless you explicitly wire it into the runtime yourself.

Important implementation detail:
- the repository does **not** rely on manual `gcloud iam service-accounts create` or `gcloud projects add-iam-policy-binding` commands
- the `bruin-ingestor` service account and its IAM bindings are created declaratively by Terraform in [main.tf](/Users/rgctechfi/Projects/ecodata_cloud/terraform/main.tf)
- in practice, running `terraform apply` or `make provision` is the documented way to create the service account and apply IAM roles in this project

Terraform currently grants this service account:
- `roles/storage.admin`
- `roles/storage.objectAdmin`
- `roles/bigquery.admin`
- `roles/bigquery.dataEditor`
- `roles/iam.serviceAccountAdmin`
- `roles/serviceusage.serviceUsageAdmin`

Required APIs managed by Terraform in this project:
- `serviceusage.googleapis.com`
- `iam.googleapis.com`
- `storage.googleapis.com`
- `bigquery.googleapis.com`

## <span style="color:#0B2D5C;">**𝙒𝙝𝙮 𝙏𝙚𝙧𝙧𝙖𝙛𝙤𝙧𝙢 𝙈𝙪𝙨𝙩 𝙍𝙪𝙣 𝙒𝙞𝙩𝙝 𝙩𝙝𝙚 𝙊𝙬𝙣𝙚𝙧 𝘼𝙘𝙘𝙤𝙪𝙣𝙩**</span>
Terraform creates and manages IAM bindings and project-level resources (service account, enabled APIs, buckets, and BigQuery dataset). Those changes require Owner-level permissions. If you run Terraform with the service account, you can end up in a bootstrap problem where the account does not yet have the rights it needs. Using your Owner/admin ADC account avoids that loop and keeps IAM changes auditable.

In the current repository setup:
- Terraform is run with your admin-capable account.
- `make` targets can use either your ADC session or the repo-local service account key.
- The `bruin-ingestor` service account is provisioned for infrastructure consistency and optional future automation.

## <span style="color:#0B2D5C;">**𝘽𝙞𝙡𝙡𝙞𝙣𝙜 𝙍𝙚𝙦𝙪𝙞𝙧𝙚𝙢𝙚𝙣𝙩**</span>
GCS buckets require an active billing account. If you see:
`Error 403: The billing account for the owning project is disabled`
then link a billing account and re-run Terraform.

## <span style="color:#0B2D5C;">**𝙋𝙧𝙤𝙫𝙞𝙨𝙞𝙤𝙣 𝙄𝙣𝙛𝙧𝙖𝙨𝙩𝙧𝙪𝙘𝙩𝙪𝙧𝙚 (𝙏𝙚𝙧𝙧𝙖𝙛𝙤𝙧𝙢)**</span>
This section explains the infrastructure bootstrap logic. The runnable end-to-end execution order remains documented in [Quickstart.md](/Users/rgctechfi/Projects/ecodata_cloud/Quickstart.md).

1. 𝙘𝙙 `terraform`
2. 𝙧𝙪𝙣 `terraform init`
3. 𝙧𝙪𝙣 `terraform plan`
4. 𝙧𝙪𝙣 `terraform apply`

Alternative: `make provision` runs `auth-check` + `terraform init` + `terraform plan` + `terraform apply`.

This creates:
1. Service account `bruin-ingestor`
2. IAM roles: Storage Admin, Storage Object Admin, BigQuery Admin, BigQuery Data Editor, IAM Service Account Admin, and Service Usage Admin
3. Buckets: bronze + silver
4. BigQuery dataset
