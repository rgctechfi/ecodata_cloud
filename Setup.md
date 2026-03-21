<h1 align="center" style="color:#0B2D5C; font-size: 40px; margin-bottom: 8px;">
  𝙎𝙚𝙩𝙪𝙥
</h1>

This guide covers the full environment setup, GCP/IAM, and infrastructure provisioning.

## <span style="color:#0B2D5C;">**𝙋𝙧𝙚𝙧𝙚𝙦𝙪𝙞𝙨𝙞𝙩𝙚𝙨**</span>
1. 𝙢𝙖𝙘𝙊𝙎/𝙇𝙞𝙣𝙪𝙭
2. 𝙂𝙤𝙤𝙜𝙡𝙚 𝘾𝙡𝙤𝙪𝙙 𝘾𝙇𝙄 (`gcloud`)
3. 𝙏𝙚𝙧𝙧𝙖𝙛𝙤𝙧𝙢
4. 𝘽𝙧𝙪𝙞𝙣 𝘾𝙇𝙄
5. 𝙋𝙮𝙩𝙝𝙤𝙣 (optional, only if you want to open the notebook)

## <span style="color:#0B2D5C;">**𝙐𝙑 𝙀𝙣𝙫𝙞𝙧𝙤𝙣𝙢𝙚𝙣𝙩 (𝙊𝙥𝙩𝙞𝙤𝙣𝙖𝙡)**</span>
Use `uv` for local development only (Bruin installs its own dependencies per asset):
```bash
uv venv
source .venv/bin/activate
uv pip install -r bruin/pipeline/assets/ingestion/requirements.txt
```

## <span style="color:#0B2D5C;">**𝙂𝘾𝙋 𝘼𝙪𝙩𝙝**</span>
1. `gcloud auth application-default login`
2. `gcloud config set project ecodatacloud`
3. `gcloud auth application-default print-access-token`

## <span style="color:#0B2D5C;">**𝙄𝘼𝙈 𝙍𝙚𝙦𝙪𝙞𝙧𝙚𝙢𝙚𝙣𝙩𝙨**</span>
User/Owner account (used by Terraform when it manages IAM): `roles/owner` (or equivalent admin permissions).

Service account `bruin-ingestor@ecodatacloud.iam.gserviceaccount.com` (used by Bruin assets) requires:
- `roles/storage.admin`
- `roles/storage.objectAdmin`
- `roles/bigquery.dataOwner` (needed for dataset updates)
- `roles/bigquery.dataEditor`
- `roles/iam.serviceAccountAdmin`
- `roles/resourcemanager.projectIamAdmin`
- `roles/serviceusage.serviceUsageAdmin`

Required APIs (enable once in the project):
- `serviceusage.googleapis.com`
- `cloudresourcemanager.googleapis.com`
- `iam.googleapis.com`
- `storage.googleapis.com`
- `bigquery.googleapis.com`

## <span style="color:#0B2D5C;">**𝙒𝙝𝙮 𝙏𝙚𝙧𝙧𝙖𝙛𝙤𝙧𝙢 𝙈𝙪𝙨𝙩 𝙍𝙪𝙣 𝙒𝙞𝙩𝙝 𝙩𝙝𝙚 𝙊𝙬𝙣𝙚𝙧 𝘼𝙘𝙘𝙤𝙪𝙣𝙩**</span>
Terraform creates and manages IAM bindings and project-level resources (service account roles, APIs, buckets, and BigQuery dataset). Those changes require Owner-level permissions. If you run Terraform with the service account, you can easily end up in a permission loop where the account does not yet have the rights it needs to grant itself. Using your Owner ADC account avoids that bootstrap problem and keeps IAM changes auditable. Bruin assets still run with the service account at execution time.

IAM setup commands (run once with an Owner account):
```bash
gcloud auth login
gcloud config set project ecodatacloud

gcloud projects add-iam-policy-binding ecodatacloud \
  --member=serviceAccount:bruin-ingestor@ecodatacloud.iam.gserviceaccount.com \
  --role=roles/storage.admin

gcloud projects add-iam-policy-binding ecodatacloud \
  --member=serviceAccount:bruin-ingestor@ecodatacloud.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin

gcloud projects add-iam-policy-binding ecodatacloud \
  --member=serviceAccount:bruin-ingestor@ecodatacloud.iam.gserviceaccount.com \
  --role=roles/bigquery.dataOwner

gcloud projects add-iam-policy-binding ecodatacloud \
  --member=serviceAccount:bruin-ingestor@ecodatacloud.iam.gserviceaccount.com \
  --role=roles/bigquery.dataEditor

gcloud projects add-iam-policy-binding ecodatacloud \
  --member=serviceAccount:bruin-ingestor@ecodatacloud.iam.gserviceaccount.com \
  --role=roles/iam.serviceAccountAdmin

gcloud projects add-iam-policy-binding ecodatacloud \
  --member=serviceAccount:bruin-ingestor@ecodatacloud.iam.gserviceaccount.com \
  --role=roles/resourcemanager.projectIamAdmin

gcloud projects add-iam-policy-binding ecodatacloud \
  --member=serviceAccount:bruin-ingestor@ecodatacloud.iam.gserviceaccount.com \
  --role=roles/serviceusage.serviceUsageAdmin
```

## <span style="color:#0B2D5C;">**𝘽𝙞𝙡𝙡𝙞𝙣𝙜 𝙍𝙚𝙦𝙪𝙞𝙧𝙚𝙢𝙚𝙣𝙩**</span>
GCS buckets require an active billing account. If you see:
`Error 403: The billing account for the owning project is disabled`
then link a billing account and re-run Terraform.

## <span style="color:#0B2D5C;">**𝙋𝙧𝙤𝙫𝙞𝙨𝙞𝙤𝙣 𝙄𝙣𝙛𝙧𝙖𝙨𝙩𝙧𝙪𝙘𝙩𝙪𝙧𝙚 (𝙏𝙚𝙧𝙧𝙖𝙛𝙤𝙧𝙢)**</span>
1. 𝙘𝙙 `terraform`
2. 𝙧𝙪𝙣 `terraform init`
3. 𝙧𝙪𝙣 `terraform plan`
4. 𝙧𝙪𝙣 `terraform apply`

Alternative: `make provision` runs `terraform init` + `terraform plan` + `terraform apply`.

This creates:
1. Service account `bruin-ingestor`
2. IAM roles: Storage Object Admin + BigQuery Data Editor
3. Buckets: bronze + silver
4. BigQuery dataset
