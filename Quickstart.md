<h1 align="center" style="color:#0B2D5C; font-size: 40px; margin-bottom: 8px;">
  𝙌𝙪𝙞𝙘𝙠𝙨𝙩𝙖𝙧𝙩
</h1>

This guide shows two complete paths (manual bash or Makefile) to run the pipeline from zero to finish.

## <span style="color:#0B2D5C;">**𝘽𝙚𝙛𝙤𝙧𝙚 𝙔𝙤𝙪 𝙎𝙩𝙖𝙧𝙩**</span>
Make sure the environment and infrastructure are ready by following `Setup.md` (GCP/IAM + Terraform + auth).

## <span style="color:#0B2D5C;">**𝙀𝙣𝙙-𝙩𝙤-𝙀𝙣𝙙 𝙊𝙧𝙙𝙚𝙧**</span>
Run the pipeline in this order:
1. Provision infrastructure with Terraform.
2. Extract IMF data and build local Parquet files.
3. Upload Parquet to the Bronze bucket.
4. Promote Bronze data to the Silver bucket with transformations.
5. Run Silver quality checks.
6. Load Gold tables into BigQuery.
7. Build the Gold One Big Table (OBT).
8. Run verification queries.

## <span style="color:#0B2D5C;">**𝙊𝙥𝙩𝙞𝙤𝙣 𝘼 — 𝙈𝙖𝙣𝙪𝙖𝙡 𝘽𝙖𝙨𝙝 (𝙛𝙧𝙤𝙢 𝙯𝙚𝙧𝙤 𝙩𝙤 𝙛𝙞𝙣𝙞𝙨𝙝)**</span>
1. Provision infrastructure:
```bash
terraform -chdir=terraform init
terraform -chdir=terraform plan
terraform -chdir=terraform apply
```

2. Run the ingestion pipeline up to Silver:
```bash
bruin run bruin/pipeline/assets/ingestion/imf_api_extract.py
bruin run bruin/pipeline/assets/ingestion/imf_json_to_parquet.py
bruin run bruin/pipeline/assets/ingestion/imf_bronze_upload.py
bruin run bruin/pipeline/assets/ingestion/imf_bronze_to_silver.py
```

3. Validate Silver:
```bash
bruin run bruin/pipeline/assets/ingestion/imf_quality_checks.py
```

4. Build Gold:
```bash
bruin run bruin/pipeline/assets/ingestion/imf_gold_load.py
bruin run bruin/pipeline/assets/gold/gold_obt.py
```

## <span style="color:#0B2D5C;">**𝙊𝙥𝙩𝙞𝙤𝙣 𝘽 — 𝙈𝙖𝙠𝙚𝙛𝙞𝙡𝙚 (𝙛𝙧𝙤𝙢 𝙯𝙚𝙧𝙤 𝙩𝙤 𝙛𝙞𝙣𝙞𝙨𝙝)**</span>
1. Provision infrastructure:
```bash
make provision
```

2. Run the pipeline up to Silver:
```bash
make full
```

3. Validate Silver:
```bash
make quality-checks
```

4. Build Gold:
```bash
make gold-full
```

## <span style="color:#0B2D5C;">**𝙋𝙞𝙥𝙚𝙡𝙞𝙣𝙚 𝙍𝙪𝙣𝙗𝙤𝙤𝙠**</span>
1. Extract IMF data into JSON with Bruin: `bruin run bruin/pipeline/assets/ingestion/imf_api_extract.py`
2. Convert JSON to Parquet with Bruin: `bruin run bruin/pipeline/assets/ingestion/imf_json_to_parquet.py`
3. Upload Parquet to bronze: `bruin run bruin/pipeline/assets/ingestion/imf_bronze_upload.py`
4. Promote Parquet from bronze to silver: `bruin run bruin/pipeline/assets/ingestion/imf_bronze_to_silver.py`
5. Run Bruin quality checks on silver: `bruin run bruin/pipeline/assets/ingestion/imf_quality_checks.py`
6. Load gold tables in BigQuery: `bruin run bruin/pipeline/assets/ingestion/imf_gold_load.py`
7. Build the Gold OBT in BigQuery: `bruin run bruin/pipeline/assets/gold/gold_obt.py`

## <span style="color:#0B2D5C;">**𝙑𝙚𝙧𝙞𝙛𝙮 𝙏𝙝𝙚 𝙍𝙪𝙣**</span>
Use these commands after the pipeline finishes:

```bash
bq ls ecodatacloud:ecodatacloud_bq_gold

bq query --use_legacy_sql=false \
'SELECT * FROM `ecodatacloud.ecodatacloud_bq_gold.gold__obt` LIMIT 5'

bq query --use_legacy_sql=false \
'SELECT COUNT(*) AS row_count, COUNT(DISTINCT country_label) AS distinct_countries
FROM `ecodatacloud.ecodatacloud_bq_gold.gold__obt`'
```

## <span style="color:#0B2D5C;">**𝙈𝙖𝙠𝙚𝙛𝙞𝙡𝙚 𝙏𝙖𝙧𝙜𝙚𝙩𝙨**</span>
- `make auth-check`: verify gcloud and ADC authentication
- `make provision`: Terraform init + plan + apply
- `make bruin-extract`: IMF API extraction to JSON
- `make bruin-convert`: JSON to Parquet conversion
- `make ingest-bronze`: upload Parquet to bronze bucket
- `make promote-silver`: copy bronze parquet objects to the silver bucket
- `make quality-checks`: run Bruin data quality checks on silver
- `make gold-load`: load partitioned + clustered gold tables
- `make gold-obt`: build the Gold OBT in BigQuery
- `make gold-full`: run `make gold-load` then `make gold-obt`
- `make full`: provision + extract + convert + upload + promote to silver
- `make init-to-bronze`: provision + extract + convert + upload (no silver promotion)

## <span style="color:#0B2D5C;">**𝙏𝙤𝙤𝙡 𝙀𝙦𝙪𝙞𝙫𝙖𝙡𝙚𝙣𝙩𝙨**</span>
- `make auth-check` → `gcloud auth application-default print-access-token` + `gcloud config get-value project`
- `make provision` → `terraform -chdir=terraform init` + `terraform -chdir=terraform plan` + `terraform -chdir=terraform apply`
- `make bruin-extract` → `bruin run bruin/pipeline/assets/ingestion/imf_api_extract.py`
- `make bruin-convert` → `bruin run bruin/pipeline/assets/ingestion/imf_json_to_parquet.py`
- `make ingest-bronze` → `bruin run bruin/pipeline/assets/ingestion/imf_bronze_upload.py`
- `make promote-silver` → `bruin run bruin/pipeline/assets/ingestion/imf_bronze_to_silver.py`
- `make quality-checks` → `bruin run bruin/pipeline/assets/ingestion/imf_quality_checks.py`
- `make gold-load` → `bruin run bruin/pipeline/assets/ingestion/imf_gold_load.py`
- `make gold-obt` → `bruin run bruin/pipeline/assets/gold/gold_obt.py`
- `make gold-full` → `make gold-load` + `make gold-obt`

## <span style="color:#0B2D5C;">**𝘽𝙖𝙩𝙘𝙝 𝘾𝙤𝙣𝙛𝙞𝙜𝙪𝙧𝙖𝙩𝙞𝙤𝙣 (𝙘𝙤𝙙𝙖𝙩𝙖 𝙑𝙖𝙧𝙨)**</span>
Batch parameters are passed via `ECODATA_VARS` as JSON. Example:
`ECODATA_VARS='{"datasets":["gdp_per_capita_usd"],"periods":["2019","2020"],"dry_run":true,"max_objects":5}'`

Using `make` with ECODATA_VARS:
`ECODATA_VARS='{"datasets":["gdp_per_capita_usd"],"periods":["2019","2020"]}' make full`

**Why and when to use `overwrite`?**
By default, the pipeline skips files that already exist in the Bronze and Silver buckets to optimize execution. If you update your transformation rules (`silver_transforms.json`) or your data models, the existing files won't be updated automatically. You must pass `"overwrite": true` to force the pipeline to replace the old data with the newly transformed files.

Examples:
```bash
# Full run with a subset of datasets/years
ECODATA_VARS='{"datasets":["gdp_per_capita_usd"],"periods":["2020"]}' make full

# Quick quality check test
ECODATA_VARS='{"dry_run":true,"max_objects":3}' make quality-checks

# Force overwrite during bronze -> silver (Useful after updating transformation rules)
ECODATA_VARS='{"overwrite":true}' make promote-silver

# Force overwrite for the entire pipeline
ECODATA_VARS='{"overwrite":true}' make full
```

## <span style="color:#0B2D5C;">**𝘾𝙤𝙣𝙛𝙞𝙜𝙪𝙧𝙖𝙩𝙞𝙤𝙣 & 𝙇𝙤𝙜𝙨 𝙇𝙤𝙘𝙖𝙩𝙞𝙤𝙣𝙨**</span>
Configuration files:
- Silver transforms: `bruin/pipeline/config/silver_transforms.json`
- Data quality checks: `bruin/pipeline/config/quality_checks.json`
- Gold tables (partition/clustering): `bruin/pipeline/config/gold_tables.json`

Logs (generated at runtime):
- API extract log: `data/raw/api_download_log.txt`
- JSON → Parquet log: `data/parquet/_logs/imf_json_to_parquet_log.csv`
- Bronze upload log: `data/bronze/_logs/imf_bronze_upload_log.csv`
- Bronze → Silver log: `data/silver/_logs/imf_bronze_to_silver_log.csv`
- Quality checks log: `data/silver/_logs/imf_quality_checks_log.csv`
- Gold load log: `data/gold/_logs/imf_gold_load_log.csv`

## <span style="color:#0B2D5C;">**𝙁𝙞𝙡𝙚𝙨 → 𝙋𝙪𝙧𝙥𝙤𝙨𝙚**</span>
- `bruin/pipeline/assets/ingestion/imf_api_extract.py`: pull IMF data to `data/raw/*` + log `data/raw/api_download_log.txt`
- `bruin/pipeline/assets/ingestion/imf_json_to_parquet.py`: convert JSON to `data/parquet/*` + log `data/parquet/_logs/imf_json_to_parquet_log.csv`
- `bruin/pipeline/assets/ingestion/imf_bronze_upload.py`: upload parquet to `gs://ecodatacloud-ds-bronze/parquet/*` + log `data/bronze/_logs/imf_bronze_upload_log.csv`
- `bruin/pipeline/assets/ingestion/imf_bronze_to_silver.py`: promote bronze → `gs://ecodatacloud-ds-silver/parquet/*` + log `data/silver/_logs/imf_bronze_to_silver_log.csv`
- `bruin/pipeline/assets/ingestion/imf_quality_checks.py`: validate silver parquet + log `data/silver/_logs/imf_quality_checks_log.csv`
- `bruin/pipeline/assets/ingestion/imf_gold_load.py`: load gold tables in BigQuery + log `data/gold/_logs/imf_gold_load_log.csv`

## <span style="color:#0B2D5C;">**𝙈𝙞𝙣𝙞 𝙏𝙖𝙗𝙡𝙚 (𝙎𝙩𝙚𝙥 / 𝙁𝙞𝙡𝙚 / 𝙄𝙣𝙥𝙪𝙩 / 𝙊𝙪𝙩𝙥𝙪𝙩)**</span>
| Step | File | Input | Output |
| --- | --- | --- | --- |
| Extract | `bruin/pipeline/assets/ingestion/imf_api_extract.py` | IMF API | `data/raw/*` + `data/raw/api_download_log.txt` |
| Convert | `bruin/pipeline/assets/ingestion/imf_json_to_parquet.py` | `data/raw/*` | `data/parquet/*` + `data/parquet/_logs/imf_json_to_parquet_log.csv` |
| Bronze upload | `bruin/pipeline/assets/ingestion/imf_bronze_upload.py` | `data/parquet/*` | `gs://ecodatacloud-ds-bronze/parquet/*` + `data/bronze/_logs/imf_bronze_upload_log.csv` |
| Silver promote | `bruin/pipeline/assets/ingestion/imf_bronze_to_silver.py` | `gs://ecodatacloud-ds-bronze/parquet/*` | `gs://ecodatacloud-ds-silver/parquet/*` + `data/silver/_logs/imf_bronze_to_silver_log.csv` |
| Quality checks | `bruin/pipeline/assets/ingestion/imf_quality_checks.py` | `gs://ecodatacloud-ds-silver/parquet/*` | `data/silver/_logs/imf_quality_checks_log.csv` |
| Gold load | `bruin/pipeline/assets/ingestion/imf_gold_load.py` | `gs://ecodatacloud-ds-silver/parquet/*` | BigQuery `ecodatacloud_bq_gold.gold__*` + `data/gold/_logs/imf_gold_load_log.csv` |

## <span style="color:#0B2D5C;">**𝙋𝙖𝙧𝙖𝙢𝙚𝙩𝙚𝙧𝙨**</span>
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
7. `transform_config`: optional path to a JSON transform config (default: `bruin/pipeline/config/silver_transforms.json`).

Bronze upload parameters:
1. `bronze_bucket`: destination bucket name. Default: `ecodatacloud-ds-bronze`.
2. `prefix`: object prefix to upload under. Default: `parquet/`.
3. `overwrite`: overwrite existing objects in bronze. Default: `false`.
4. `dry_run`: log actions without uploading. Default: `false`.
5. `max_files`: limit the number of parquet files uploaded.
6. `local_parquet_dir`: override local parquet directory (default: `data/parquet`).

Transform configuration file (`bruin/pipeline/config/silver_transforms.json`):
1. `default.drop_columns`: columns to drop for all datasets.
2. `default.rename_columns`: rename map for all datasets.
3. `datasets.<dataset>.drop_columns`: dataset-specific columns to drop.
4. `datasets.<dataset>.rename_columns`: dataset-specific rename map.

Quality checks parameters:
1. `quality_config`: optional path to a JSON quality config (default: `bruin/pipeline/config/quality_checks.json`).
2. `fail_on_error`: fail the run if any dataset violates checks. Default: `true`.
3. `max_objects`: limit the number of datasets validated.

Gold load parameters:
1. `bq_project`: BigQuery project id (defaults to ADC project).
2. `bq_dataset`: BigQuery dataset for gold tables. Default: `ecodatacloud_bq_gold`.
3. `bq_location`: BigQuery location. Default: `EU`.
4. `table_prefix`: prefix for gold tables. Default: `gold__`.
5. `overwrite`: overwrite existing tables. Default: `false`.
6. `write_disposition`: BigQuery write disposition. Default: `WRITE_TRUNCATE`.
7. `create_disposition`: BigQuery create disposition. Default: `CREATE_IF_NEEDED`.
8. `gold_config`: optional path to gold table config (default: `bruin/pipeline/config/gold_tables.json`).

Gold table configuration file (`bruin/pipeline/config/gold_tables.json`):
1. `default.partition_field`: field used for partitioning (default `year`).
2. `default.partition_range`: range settings (`start`, `end`, `interval`).
3. `default.cluster_fields`: list of clustering fields (default `country`).
4. `datasets.<dataset>.partition_field`: override per dataset (use `null` to disable).
5. `datasets.<dataset>.cluster_fields`: override per dataset.
