variable "project" {
  description = "Project ID"
  type        = string
  default     = "taxi-rides-ny-485214" 
}

variable "region" {
  description = "Project Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My WEO dataset"
  default     = "weo_bigquery_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "taxi-rides-ny-485214-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}