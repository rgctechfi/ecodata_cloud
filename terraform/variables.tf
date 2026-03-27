variable "project" {
  description = "Project ID"
  type        = string
  default     = "ecodatacloud"
}

variable "region" {
  description = "Project Region"
  default     = "europe-west1"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My WEO dataset"
  default     = "ecodatacloud_bq_gold"
}

variable "gcs_bucket_bronze_name" {
  description = "Bronze Storage Bucket Name"
  default     = "ecodatacloud-ds-bronze"
}

variable "gcs_bucket_silver_name" {
  description = "Silver Storage Bucket Name"
  default     = "ecodatacloud-ds-silver"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
