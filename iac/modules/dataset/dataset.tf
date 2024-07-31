# Creates a BigQuery dataset in the EU using a provided name
# Project ID is inherited from the calling module.

# Variables
variable "dataset_name" {
  type        = string
  description = "Name of the BigQuery dataset (without suffix)"

  validation {
    condition     = length(var.dataset_name) <= 1024 && can(regex("[a-z0-9\\_]+", var.dataset_name))
    error_message = "Encountered bad dataset name. Only lowercase letters, digits, and underscores allowed."
  }
}

variable "dataset_suffix" {
  type        = string
  description = "BigQuery dataset name suffix"

  validation {
    condition     = length(var.dataset_suffix) == 0 || can(regex("[a-z0-9\\_]+", var.dataset_suffix))
    error_message = "Encountered bad dataset suffix. Only lowercase letters, digits, and underscores allowed."
  }
}

variable "env" {
  type        = string
  description = "Target environment; pro, pre or dev"

  validation {
    condition     = can(regex("(pro|pre|dev)", var.env))
    error_message = "Encountered bad input variable 'env'. Must be one of 'pro', 'pre' or 'dev'."
  }
}

variable "provisioning_service_account" {
  type        = string
  description = "Provisioning service account which gets full read/write access to the data contained in the dataset"
}

# Outputs
output "dataset" {
  value = google_bigquery_dataset.dataset
}

# Helper Variables
locals {
  config = fileexists("${path.root}/bq_schemas_static/${var.dataset_name}/access.json") ? tomap(jsondecode(file("${path.root}/bq_schemas_static/${var.dataset_name}/access.json"))) : {}

  accessors = toset(keys(local.config))
}

# BigQuery Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = "${var.dataset_name}${var.dataset_suffix}"
  location   = "EU"

  labels = {
    env = var.env == "pro" ? "prod" : var.env
    sgt_include = "false"
  }
}

# Dataset IAM Permissions (non-PII, PII, potential PII) from ACLs

resource "google_bigquery_dataset_iam_member" "dataset_iam_members" {
  for_each = local.accessors

  dataset_id = google_bigquery_dataset.dataset.dataset_id
  member = each.value
  role = "roles/bigquery.dataViewer"
}

# General Dataset IAM Permissions

# The provisioning service account used in Unicron needs to get full read/write access to the tables

resource "google_bigquery_dataset_iam_member" "provisioning_service_account" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  member     = "serviceAccount:${var.provisioning_service_account}"
  role       = "roles/bigquery.dataEditor"
}
