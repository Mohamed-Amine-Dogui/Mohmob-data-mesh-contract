# Creates BigQuery Tables in an already existing (!) BigQuery Dataset given a reference
# to the dataset and a list of table schema template files (tablename.tpl).
# The schema for the created tables is derived from schema template files which are expected
# to be located in "${path.root}/bq_schemas_static/<provided-dataset>.dataset_id/<provided-table-schema-file>".
#
# Also creates google_bigquery_table_iam_member and google_data_catalog_policy_tag_iam_member resources
# to non-authoritatively (https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/data_catalog_policy_tag_iam)
# grant table and PII-column access to users and service accounts based on <table-name>_access.json file contents.
#
# A table description can be set by providing a "tablename.txt" file with the description in the same directory.
#
# The created tables default to
# - Time partitioning on the "date" field (override available via a tablename_time_part_field.override file in the same directory)
# - Daily partitioning
# - Expiration of records after 760d 10h
# - Queries requiring a partition filter
#
# The project ID is inherited from the calling module.

# Variables
variable "dataset" {
  type = object({
    id         = string
    dataset_id = string
  })
  description = "Dataset for which to create tables"
}

variable "dataset_base_name" {
  type        = string
  description = "Dataset name without suffix (corresponding to namespace, schema directory and PII policy tags)"
}

variable "tables" {
  type        = list(string) # config: [tableA.tpl, tableB.tpl]
  description = "List of table schema file names"

  validation {
    condition     = can([for table in var.tables : regex("[a-zA-Z0-9]+\\.tpl", table)])
    error_message = "Encountered bad table name. Only lowercase & uppercase letters and digits allowed. Schema definitions must be *.tpl files."
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

# Outputs
output "tables" {
  value = google_bigquery_table.tables
}

# Helper Variables
locals {
  tables = toset([for table in toset(var.tables): element(split(".", table), 0)]) # cleaned: [tableA, tableB]

  _table_access_configs = {for table in local.tables : table => (
    fileexists("${path.root}/bq_schemas_static/${var.dataset_base_name}/${table}_access.json") ? {
      value    = jsondecode(file("${path.root}/bq_schemas_static/${var.dataset_base_name}/${table}_access.json")),
      nonempty = true
    } : {
      value    = {},
      nonempty = false
    }
  )}
  _non_empty_access_configs = { for table, compval in local._table_access_configs : table => compval.value if compval.nonempty == true}

  # Used to grant general table access to users that exists as keys in the the respective table's `*_access.json`
  tbl_accesses = toset(flatten([for table, config in local._non_empty_access_configs : [ for member in toset(keys(tomap(config))) : "${table}|${member}"]]))

  table_deletion_protection = can(regex("dev|pre", var.env)) ? false : true
}

# BigQuery Tables
resource "google_bigquery_table" "tables" {
  for_each = local.tables

  dataset_id = var.dataset.dataset_id
  table_id   = each.key

  schema = file("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}.tpl")

  description = fileexists("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}.description") ? file("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}.description") : "No table description currently in place. Please reach out to @ds-goalie and let us know that we can improve on that! :)"

  dynamic "time_partitioning" {
    for_each = fileexists("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}_time_part.deactivate") ? [] : [1]
    content {
      field                    = fileexists("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}_time_part_field.override") ? file("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}_time_part_field.override") : "date"
      expiration_ms            = fileexists("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}_expiration_ms.override") ? file("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}_expiration_ms.override") : 62208000000
      type                     = "DAY"
    }
  }
  require_partition_filter = fileexists("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}_time_part.deactivate") ? false : true

  clustering = fileexists("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}_clustering.override") ? split(",", file("${path.root}/bq_schemas_static/${var.dataset_base_name}/${each.key}_clustering.override")) : []

  labels = {
    env = var.env == "pro" ? "prod" : var.env
    sgt_include = "false"
  }

  deletion_protection = local.table_deletion_protection
}

# Table IAM Permissions (non-PII) from ACLs
resource "google_bigquery_table_iam_member" "tables_iam_members" {
  # for_each sadly only supports maps and sets of strings. therefore we need to provide a custom string format and parse it
  for_each = local.tbl_accesses

  dataset_id = var.dataset.dataset_id
  table_id   = google_bigquery_table.tables[element(split("|", each.value), 0)].table_id # acc to table part of string
  role       = "roles/bigquery.dataViewer"
  member     = element(split("|", each.value), 1) # accesses the member part of the string
}
