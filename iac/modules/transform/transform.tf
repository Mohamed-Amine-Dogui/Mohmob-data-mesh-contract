# Variables
variable "dataset_suffix" {
  type        = string
  description = "BigQuery dataset name suffix"

  validation {
    condition     = length(var.dataset_suffix) == 0 || can(regex("[a-z0-9\\_]+", var.dataset_suffix))
    error_message = "Encountered bad dataset suffix. Only lowercase letters, digits, and underscores allowed."
  }
}



resource "google_bigquery_data_transfer_config" "data-mesh-contracts-data_products_usage" {
  display_name   = "data-mesh-contracts-data_products_usage"
  location       = "EU"
  data_source_id = "scheduled_query"
  schedule       = "every day 07:00"
  params = {
    query = templatefile(
      "${path.root}/bq_scheduled_queries/transformations/transform_data_products_usage.sql",
      {
        "dataset" : "data_mesh_contracts${var.dataset_suffix}"
        "table" : "data_products_usage"
      }
    )
  }
}
