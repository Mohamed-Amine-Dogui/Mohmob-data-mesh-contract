terraform {
  backend "gcs" {
    bucket = "mo-data-lake-prod-tfstate"
    prefix = "data-mesh-contracts-pro"
  }
}
