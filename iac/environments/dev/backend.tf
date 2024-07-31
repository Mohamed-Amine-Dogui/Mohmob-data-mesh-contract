terraform {
  backend "gcs" {
    bucket = "mo-data-lake-dev-tfstate"
    prefix = "data-mesh-contracts-dev"
  }
}
