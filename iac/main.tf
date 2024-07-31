terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.5.0"
    }
  }
}

locals {
  dataset_suffix = local.env == "pro" ? "" : "_${local.env}"
  _all_files = fileset("${path.root}/bq_schemas_static", "**")
  _ftype_map = {for filepath in local._all_files : filepath => element(split(".", filepath), 1)}
  schemas    = toset(matchkeys(keys(local._ftype_map), values(local._ftype_map), ["tpl"])) # [dset/table.tpl, ...]

  datasets   = toset(distinct([for schema in local.schemas: element(split("/", schema), 0)])) # [dset1, dset2, ...]

  # Group tables by their respective dataset
  _symmetric_dset_list = [for schema in local.schemas : element(split("/", schema), 0)]
  _symmetric_tbl_list  = [for schema in local.schemas : element(split("/", schema), 1)]
  dataset_to_tables_map  = {
    for dataset in local.datasets:
    dataset => matchkeys(local._symmetric_tbl_list, local._symmetric_dset_list, [dataset])
  } # <dataset> => [<tableA>.tpl, <tableB>.tpl, ...]
}



# BigQuery Datasets
module "datasets" {
  for_each = local.datasets

  source = "./modules/dataset"

  dataset_name   = each.key
  dataset_suffix = local.dataset_suffix
  env            = local.env

  provisioning_service_account = local.cicd_provisioning_service_account
}

# BigQuery Tables
# Module is instantiated for each BQ data set given a map: <dataset> => [<tableA>.tpl, <tableB>.tpl, ...]
module "tables" {
  for_each = local.dataset_to_tables_map

  source = "./modules/tables"

  dataset_base_name = each.key
  dataset           = module.datasets[each.key].dataset
  tables            = each.value
  env               = local.env
}


module "transform" {
  source = "./modules/transform"
  dataset_suffix = local.dataset_suffix
}